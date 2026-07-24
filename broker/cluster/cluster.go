package cluster

import (
	"strings"
	"sync"
	"time"

	"github.com/mohitkumar/mlog/api/protocol"
	"github.com/mohitkumar/mlog/broker/cluster/discovery"
	raft "github.com/mohitkumar/mlog/broker/cluster/raft"
	"github.com/mohitkumar/mlog/broker/config"
	"go.uber.org/zap"
)

// MemberLister returns information about cluster members currently alive (as seen by
// Serf). Used by Cluster to reconcile Raft voters with Serf membership, and as the
// source of node addresses (Raft's own configuration only knows raft addresses, not
// RPC addresses — see AliveNodeIDs/NodeRPCAddr).
type MemberLister interface {
	AliveMembers() []string
	AliveNodeDetails() []discovery.NodeInfo
}

// Cluster is the write path for cluster-wide state: it drives Raft consensus and
// membership (Join/Leave), owns the Raft-replicated metadata store (ClusterMetadataStore
// is the FSM state), and issues the metadata events (create/delete topic, leader change,
// ISR update) that store applies. Address/membership queries are answered from Raft's
// own voter configuration reconciled with Serf gossip (see AliveNodeIDs, NodeRPCAddr)
// rather than a separate replicated node map.
type Cluster struct {
	Logger        *zap.Logger
	node          *raft.RaftNode
	cfg           config.Config
	metadataStore *ClusterMetadataStore

	mu            sync.RWMutex
	memberLister  MemberLister
	onNodeRemoved func(nodeID string)
}

func NewCluster(cfg config.Config, logger *zap.Logger) (*Cluster, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	c := &Cluster{
		Logger:        logger,
		cfg:           cfg,
		metadataStore: NewClusterMetadataStore(),
	}
	node, err := raft.NewRaftNode(cfg, c.metadataStore, logger)
	if err != nil {
		return nil, err
	}
	c.node = node
	c.node.Start()
	go c.watchPeerChanges()
	return c, nil
}

// — Topic metadata queries (read-only; answered from the local, Raft-replicated
// ClusterMetadataStore — see the type's own docs for why it never touches Raft
// directly). topic.TopicManager reaches cluster metadata only through these, never by
// holding the store itself. —

// TopicExists reports whether topic exists.
func (c *Cluster) TopicExists(topic string) bool {
	return c.metadataStore.TopicExists(topic)
}

// TopicNames returns all topic names.
func (c *Cluster) TopicNames() []string {
	return c.metadataStore.TopicNames()
}

// TopicInfo returns a point-in-time snapshot of topic's leader/epoch/replica state,
// or ok=false if topic doesn't exist.
func (c *Cluster) TopicInfo(topic string) (info protocol.TopicInfo, ok bool) {
	t := c.metadataStore.GetTopic(topic)
	if t == nil {
		return protocol.TopicInfo{}, false
	}
	leaderID, epoch, replicaSnaps := t.Snapshot()
	replicas := make([]protocol.ReplicaInfo, 0, len(replicaSnaps))
	for _, rs := range replicaSnaps {
		replicas = append(replicas, protocol.ReplicaInfo{NodeID: rs.ReplicaNodeID, IsISR: rs.IsISR, LEO: rs.LEO})
	}
	return protocol.TopicInfo{Name: topic, LeaderNodeID: leaderID, LeaderEpoch: epoch, Replicas: replicas}, true
}

// TopicLeaderNodeID returns the current leader node ID for topic, or ok=false if
// topic doesn't exist.
func (c *Cluster) TopicLeaderNodeID(topic string) (leaderNodeID string, ok bool) {
	t := c.metadataStore.GetTopic(topic)
	if t == nil {
		return "", false
	}
	return t.LeaderID(), true
}

// TopicHasReplica reports whether nodeID is a tracked replica of topic.
func (c *Cluster) TopicHasReplica(topic, nodeID string) bool {
	t := c.metadataStore.GetTopic(topic)
	if t == nil {
		return false
	}
	return t.HasReplica(nodeID)
}

// TopicMinISRLeo returns min(localLEO, all of topic's in-sync replicas' LEO) — used to
// compute the consumer-visible high watermark. Returns localLEO unchanged if topic
// doesn't exist.
func (c *Cluster) TopicMinISRLeo(topic string, localLEO uint64) uint64 {
	t := c.metadataStore.GetTopic(topic)
	if t == nil {
		return localLEO
	}
	return t.MinISRLeo(localLEO)
}

// NodeIDWithLeastTopics returns whichever of candidateNodeIDs currently leads the
// fewest topics, for CreateTopic leader placement.
func (c *Cluster) NodeIDWithLeastTopics(candidateNodeIDs []string) (string, error) {
	return c.metadataStore.NodeIDWithLeastTopics(candidateNodeIDs)
}

// RecordReplicaFetch updates replicaNodeID's LEO for topic from a Fetch call and
// recomputes its ISR status against lagThreshold; ok is false if topic doesn't exist.
// Local only (not Raft-replicated) — the caller applies the returned isr status via
// ApplyIsrUpdateEventInternal if it changed.
func (c *Cluster) RecordReplicaFetch(topic, replicaNodeID string, leo int64, lagThreshold uint64, localLEO uint64) (isr bool, ok bool) {
	t := c.metadataStore.GetTopic(topic)
	if t == nil {
		return false, false
	}
	return t.RecordReplicaFetch(replicaNodeID, leo, lagThreshold, localLEO), true
}

// SetMemberLister sets the Serf member lister used for Raft-Serf reconciliation and
// node address lookups. Must be called after discovery.New() completes.
func (c *Cluster) SetMemberLister(ml MemberLister) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.memberLister = ml
}

// SetOnNodeRemoved registers a callback invoked after a node is removed as a Raft
// voter (either via explicit Leave or reconciliation dropping a stale voter). Used by
// topic.TopicManager to reassign leadership for topics the removed node was leading.
func (c *Cluster) SetOnNodeRemoved(fn func(nodeID string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onNodeRemoved = fn
}

// AliveNodeIDs returns the current cluster member node IDs: Raft's own voter
// configuration (authoritative — what consensus has actually agreed to), intersected
// with Serf's alive set when a member lister is available. Used as the candidate list
// for topic/replica placement.
func (c *Cluster) AliveNodeIDs() []string {
	raftIDs, err := c.node.RaftServerIDs()
	if err != nil {
		c.Logger.Warn("alive node ids: raft config unavailable", zap.Error(err))
		return nil
	}
	c.mu.RLock()
	ml := c.memberLister
	c.mu.RUnlock()
	if ml == nil {
		return raftIDs
	}
	alive := make(map[string]struct{})
	for _, name := range ml.AliveMembers() {
		alive[name] = struct{}{}
	}
	out := make([]string, 0, len(raftIDs))
	for _, id := range raftIDs {
		if _, ok := alive[id]; ok {
			out = append(out, id)
		}
	}
	return out
}

// IsNodeAlive reports whether nodeID is currently a Raft voter (a recognized cluster member).
func (c *Cluster) IsNodeAlive(nodeID string) bool {
	ids, err := c.node.RaftServerIDs()
	if err != nil {
		return false
	}
	for _, id := range ids {
		if id == nodeID {
			return true
		}
	}
	return false
}

// NodeRPCAddr returns the RPC address Serf has gossiped for nodeID.
func (c *Cluster) NodeRPCAddr(nodeID string) (string, bool) {
	c.mu.RLock()
	ml := c.memberLister
	c.mu.RUnlock()
	if ml == nil {
		return "", false
	}
	for _, n := range ml.AliveNodeDetails() {
		if n.Name == nodeID && n.RpcAddr != "" {
			return n.RpcAddr, true
		}
	}
	return "", false
}

func (c *Cluster) ApplyCreateTopicEvent(topic string, replicaCount uint32, leaderNodeID string, replicaNodeIds []string) error {
	if !c.node.IsLeader() {
		c.Logger.Debug("not leader, skipping create topic event", zap.String("topic", topic))
		return nil
	}
	eventData, err := raft.EncodeCreateTopicEvent(raft.CreateTopicEvent{
		Topic:          topic,
		ReplicaCount:   replicaCount,
		LeaderNodeID:   leaderNodeID,
		LeaderEpoch:    1,
		ReplicaNodeIds: replicaNodeIds,
	})
	if err != nil {
		return err
	}
	data, err := raft.EncodeMetadataEvent(&raft.MetadataEvent{
		EventType: raft.MetadataEventTypeCreateTopic,
		Data:      eventData,
	})
	if err != nil {
		return err
	}
	c.Logger.Info("apply create topic event", zap.String("topic", topic), zap.String("leader_node_id", leaderNodeID))
	if err := c.node.ApplyEvent(data); err != nil {
		c.Logger.Error("raft apply create topic failed", zap.Error(err), zap.String("topic", topic))
		return err
	}
	return nil
}

func (c *Cluster) ApplyDeleteTopicEventInternal(topic string) error {
	if !c.node.IsLeader() {
		c.Logger.Debug("not leader, skipping delete topic event", zap.String("topic", topic))
		return nil
	}
	eventData, err := raft.EncodeDeleteTopicEvent(raft.DeleteTopicEvent{Topic: topic})
	if err != nil {
		return err
	}
	data, err := raft.EncodeMetadataEvent(&raft.MetadataEvent{
		EventType: raft.MetadataEventTypeDeleteTopic,
		Data:      eventData,
	})
	if err != nil {
		return err
	}
	c.Logger.Info("apply delete topic event", zap.String("topic", topic))
	if err := c.node.ApplyEvent(data); err != nil {
		c.Logger.Error("raft apply delete topic failed", zap.Error(err), zap.String("topic", topic))
		return err
	}
	return nil
}

func (c *Cluster) ApplyIsrUpdateEventInternal(topic, replicaNodeID string, isr bool) error {
	if !c.node.IsLeader() {
		c.Logger.Debug("not leader, skipping ISR update event", zap.String("topic", topic))
		return nil
	}
	eventData, err := raft.EncodeIsrUpdateEvent(raft.IsrUpdateEvent{Topic: topic, ReplicaNodeID: replicaNodeID, Isr: isr})
	if err != nil {
		return err
	}
	data, err := raft.EncodeMetadataEvent(&raft.MetadataEvent{
		EventType: raft.MetadataEventTypeIsrUpdate,
		Data:      eventData,
	})
	if err != nil {
		return err
	}
	if err := c.node.ApplyEvent(data); err != nil {
		msg := err.Error()
		if strings.Contains(msg, "shutdown") || strings.Contains(msg, "leadership lost") {
			c.Logger.Debug("raft apply ISR update failed (shutdown or leadership change)", zap.Error(err))
		} else {
			c.Logger.Error("raft apply ISR update failed", zap.Error(err))
		}
		return err
	}
	return nil
}

func (c *Cluster) ApplyLeaderChangeEvent(topic, leaderNodeID string, leaderEpoch int64) error {
	if !c.node.IsLeader() {
		c.Logger.Debug("not leader, skipping leader change event", zap.String("topic", topic))
		return nil
	}
	eventData, err := raft.EncodeLeaderChangeEvent(raft.LeaderChangeEvent{
		Topic:        topic,
		LeaderNodeID: leaderNodeID,
		LeaderEpoch:  leaderEpoch,
	})
	if err != nil {
		return err
	}
	data, err := raft.EncodeMetadataEvent(&raft.MetadataEvent{
		EventType: raft.MetadataEventTypeLeaderChange,
		Data:      eventData,
	})
	if err != nil {
		return err
	}
	c.Logger.Info("apply leader change event", zap.String("topic", topic), zap.String("new_leader_node_id", leaderNodeID), zap.Int64("leader_epoch", leaderEpoch))
	if err := c.node.ApplyEvent(data); err != nil {
		c.Logger.Error("raft apply leader change failed", zap.Error(err), zap.String("topic", topic))
		return err
	}
	return nil
}

func (c *Cluster) IsLeader() bool {
	return c.node.IsLeader()
}

func (c *Cluster) GetRaftLeaderNodeID() (string, error) {
	return c.node.GetRaftLeaderNodeID()
}

func (c *Cluster) RaftServerIDs() ([]string, error) {
	return c.node.RaftServerIDs()
}

func (c *Cluster) WaitforRaftReady(timeout time.Duration) error {
	return c.node.WaitforRaftReady(timeout)
}

func (c *Cluster) IsRaftReady() bool {
	return c.node.IsRaftReady()
}

// Join adds id as a Raft voter. Cluster membership itself is now purely Raft's own
// configuration — no separate metadata event is applied for it.
func (c *Cluster) Join(id, raftAddr, rpcAddr string) error {
	return c.node.Join(id, raftAddr, rpcAddr)
}

// Leave removes id as a Raft voter. watchPeerChanges (below) observes the
// resulting Raft PeerObservation and fires onNodeRemoved — that single path
// covers removals regardless of what triggered them, not just this method.
func (c *Cluster) Leave(id string) error {
	return c.node.Leave(id)
}

// Start is a no-op: bootstrap (if configured) already happened during NewCluster.
// Kept for interface parity with callers that call Start() explicitly after construction.
func (c *Cluster) Start() error {
	return nil
}

func (c *Cluster) Shutdown() error {
	c.Logger.Info("cluster shutting down")
	return c.node.Shutdown()
}

// watchPeerChanges reacts to Raft peer configuration changes as they commit
// (see raft.RaftNode.WatchPeerChanges) — the event-driven replacement for
// periodically polling Raft's configuration against Serf membership. The
// decision to add or remove a voter still comes from Serf (discovery.Membership
// calls Join/Leave directly off Serf's own join/leave/failed events, in real
// time); this just reacts the instant that decision actually takes effect,
// regardless of what triggered it.
func (c *Cluster) watchPeerChanges() {
	events, stop := c.node.WatchPeerChanges()
	defer stop()
	for ev := range events {
		if ev.Removed {
			c.Logger.Info("raft peer removed", zap.String("node_id", ev.NodeID))
			c.mu.RLock()
			fn := c.onNodeRemoved
			c.mu.RUnlock()
			if fn != nil {
				fn(ev.NodeID)
			}
		} else {
			c.Logger.Info("raft peer added", zap.String("node_id", ev.NodeID))
		}
	}
}
