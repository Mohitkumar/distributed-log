package tests

import (
	"fmt"
	"sync"

	"github.com/mohitkumar/mlog/api/protocol"
	"github.com/mohitkumar/mlog/broker/cluster"
	raft "github.com/mohitkumar/mlog/broker/cluster/raft"
	"github.com/mohitkumar/mlog/broker/topic"
)

var _ topic.TopicCoordinator = (*FakeTopicCoordinator)(nil)

// FakeNodeInfo is a minimal node record for FakeTopicCoordinator (no Raft): just enough
// to answer AliveNodeIDs/NodeRPCAddr/IsNodeAlive, which in production come from Raft's
// voter configuration reconciled with Serf gossip (see cluster.Cluster).
type FakeNodeInfo struct {
	NodeID  string
	RpcAddr string
}

// FakeTopicCoordinator implements topic.TopicCoordinator in memory (no Raft). Mimics
// Cluster: it owns a real *cluster.ClusterMetadataStore (the same Apply/state-machine
// logic production uses) and, when a callback is registered via SetOnMetadataEvent,
// forwards every applied event to it synchronously (like Cluster's notifyingMetadataStore).
type FakeTopicCoordinator struct {
	mu sync.RWMutex

	NodeID       string
	RPCAddr      string
	IsRaftLeader bool

	Nodes         map[string]*FakeNodeInfo // nodeID -> node (fake's own membership view)
	metadataStore *cluster.ClusterMetadataStore

	onMetadataEvent func(ev *raft.MetadataEvent) error
	stopReplication chan struct{}
}

func NewFakeTopicCoordinator(nodeID, rpcAddr string) *FakeTopicCoordinator {
	f := &FakeTopicCoordinator{
		NodeID:        nodeID,
		RPCAddr:       rpcAddr,
		IsRaftLeader:  true,
		Nodes:         make(map[string]*FakeNodeInfo),
		metadataStore: cluster.NewClusterMetadataStore(),
	}
	f.Nodes[nodeID] = &FakeNodeInfo{NodeID: nodeID, RpcAddr: rpcAddr}
	return f
}

// — TopicCoordinator interface —

func (f *FakeTopicCoordinator) TopicExists(topicName string) bool {
	return f.metadataStore.TopicExists(topicName)
}

func (f *FakeTopicCoordinator) TopicNames() []string {
	return f.metadataStore.TopicNames()
}

func (f *FakeTopicCoordinator) TopicInfo(topicName string) (protocol.TopicInfo, bool) {
	t := f.metadataStore.GetTopic(topicName)
	if t == nil {
		return protocol.TopicInfo{}, false
	}
	leaderID, epoch, replicaSnaps := t.Snapshot()
	replicas := make([]protocol.ReplicaInfo, 0, len(replicaSnaps))
	for _, rs := range replicaSnaps {
		replicas = append(replicas, protocol.ReplicaInfo{NodeID: rs.ReplicaNodeID, IsISR: rs.IsISR, LEO: rs.LEO})
	}
	return protocol.TopicInfo{Name: topicName, LeaderNodeID: leaderID, LeaderEpoch: epoch, Replicas: replicas}, true
}

func (f *FakeTopicCoordinator) TopicLeaderNodeID(topicName string) (string, bool) {
	t := f.metadataStore.GetTopic(topicName)
	if t == nil {
		return "", false
	}
	return t.LeaderID(), true
}

func (f *FakeTopicCoordinator) TopicHasReplica(topicName, nodeID string) bool {
	t := f.metadataStore.GetTopic(topicName)
	if t == nil {
		return false
	}
	return t.HasReplica(nodeID)
}

func (f *FakeTopicCoordinator) TopicMinISRLeo(topicName string, localLEO uint64) uint64 {
	t := f.metadataStore.GetTopic(topicName)
	if t == nil {
		return localLEO
	}
	return t.MinISRLeo(localLEO)
}

func (f *FakeTopicCoordinator) NodeIDWithLeastTopics(candidateNodeIDs []string) (string, error) {
	return f.metadataStore.NodeIDWithLeastTopics(candidateNodeIDs)
}

func (f *FakeTopicCoordinator) RecordReplicaFetch(topicName, replicaNodeID string, leo int64, lagThreshold uint64, localLEO uint64) (bool, bool) {
	t := f.metadataStore.GetTopic(topicName)
	if t == nil {
		return false, false
	}
	return t.RecordReplicaFetch(replicaNodeID, leo, lagThreshold, localLEO), true
}

func (f *FakeTopicCoordinator) SetOnMetadataEvent(fn func(ev *raft.MetadataEvent) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onMetadataEvent = fn
}

func (f *FakeTopicCoordinator) ApplyCreateTopicEvent(topicName string, replicaCount uint32, leaderNodeID string, replicaNodeIds []string) error {
	if replicaCount == 0 {
		leaderNodeID = f.NodeID
	}
	eventData, err := raft.EncodeCreateTopicEvent(raft.CreateTopicEvent{
		Topic: topicName, ReplicaCount: replicaCount, LeaderNodeID: leaderNodeID, LeaderEpoch: 1, ReplicaNodeIds: replicaNodeIds,
	})
	if err != nil {
		return err
	}
	return f.apply(&raft.MetadataEvent{EventType: raft.MetadataEventTypeCreateTopic, Data: eventData})
}

func (f *FakeTopicCoordinator) ApplyDeleteTopicEventInternal(topicName string) error {
	eventData, err := raft.EncodeDeleteTopicEvent(raft.DeleteTopicEvent{Topic: topicName})
	if err != nil {
		return err
	}
	return f.apply(&raft.MetadataEvent{EventType: raft.MetadataEventTypeDeleteTopic, Data: eventData})
}

func (f *FakeTopicCoordinator) ApplyIsrUpdateEventInternal(topicName, replicaNodeID string, isr bool) error {
	eventData, err := raft.EncodeIsrUpdateEvent(raft.IsrUpdateEvent{Topic: topicName, ReplicaNodeID: replicaNodeID, Isr: isr})
	if err != nil {
		return err
	}
	return f.apply(&raft.MetadataEvent{EventType: raft.MetadataEventTypeIsrUpdate, Data: eventData})
}

func (f *FakeTopicCoordinator) ApplyLeaderChangeEvent(topicName, leaderNodeID string, leaderEpoch int64) error {
	eventData, err := raft.EncodeLeaderChangeEvent(raft.LeaderChangeEvent{
		Topic: topicName, LeaderNodeID: leaderNodeID, LeaderEpoch: leaderEpoch,
	})
	if err != nil {
		return err
	}
	return f.apply(&raft.MetadataEvent{EventType: raft.MetadataEventTypeLeaderChange, Data: eventData})
}

func (f *FakeTopicCoordinator) IsLeader() bool {
	return f.IsRaftLeader
}

func (f *FakeTopicCoordinator) GetRaftLeaderNodeID() (string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.IsRaftLeader {
		return f.NodeID, nil
	}
	return "", fmt.Errorf("not raft leader")
}

// AliveNodeIDs returns all known node IDs (fakes don't model liveness beyond "known").
func (f *FakeTopicCoordinator) AliveNodeIDs() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	ids := make([]string, 0, len(f.Nodes))
	for id := range f.Nodes {
		ids = append(ids, id)
	}
	return ids
}

// NodeRPCAddr returns the RPC address for nodeID, if known.
func (f *FakeTopicCoordinator) NodeRPCAddr(nodeID string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	n := f.Nodes[nodeID]
	if n == nil {
		return "", false
	}
	return n.RpcAddr, true
}

// IsNodeAlive reports whether nodeID is known to this fake.
func (f *FakeTopicCoordinator) IsNodeAlive(nodeID string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.Nodes[nodeID] != nil
}

// — Helpers for tests (testutil and node/topic/replica tests) —

func (f *FakeTopicCoordinator) AddNode(nodeID, rpcAddr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Nodes[nodeID] = &FakeNodeInfo{NodeID: nodeID, RpcAddr: rpcAddr}
}

// StartReplicationThread/StopReplicationThread exist for API parity with callers that
// start/stop replication via the coordinator; actual replication in tests is driven by
// the owning topic.TopicManager's own replication thread (see
// topic.TopicManager.StartReplicationThread), not by the fake itself.
func (f *FakeTopicCoordinator) StartReplicationThread() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopReplication == nil {
		f.stopReplication = make(chan struct{})
	}
}

func (f *FakeTopicCoordinator) StopReplicationThread() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopReplication != nil {
		close(f.stopReplication)
		f.stopReplication = nil
	}
}

func (f *FakeTopicCoordinator) GetOtherNodes() []*FakeNodeInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]*FakeNodeInfo, 0, len(f.Nodes))
	for id, n := range f.Nodes {
		if id != f.NodeID && n != nil {
			out = append(out, n)
		}
	}
	return out
}

// ApplyEvent applies a single metadata event (for tests that drive create/delete/isr
// on one fake to simulate another node observing Raft-replicated metadata, without
// going through a shared TopicManager).
func (f *FakeTopicCoordinator) ApplyEvent(ev topic.ApplyEvent) {
	switch ev.Type {
	case topic.ApplyEventCreateTopic:
		if ev.CreateTopic != nil {
			_ = f.ApplyCreateTopicEvent(ev.CreateTopic.Topic, ev.CreateTopic.ReplicaCount, ev.CreateTopic.LeaderNodeID, ev.CreateTopic.ReplicaNodeIds)
		}
	case topic.ApplyEventDeleteTopic:
		if ev.DeleteTopic != nil {
			_ = f.ApplyDeleteTopicEventInternal(ev.DeleteTopic.Topic)
		}
	case topic.ApplyEventIsrUpdate:
		if ev.IsrUpdate != nil {
			_ = f.ApplyIsrUpdateEventInternal(ev.IsrUpdate.Topic, ev.IsrUpdate.ReplicaNodeID, ev.IsrUpdate.Isr)
		}
	}
}

// apply applies ev to the metadata store (same logic production's ClusterMetadataStore
// uses) and, if a callback is registered, forwards it synchronously — mirroring
// cluster.Cluster's notifyingMetadataStore.
func (f *FakeTopicCoordinator) apply(ev *raft.MetadataEvent) error {
	if err := f.metadataStore.Apply(ev); err != nil {
		return err
	}
	f.mu.RLock()
	fn := f.onMetadataEvent
	f.mu.RUnlock()
	if fn != nil {
		_ = fn(ev)
	}
	return nil
}
