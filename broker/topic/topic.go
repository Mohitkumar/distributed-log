package topic

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/mohitkumar/mlog/api/protocol"
	"github.com/mohitkumar/mlog/api/protocol/pb"
	"github.com/mohitkumar/mlog/broker/coordinator"
	"github.com/mohitkumar/mlog/broker/log"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	defaultMetadataLogInterval = 30 * time.Second
	// DefaultISRLagThreshold is the max number of records a replica can lag
	// behind the leader and still be considered in-sync. Configurable via TopicManager.ISRLagThreshold.
	DefaultISRLagThreshold = uint64(100)
)

type NodeMetadata struct {
	mu      sync.RWMutex `json:"-"`
	NodeID  string       `json:"node_id"`
	Addr    string       `json:"addr"`
	RpcAddr string       `json:"rpc_addr"`
}

type ReplicaState struct {
	ReplicaNodeID string `json:"replica_id"`
	LEO           int64  `json:"leo"`
	IsISR         bool   `json:"is_isr"`
}

// In memory representation of a topic
type Topic struct {
	mu                  sync.RWMutex             `json:"-"`
	Name                string                   `json:"name"`
	LeaderNodeID        string                   `json:"leader_id"`
	LeaderEpoch         int64                    `json:"leader_epoch"`
	DesiredReplicaCount int                      `json:"desired_replica_count"` // from CreateTopic; used to re-add replicas when nodes rejoin
	Replicas            map[string]*ReplicaState `json:"replicas"`
	Log                 *log.LogManager          `json:"-"`
	Logger              *zap.Logger              `json:"-"`
}

var _ coordinator.MetadataStore = (*TopicManager)(nil)

type TopicManager struct {
	mu                   sync.RWMutex
	Topics               map[string]*Topic        `json:"topics"`
	BaseDir              string                   `json:"-"`
	Logger               *zap.Logger              `json:"-"`
	Nodes                map[string]*NodeMetadata `json:"nodes"`
	CurrentNodeID        string                   `json:"-"` // Local node ID from config; not persisted in Raft snapshot.
	coordinator          TopicCoordinator         `json:"-"`
	stopPeriodic         chan struct{}            `json:"-"`
	stopReplication      chan struct{}            `json:"-"`
	replicationBatchSize uint32                   `json:"-"`
	ISRLagThreshold      uint64                   `json:"-"` // max record lag for ISR membership
}

// NewTopicManager creates a TopicManager. Coordinator may be nil and set later via SetCoordinator
// (e.g. when TopicManager is used as MetadataStore for the Coordinator).
func NewTopicManager(baseDir string, coord TopicCoordinator, logger *zap.Logger) (*TopicManager, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	tm := &TopicManager{
		Topics:          make(map[string]*Topic),
		BaseDir:         baseDir,
		Logger:          logger,
		Nodes:           make(map[string]*NodeMetadata),
		coordinator:     coord,
		stopPeriodic:    make(chan struct{}),
		ISRLagThreshold: DefaultISRLagThreshold,
	}
	go tm.periodicLog(defaultMetadataLogInterval)
	tm.replicationBatchSize = DefaultReplicationBatchSize
	return tm, nil
}

// SetCoordinator sets the coordinator (e.g. after Coordinator is created with this TopicManager as MetadataStore).
func (tm *TopicManager) SetCoordinator(c TopicCoordinator) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.coordinator = c
}

// SetCurrentNodeID sets this node's ID (from config); not persisted in Raft state.
func (tm *TopicManager) SetCurrentNodeID(nodeID string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.CurrentNodeID = nodeID
}

func (tm *TopicManager) IsLeader(topic string) (bool, error) {
	tm.mu.RLock()
	topicObj, ok := tm.Topics[topic]
	currentNodeID := tm.CurrentNodeID
	tm.mu.RUnlock()
	if !ok {
		return false, ErrTopicNotFoundf(topic)
	}
	return topicObj.LeaderID() == currentNodeID, nil
}

// GetTopicLeaderRPCAddr returns the RPC address of the current leader for the given topic.
func (tm *TopicManager) GetTopicLeaderRPCAddr(topic string) (string, error) {
	tm.mu.RLock()
	topicObj, ok := tm.Topics[topic]
	tm.mu.RUnlock()
	if !ok {
		return "", ErrTopicNotFoundf(topic)
	}
	leaderNodeID := topicObj.LeaderID()
	tm.mu.RLock()
	node := tm.Nodes[leaderNodeID]
	tm.mu.RUnlock()
	if node == nil {
		return "", ErrTopicNotFoundf(topic)
	}
	return node.RpcAddr, nil
}

// lookupTopic returns the Topic for name, or nil if not found. Safe for concurrent use.
func (tm *TopicManager) lookupTopic(name string) *Topic {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.Topics[name]
}

// GetRaftLeaderRPCAddr returns the RPC address of the current Raft (metadata) leader.
// selfRpcAddr is this node's RPC address (e.g. from the server); it is returned when this node is the Raft leader.
func (tm *TopicManager) GetRaftLeaderRPCAddr() (string, error) {
	if tm.coordinator == nil {
		return "", fmt.Errorf("topic: no coordinator")
	}
	leaderNodeID, err := tm.coordinator.GetRaftLeaderNodeID()
	if err != nil {
		return "", err
	}
	tm.mu.RLock()
	node := tm.Nodes[leaderNodeID]
	tm.mu.RUnlock()
	if node == nil {
		return "", fmt.Errorf("raft leader node %q not in metadata", leaderNodeID)
	}
	return node.RpcAddr, nil
}

// CreateTopic applies a CreateTopic event via Raft and returns the chosen leader and replica set.
// Must be called on the Raft leader (client should use GetRaftLeader first). Replicas are created
// when each node applies the event in Apply() via createTopicFromEvent.
func (tm *TopicManager) CreateTopic(ctx context.Context, req *protocol.CreateTopicRequest) (*protocol.CreateTopicResponse, error) {
	if tm.coordinator == nil {
		return nil, fmt.Errorf("topic: no coordinator")
	}
	c := tm.coordinator
	if !c.IsLeader() {
		return nil, fmt.Errorf("create topic must be sent to Raft leader: %w", ErrCannotReachLeader)
	}
	tm.mu.RLock()
	_, exists := tm.Topics[req.Topic]
	tm.mu.RUnlock()
	if exists {
		return nil, ErrTopicExistsf(req.Topic)
	}
	leaderNodeID, err := tm.GetNodeIDWithLeastTopics()
	if err != nil {
		return nil, err
	}
	replicaNodeIds, err := tm.pickReplicaNodeIds(leaderNodeID, int(req.ReplicaCount))
	if err != nil {
		return nil, ErrCreateTopic(err)
	}
	tm.Logger.Info("create topic via Raft", zap.String("topic", req.Topic), zap.String("leader_node_id", leaderNodeID), zap.Strings("replica_node_ids", replicaNodeIds))
	if err := c.ApplyCreateTopicEvent(req.Topic, req.ReplicaCount, leaderNodeID, replicaNodeIds); err != nil {
		return nil, err
	}
	return &protocol.CreateTopicResponse{Topic: req.Topic, ReplicaNodeIds: replicaNodeIds}, nil
}

func (tm *TopicManager) GetNodeIDWithLeastTopics() (string, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	countByNode := make(map[string]int)
	for _, node := range tm.Nodes {
		if node != nil {
			countByNode[node.NodeID] = 0
		}
	}
	for _, t := range tm.Topics {
		if t == nil {
			continue
		}
		if leaderID := t.LeaderID(); leaderID != "" {
			countByNode[leaderID]++
		}
	}
	if len(countByNode) == 0 {
		return "", ErrNoNodesInCluster
	}
	// Deterministic tie-breaking: pick the node ID with the smallest topic count;
	// when counts are equal, pick lexicographically smallest node ID.
	ids := make([]string, 0, len(countByNode))
	for id := range countByNode {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	bestID := ids[0]
	minCount := countByNode[bestID]
	for _, id := range ids[1:] {
		if c := countByNode[id]; c < minCount {
			minCount = c
			bestID = id
		}
	}
	return bestID, nil
}

// pickReplicaNodeIds returns up to replicaCount node IDs from the cluster, excluding leaderNodeID.
func (tm *TopicManager) pickReplicaNodeIds(leaderNodeID string, replicaCount int) ([]string, error) {
	tm.mu.RLock()
	var otherNodes []*NodeMetadata
	for _, node := range tm.Nodes {
		if node != nil && node.NodeID != leaderNodeID {
			otherNodes = append(otherNodes, node)
		}
	}
	tm.mu.RUnlock()
	if len(otherNodes) < replicaCount {
		return nil, ErrNotEnoughNodesf(replicaCount, len(otherNodes))
	}
	replicaNodeIds := make([]string, 0, replicaCount)
	for i := 0; i < replicaCount; i++ {
		replicaNodeIds = append(replicaNodeIds, otherNodes[i].NodeID)
	}
	return replicaNodeIds, nil
}

// DeleteTopic applies a DeleteTopic event via Raft. Must be called on the Raft leader (client should use GetRaftLeader first).
// Replicas are removed when each node applies the event in Apply() via deleteTopicFromEvent.
func (tm *TopicManager) DeleteTopic(ctx context.Context, req *protocol.DeleteTopicRequest) (*protocol.DeleteTopicResponse, error) {
	if tm.coordinator == nil {
		return nil, fmt.Errorf("topic: no coordinator")
	}
	c := tm.coordinator
	if !c.IsLeader() {
		return nil, fmt.Errorf("delete topic must be sent to Raft leader: %w", ErrCannotReachLeader)
	}
	tm.mu.RLock()
	_, exists := tm.Topics[req.Topic]
	tm.mu.RUnlock()
	if !exists {
		return nil, ErrTopicNotFoundf(req.Topic)
	}
	tm.Logger.Info("delete topic via Raft", zap.String("topic", req.Topic))
	if err := c.ApplyDeleteTopicEventInternal(req.Topic); err != nil {
		return nil, ErrApplyDeleteTopic(err)
	}
	return &protocol.DeleteTopicResponse{Topic: req.Topic}, nil
}

// GetLeader returns the leader log view for a topic (this node must be the leader).
func (tm *TopicManager) GetLeader(topic string) (*log.LogManager, error) {
	tm.mu.RLock()
	topicObj, ok := tm.Topics[topic]
	currentNodeID := tm.CurrentNodeID
	tm.mu.RUnlock()
	if !ok {
		return nil, ErrTopicNotFoundf(topic)
	}
	if topicObj.LeaderID() != currentNodeID {
		return nil, ErrThisNodeNotLeaderf(topic)
	}
	return topicObj.GetLog(), nil
}

// GetTopic returns the topic object.
func (tm *TopicManager) GetTopic(topic string) (*Topic, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	topicObj, ok := tm.Topics[topic]
	if !ok {
		return nil, ErrTopicNotFoundf(topic)
	}
	return topicObj, nil
}

// ListTopics returns topic names with leader and replica info. Any node can serve this (metadata is replicated).
func (tm *TopicManager) ListTopics() *protocol.ListTopicsResponse {
	tm.mu.RLock()
	names := make([]string, 0, len(tm.Topics))
	topics := make([]*Topic, 0, len(tm.Topics))
	for name, t := range tm.Topics {
		if t == nil {
			continue
		}
		names = append(names, name)
		topics = append(topics, t)
	}
	tm.mu.RUnlock()

	out := make([]protocol.TopicInfo, 0, len(topics))
	for i, t := range topics {
		leaderID, epoch, replicaSnaps := t.Snapshot()
		replicas := make([]protocol.ReplicaInfo, 0, len(replicaSnaps))
		for _, rs := range replicaSnaps {
			replicas = append(replicas, protocol.ReplicaInfo{
				NodeID: rs.ReplicaNodeID,
				IsISR:  rs.IsISR,
				LEO:    rs.LEO,
			})
		}
		out = append(out, protocol.TopicInfo{
			Name:         names[i],
			LeaderNodeID: leaderID,
			LeaderEpoch:  epoch,
			Replicas:     replicas,
		})
	}
	return &protocol.ListTopicsResponse{Topics: out}
}

// RestoreFromMetadata rebuilds local logs from in-memory metadata (Topics/Nodes).
// Call after Restore() has populated Topics and Nodes from snapshot so local logs are opened where this node is leader or replica.
func (tm *TopicManager) RestoreFromMetadata() error {
	tm.mu.RLock()
	currentNodeID := tm.CurrentNodeID
	topicNames := make([]string, 0, len(tm.Topics))
	for name := range tm.Topics {
		topicNames = append(topicNames, name)
	}
	tm.mu.RUnlock()
	if len(topicNames) == 0 {
		return nil
	}
	tm.Logger.Info("restore from metadata", zap.Int("topic_count", len(topicNames)), zap.Strings("topics", topicNames))
	for _, topicName := range topicNames {
		t := tm.lookupTopic(topicName)
		leaderID := ""
		if t != nil {
			leaderID = t.LeaderID()
		}
		if leaderID == "" {
			continue
		}
		if leaderID == currentNodeID {
			if err := tm.restoreLeaderTopic(topicName); err != nil {
				tm.Logger.Warn("restore leader topic failed", zap.String("topic", topicName), zap.Error(err))
				continue
			}
		} else {
			if err := tm.restoreReplicaTopic(topicName, leaderID); err != nil {
				tm.Logger.Warn("restore replica topic failed", zap.String("topic", topicName), zap.Error(err))
				continue
			}
		}
	}
	return nil
}

// restoreLeaderTopic opens the local leader log for the topic if not already open.
func (tm *TopicManager) restoreLeaderTopic(topic string) error {
	t := tm.lookupTopic(topic)
	if t == nil {
		return nil
	}
	if t.GetLog() != nil {
		return nil
	}
	logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topic))
	if err != nil {
		return ErrCreateLog(err)
	}
	t.SetLog(logManager)
	tm.Logger.Info("leader topic restored", zap.String("topic", topic))
	return nil
}

// restoreReplicaTopic creates a replica for the topic on this node (called by leader via RPC).
func (tm *TopicManager) restoreReplicaTopic(topic string, leaderId string) error {
	tm.mu.Lock()
	topicObj, ok := tm.Topics[topic]
	if !ok {
		topicObj = &Topic{
			Name:         topic,
			LeaderNodeID: leaderId,
			Replicas:     make(map[string]*ReplicaState),
			Logger:       tm.Logger,
		}
		tm.Topics[topic] = topicObj
	}
	tm.mu.Unlock()

	if topicObj.GetLog() != nil {
		return ErrTopicAlreadyReplicaf(topic)
	}
	logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topic))
	if err != nil {
		return ErrCreateLogReplica(err)
	}
	topicObj.SetLog(logManager)
	tm.Logger.Info("replica topic restored", zap.String("topic", topic), zap.String("leader_id", leaderId))
	return nil
}

// ReplicationTarget implementation (TopicManager runs the replication thread and uses these).

func (tm *TopicManager) ListReplicaTopics() []ReplicaTopicInfo {
	tm.mu.RLock()
	names := make([]string, 0, len(tm.Topics))
	topics := make([]*Topic, 0, len(tm.Topics))
	currentNodeID := tm.CurrentNodeID
	for name, t := range tm.Topics {
		if t == nil {
			continue
		}
		names = append(names, name)
		topics = append(topics, t)
	}
	tm.mu.RUnlock()

	var out []ReplicaTopicInfo
	for i, t := range topics {
		if t.GetLog() == nil {
			continue
		}
		leaderID := t.LeaderID()
		if leaderID == currentNodeID {
			continue
		}
		out = append(out, ReplicaTopicInfo{TopicName: names[i], LeaderNodeID: leaderID})
	}
	return out
}

func (tm *TopicManager) GetLEO(topicName string) (uint64, bool) {
	t := tm.lookupTopic(topicName)
	if t == nil {
		return 0, false
	}
	l := t.GetLog()
	if l == nil {
		return 0, false
	}
	return l.LEO(), true
}

func (tm *TopicManager) ApplyChunk(topicName string, rawChunk []byte) error {
	if len(rawChunk) == 0 {
		return nil
	}
	records, err := protocol.DecodeReplicationBatch(rawChunk)
	if err != nil {
		return err
	}
	t := tm.lookupTopic(topicName)
	if t == nil {
		return nil
	}
	l := t.GetLog()
	if l == nil {
		return nil
	}
	values := make([][]byte, len(records))
	for i, rec := range records {
		values[i] = rec.Value
	}
	_, err = l.AppendBatch(values)
	return err
}

// ApplyRecord appends a single record to the topic log (used by replica when replicating via Fetch).
func (tm *TopicManager) ApplyRecord(topicName string, value []byte) error {
	if len(value) == 0 {
		return nil
	}
	t := tm.lookupTopic(topicName)
	if t == nil {
		return nil
	}
	l := t.GetLog()
	if l == nil {
		return nil
	}
	_, err := l.Append(value)
	return err
}

// ApplyRecordBatch appends multiple records to the topic log in one batch (used by replication).
func (tm *TopicManager) ApplyRecordBatch(topicName string, values [][]byte) error {
	if len(values) == 0 {
		return nil
	}
	t := tm.lookupTopic(topicName)
	if t == nil {
		return nil
	}
	l := t.GetLog()
	if l == nil {
		return nil
	}
	_, err := l.AppendBatch(values)
	return err
}

// RecordReplicaLEOFromFetch is called by the leader when it serves a Fetch from a replica (ReplicaNodeID set).
// It updates the replica's LEO and applies an ISR update via Raft.
func (tm *TopicManager) RecordReplicaLEOFromFetch(ctx context.Context, topicName, replicaNodeID string, leo int64) error {
	t := tm.lookupTopic(topicName)
	if t == nil {
		return nil
	}
	lagThreshold := tm.ISRLagThreshold
	if lagThreshold == 0 {
		lagThreshold = DefaultISRLagThreshold
	}
	isr := t.RecordReplicaFetch(replicaNodeID, leo, lagThreshold)
	if tm.coordinator == nil {
		return nil
	}
	return tm.coordinator.ApplyIsrUpdateEventInternal(topicName, replicaNodeID, isr)
}

func (tm *TopicManager) Apply(ev *protocol.MetadataEvent) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	switch ev.EventType {
	case protocol.MetadataEventTypeCreateTopic:
		e, err := protocol.DecodeCreateTopicEvent(ev.Data)
		if err != nil {
			return err
		}
		tm.createTopicFromEvent(e.Topic, e.LeaderNodeID, e.LeaderEpoch, e.ReplicaNodeIds)
	case protocol.MetadataEventTypeLeaderChange:
		e, err := protocol.DecodeLeaderChangeEvent(ev.Data)
		if err != nil {
			return err
		}
		t := tm.Topics[e.Topic]
		if t != nil {
			oldLeaderID := t.SetLeader(e.LeaderNodeID, e.LeaderEpoch)
			// Only add the old leader as a replica if it's still a live node in the cluster.
			// If the old leader was removed (node killed), it was already cleaned up from Replicas
			// by the RemoveNode handler. Adding a dead node would pin HW at 0.
			if oldLeaderID != e.LeaderNodeID && oldLeaderID != "" && tm.Nodes[oldLeaderID] != nil {
				t.AddReplicaIfAbsent(oldLeaderID, false)
			}
			tm.ensureLocalLogAfterLeaderChange(e.Topic, oldLeaderID, e.LeaderNodeID)
		}
	case protocol.MetadataEventTypeIsrUpdate:
		e, err := protocol.DecodeIsrUpdateEvent(ev.Data)
		if err != nil {
			return err
		}
		t := tm.Topics[e.Topic]
		if t != nil {
			// SetReplicaISR creates the replica entry with LEO 0 if absent (e.g. node
			// restarted, or event order) and advances the high watermark.
			t.SetReplicaISR(e.ReplicaNodeID, e.Isr)
		}
	case protocol.MetadataEventTypeDeleteTopic:
		e, err := protocol.DecodeDeleteTopicEvent(ev.Data)
		if err != nil {
			return err
		}
		tm.deleteTopicFromEvent(e.Topic)
	case protocol.MetadataEventTypeAddNode:
		e, err := protocol.DecodeAddNodeEvent(ev.Data)
		if err != nil {
			return err
		}
		tm.Nodes[e.NodeID] = &NodeMetadata{
			NodeID:  e.NodeID,
			Addr:    e.Addr,
			RpcAddr: e.RpcAddr,
		}
		// When a node (re)joins, add it as a replica for topics that are below their
		// desired replica count and where this node is not already leader or replica.
		tm.maybeAddReplicasForNode(e.NodeID)
	case protocol.MetadataEventTypeRemoveNode:
		e, err := protocol.DecodeRemoveNodeEvent(ev.Data)
		if err != nil {
			return err
		}
		delete(tm.Nodes, e.NodeID)
		// Dead node stays in Replicas so it can resume replication when it comes back.
		// HW is not affected because maybeAdvanceHW only considers ISR replicas.
		tm.maybeReassignTopicLeaders(e.NodeID)
	case protocol.MetadataEventTypeUpdateNode:
		if _, err := protocol.DecodeUpdateNodeEvent(ev.Data); err != nil {
			return err
		}
		//TODO: update node status
	default:
		return fmt.Errorf("unknown event type: %d", ev.EventType)
	}
	return nil
}

// createTopicFromEvent creates the topic locally from a CreateTopic event (leader + replica IDs).
// Caller holds tm.mu. Used when applying MetadataEventTypeCreateTopic in Apply().
// Idempotent: if topic already exists, this is a no-op (guards against TOCTOU races in CreateTopic).
func (tm *TopicManager) createTopicFromEvent(topicName, leaderNodeID string, leaderEpoch int64, replicaNodeIds []string) {
	if _, exists := tm.Topics[topicName]; exists {
		return
	}
	t := &Topic{
		Name:                topicName,
		LeaderNodeID:        leaderNodeID,
		LeaderEpoch:         leaderEpoch,
		DesiredReplicaCount: len(replicaNodeIds),
		Replicas:            make(map[string]*ReplicaState),
		Logger:              tm.Logger,
	}
	for _, replica := range replicaNodeIds {
		t.Replicas[replica] = &ReplicaState{
			ReplicaNodeID: replica,
			LEO:           0,
			IsISR:         true,
		}
	}
	tm.Topics[topicName] = t
	tm.ensureLocalLogForTopic(topicName, leaderNodeID, replicaNodeIds)
}

// ensureLocalLogForTopic opens the local log for the topic if this node is leader or replica. Caller holds tm.mu.
func (tm *TopicManager) ensureLocalLogForTopic(topicName, leaderNodeID string, replicaNodeIds []string) {
	t := tm.Topics[topicName]
	if t == nil {
		return
	}
	if tm.CurrentNodeID == leaderNodeID {
		if t.GetLog() == nil {
			logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topicName))
			if err != nil {
				tm.Logger.Warn("open leader log failed", zap.String("topic", topicName), zap.Error(err))
				return
			}
			t.SetLog(logManager)
			tm.Logger.Debug("leader log opened", zap.String("topic", topicName))
		}
		return
	}
	for _, rid := range replicaNodeIds {
		if rid == tm.CurrentNodeID {
			if t.GetLog() == nil {
				logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topicName))
				if err != nil {
					tm.Logger.Warn("open replica log failed", zap.String("topic", topicName), zap.Error(err))
					return
				}
				t.SetLog(logManager)
				tm.Logger.Debug("replica log opened", zap.String("topic", topicName), zap.String("leader_id", leaderNodeID))
			}
			return
		}
	}
}

// ensureLocalLogAfterLeaderChange updates local log after leader change (promote or demote). Caller holds tm.mu.
func (tm *TopicManager) ensureLocalLogAfterLeaderChange(topicName, oldLeaderID, newLeaderID string) {
	t := tm.Topics[topicName]
	if t == nil {
		return
	}
	// This node is the new leader — open log if needed.
	if tm.CurrentNodeID == newLeaderID {
		if t.GetLog() == nil {
			logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topicName))
			if err != nil {
				tm.Logger.Warn("open leader log failed", zap.String("topic", topicName), zap.Error(err))
				return
			}
			t.SetLog(logManager)
		}
		tm.Logger.Info("promoted to leader", zap.String("topic", topicName))
		return
	}
	// This node was the old leader — keep log open for replication as a follower.
	if tm.CurrentNodeID == oldLeaderID {
		tm.Logger.Info("demoted from leader", zap.String("topic", topicName), zap.String("new_leader", newLeaderID))
		return
	}
	// This node is a replica — no action needed (leaderNodeID already updated by caller).
}

// deleteTopicFromEvent removes the topic locally when applying a DeleteTopic event. Caller holds tm.mu.
func (tm *TopicManager) deleteTopicFromEvent(topicName string) {
	tm.removeTopicLocalLocked(topicName)
}

// removeTopicLocalLocked closes the topic log, removes from map, and deletes the topic dir. Caller holds tm.mu.
func (tm *TopicManager) removeTopicLocalLocked(topicName string) {
	t, ok := tm.Topics[topicName]
	if !ok {
		return
	}
	if t != nil {
		if l := t.GetLog(); l != nil {
			l.Close()
			l.Delete()
			t.SetLog(nil)
		}
	}
	delete(tm.Topics, topicName)
	_ = os.RemoveAll(filepath.Join(tm.BaseDir, topicName))
	tm.Logger.Info("topic removed", zap.String("topic", topicName))
}

// maybeReassignTopicLeaders is called from Apply() (FSM goroutine) when a node is removed.
// It must NOT call raft.Apply synchronously — that would deadlock the FSM goroutine
// (FSM.Apply waits for the new entry to be applied, but the FSM is blocked in the current Apply).
// Instead, it collects the needed changes and applies them asynchronously in a goroutine.
func (tm *TopicManager) maybeReassignTopicLeaders(nodeID string) {
	if tm.coordinator == nil || !tm.coordinator.IsLeader() {
		return
	}

	type leaderChange struct {
		topic     string
		newLeader string
		epoch     int64
	}
	var changes []leaderChange

	for topicName, t := range tm.Topics {
		if t == nil {
			continue
		}
		leaderNodeID, epoch, replicas := t.Snapshot()
		if leaderNodeID != nodeID {
			continue
		}
		var newLeader string
		for _, rs := range replicas {
			if rs.ReplicaNodeID == nodeID || !rs.IsISR {
				continue
			}
			if tm.Nodes[rs.ReplicaNodeID] == nil {
				continue
			}
			newLeader = rs.ReplicaNodeID
			break
		}
		if newLeader == "" {
			tm.Logger.Warn("no ISR replica for leadership", zap.String("topic", topicName), zap.String("old_leader_node_id", nodeID))
			continue
		}
		changes = append(changes, leaderChange{
			topic:     topicName,
			newLeader: newLeader,
			epoch:     epoch + 1,
		})
	}

	if len(changes) == 0 {
		return
	}

	// Apply leader changes asynchronously so the FSM's current Apply() can return first.
	go func() {
		for _, ch := range changes {
			if err := tm.coordinator.ApplyLeaderChangeEvent(ch.topic, ch.newLeader, ch.epoch); err != nil {
				tm.Logger.Warn("leader change apply failed", zap.String("topic", ch.topic), zap.Error(err))
			}
		}
	}()
}

// maybeAddReplicasForNode is called from Apply() (AddNode) when a node (re)joins the cluster.
// For each topic below its desired replica count where this node is not leader or already a replica,
// add the node as a non-ISR replica. Since this runs inside FSM.Apply(), the change is Raft-replicated.
// The node will open its local log via Restore/ensureLocalLogForTopic and start replicating.
func (tm *TopicManager) maybeAddReplicasForNode(nodeID string) {
	for _, t := range tm.Topics {
		if t == nil || t.LeaderID() == nodeID {
			continue
		}
		if t.HasReplica(nodeID) {
			continue
		}
		if t.ReplicaCount() >= t.DesiredReplicaCount {
			continue
		}
		t.AddReplicaIfAbsent(nodeID, false)
		tm.Logger.Info("added rejoined node as replica",
			zap.String("topic", t.Name),
			zap.String("node_id", nodeID),
		)
	}
}

// Snapshot serializes the topic/node metadata (not runtime-only fields like open
// log handles or loggers) as protobuf, for Raft snapshot persistence.
func (tm *TopicManager) Snapshot() ([]byte, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return proto.Marshal(snapshotToPB(tm.Topics, tm.Nodes))
}

func (tm *TopicManager) Restore(data []byte) error {
	var m pb.MetadataSnapshot
	if err := proto.Unmarshal(data, &m); err != nil {
		return err
	}
	topics, nodes := pbToSnapshot(&m)
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.Topics = topics
	tm.Nodes = nodes
	// CurrentNodeID is not restored; it is set from config via SetCurrentNodeID.
	for name, t := range tm.Topics {
		if t == nil {
			continue
		}
		t.Logger = tm.Logger
		if tm.CurrentNodeID == t.LeaderNodeID {
			if t.GetLog() == nil {
				logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, name))
				if err != nil {
					tm.Logger.Warn("open leader log failed", zap.String("topic", name), zap.Error(err))
					continue
				}
				t.SetLog(logManager)
			}
			// Initialize HW from local state so consumers can read immediately
			// if no replicas exist (otherwise HW stays 0 until replicas report in).
			t.AdvanceHW()
		} else if t.HasReplica(tm.CurrentNodeID) {
			if t.GetLog() == nil {
				logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, name))
				if err != nil {
					tm.Logger.Warn("open replica log failed", zap.String("topic", name), zap.Error(err))
					continue
				}
				t.SetLog(logManager)
			}
		}
	}
	return nil
}

func (tm *TopicManager) periodicLog(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-tm.stopPeriodic:
			return
		case <-ticker.C:
			tm.mu.RLock()
			b, err := json.Marshal(tm)
			// Build local log LEO summary (not in metadata, each node knows its own).
			localLEOs := make(map[string]uint64, len(tm.Topics))
			for name, t := range tm.Topics {
				if t == nil {
					continue
				}
				if l := t.GetLog(); l != nil {
					localLEOs[name] = l.LEO()
				}
			}
			tm.mu.RUnlock()
			if err != nil {
				tm.Logger.Warn("metadata periodic log marshal error", zap.Error(err))
				continue
			}
			leoBytes, _ := json.Marshal(localLEOs)
			tm.Logger.Info("metadata store",
				zap.String("state", string(b)),
				zap.String("local_leo", string(leoBytes)),
			)
		}
	}
}

func (tm *TopicManager) StopPeriodicLog() {
	select {
	case <-tm.stopPeriodic:
		return
	default:
		close(tm.stopPeriodic)
	}
}
