package topic

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mohitkumar/mlog/api/protocol"
	raft "github.com/mohitkumar/mlog/broker/cluster/raft"
	"github.com/mohitkumar/mlog/broker/log"
	"go.uber.org/zap"
)

const defaultMetadataLogInterval = 30 * time.Second

// Topic is the per-broker runtime state for a topic: its locally-open log (only
// present on nodes that are currently leader or replica for it) and nothing else.
// Cluster-wide state (leader, epoch, replica set, ISR) lives behind TopicCoordinator,
// not here — see TopicManager.coordinator.
type Topic struct {
	mu   sync.RWMutex
	Name string          `json:"name"`
	Log  *log.LogManager `json:"-"`
}

type TopicManager struct {
	mu                   sync.RWMutex
	Topics               map[string]*Topic // local runtime state only: open log handles, keyed by topic name
	BaseDir              string
	Logger               *zap.Logger
	CurrentNodeID        string // Local node ID from config; not persisted.
	coordinator          TopicCoordinator
	stopPeriodic         chan struct{}
	stopReplication      chan struct{}
	replicationBatchSize uint32
	ISRLagThreshold      uint64 // max record lag for ISR membership
}

// NewTopicManager creates a TopicManager. coord is the cluster coordinator (real
// *cluster.Cluster or a test fake) — the sole source of cluster-wide (Raft-replicated)
// topic metadata; TopicManager never stores that metadata itself, only queries it.
func NewTopicManager(baseDir string, coord TopicCoordinator, logger *zap.Logger) (*TopicManager, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	tm := &TopicManager{
		Topics:          make(map[string]*Topic),
		BaseDir:         baseDir,
		Logger:          logger,
		coordinator:     coord,
		stopPeriodic:    make(chan struct{}),
		ISRLagThreshold: DefaultISRLagThreshold,
	}
	go tm.periodicLog(defaultMetadataLogInterval)
	tm.replicationBatchSize = DefaultReplicationBatchSize
	return tm, nil
}

// SetCurrentNodeID sets this node's ID (from config); not persisted in Raft state.
func (tm *TopicManager) SetCurrentNodeID(nodeID string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.CurrentNodeID = nodeID
}

func (tm *TopicManager) currentNodeID() string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.CurrentNodeID
}

func (tm *TopicManager) IsLeader(topic string) (bool, error) {
	leaderID, ok := tm.coordinator.TopicLeaderNodeID(topic)
	if !ok {
		return false, ErrTopicNotFoundf(topic)
	}
	return leaderID == tm.currentNodeID(), nil
}

// GetTopicLeaderRPCAddr returns the RPC address of the current leader for the given topic.
func (tm *TopicManager) GetTopicLeaderRPCAddr(topic string) (string, error) {
	leaderID, ok := tm.coordinator.TopicLeaderNodeID(topic)
	if !ok {
		return "", ErrTopicNotFoundf(topic)
	}
	addr, ok := tm.coordinator.NodeRPCAddr(leaderID)
	if !ok {
		return "", ErrTopicNotFoundf(topic)
	}
	return addr, nil
}

// lookupTopic returns the local runtime Topic for name, or nil if not found. Safe for concurrent use.
func (tm *TopicManager) lookupTopic(name string) *Topic {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.Topics[name]
}

// ensureLocalTopic returns the local runtime Topic for name, creating an empty one if absent.
func (tm *TopicManager) ensureLocalTopic(name string) *Topic {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	t, ok := tm.Topics[name]
	if !ok {
		t = &Topic{Name: name}
		tm.Topics[name] = t
	}
	return t
}

// GetRaftLeaderRPCAddr returns the RPC address of the current Raft (metadata) leader.
func (tm *TopicManager) GetRaftLeaderRPCAddr() (string, error) {
	leaderNodeID, err := tm.coordinator.GetRaftLeaderNodeID()
	if err != nil {
		return "", err
	}
	addr, ok := tm.coordinator.NodeRPCAddr(leaderNodeID)
	if !ok {
		return "", fmt.Errorf("raft leader node %q not in metadata", leaderNodeID)
	}
	return addr, nil
}

// CreateTopic applies a CreateTopic event via Raft and returns the chosen leader and replica set.
// Must be called on the Raft leader (client should use GetRaftLeader first). Replicas are created
// when each node applies the event via HandleMetadataEvent's ensureLocalLogForTopic.
func (tm *TopicManager) CreateTopic(ctx context.Context, req *protocol.CreateTopicRequest) (*protocol.CreateTopicResponse, error) {
	c := tm.coordinator
	if !c.IsLeader() {
		return nil, fmt.Errorf("create topic must be sent to Raft leader: %w", ErrCannotReachLeader)
	}
	if c.TopicExists(req.Topic) {
		return nil, ErrTopicExistsf(req.Topic)
	}
	candidates := c.AliveNodeIDs()
	leaderNodeID, err := c.NodeIDWithLeastTopics(candidates)
	if err != nil {
		return nil, err
	}
	replicaNodeIds, err := PickReplicaNodeIds(leaderNodeID, int(req.ReplicaCount), candidates)
	if err != nil {
		return nil, ErrCreateTopic(err)
	}
	tm.Logger.Info("create topic via Raft", zap.String("topic", req.Topic), zap.String("leader_node_id", leaderNodeID), zap.Strings("replica_node_ids", replicaNodeIds))
	if err := c.ApplyCreateTopicEvent(req.Topic, req.ReplicaCount, leaderNodeID, replicaNodeIds); err != nil {
		return nil, err
	}
	return &protocol.CreateTopicResponse{Topic: req.Topic, ReplicaNodeIds: replicaNodeIds}, nil
}

// DeleteTopic applies a DeleteTopic event via Raft. Must be called on the Raft leader (client should use GetRaftLeader first).
func (tm *TopicManager) DeleteTopic(ctx context.Context, req *protocol.DeleteTopicRequest) (*protocol.DeleteTopicResponse, error) {
	c := tm.coordinator
	if !c.IsLeader() {
		return nil, fmt.Errorf("delete topic must be sent to Raft leader: %w", ErrCannotReachLeader)
	}
	if !c.TopicExists(req.Topic) {
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
	leaderID, ok := tm.coordinator.TopicLeaderNodeID(topic)
	if !ok {
		return nil, ErrTopicNotFoundf(topic)
	}
	if leaderID != tm.currentNodeID() {
		return nil, ErrThisNodeNotLeaderf(topic)
	}
	t := tm.lookupTopic(topic)
	if t == nil {
		return nil, ErrTopicNotFoundf(topic)
	}
	return t.GetLog(), nil
}

// GetTopic returns the local runtime topic object (open log handle).
func (tm *TopicManager) GetTopic(topic string) (*Topic, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	t, ok := tm.Topics[topic]
	if !ok {
		return nil, ErrTopicNotFoundf(topic)
	}
	return t, nil
}

// ListTopics returns topic names with leader and replica info. Any node can serve this (metadata is replicated).
func (tm *TopicManager) ListTopics() *protocol.ListTopicsResponse {
	names := tm.coordinator.TopicNames()
	out := make([]protocol.TopicInfo, 0, len(names))
	for _, name := range names {
		info, ok := tm.coordinator.TopicInfo(name)
		if !ok {
			continue
		}
		out = append(out, info)
	}
	return &protocol.ListTopicsResponse{Topics: out}
}

// RestoreFromMetadata rebuilds local logs from cluster metadata. Call after Restore()
// has populated the metadata store from snapshot, so local logs are opened where this
// node is leader or replica.
func (tm *TopicManager) RestoreFromMetadata() error {
	currentNodeID := tm.currentNodeID()
	topicNames := tm.coordinator.TopicNames()
	if len(topicNames) == 0 {
		return nil
	}
	tm.Logger.Info("restore from metadata", zap.Int("topic_count", len(topicNames)), zap.Strings("topics", topicNames))
	for _, topicName := range topicNames {
		leaderID, ok := tm.coordinator.TopicLeaderNodeID(topicName)
		if !ok || leaderID == "" {
			continue
		}
		if leaderID == currentNodeID {
			if err := tm.restoreLeaderTopic(topicName); err != nil {
				tm.Logger.Warn("restore leader topic failed", zap.String("topic", topicName), zap.Error(err))
				continue
			}
			// Initialize HW from local state so consumers can read immediately if no
			// replicas exist (otherwise HW stays 0 until replicas report in).
			if t := tm.lookupTopic(topicName); t != nil {
				if l := t.GetLog(); l != nil {
					l.SetHighWatermark(tm.coordinator.TopicMinISRLeo(topicName, l.LEO()))
				}
			}
		} else if tm.coordinator.TopicHasReplica(topicName, currentNodeID) {
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
	t := tm.ensureLocalTopic(topic)
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
	t := tm.ensureLocalTopic(topic)
	if t.GetLog() != nil {
		return ErrTopicAlreadyReplicaf(topic)
	}
	logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topic))
	if err != nil {
		return ErrCreateLogReplica(err)
	}
	t.SetLog(logManager)
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
		leaderID, ok := tm.coordinator.TopicLeaderNodeID(names[i])
		if !ok || leaderID == currentNodeID {
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
	var localLEO uint64
	var l *log.LogManager
	if t != nil {
		l = t.GetLog()
		if l != nil {
			localLEO = l.LEO()
		}
	}
	lagThreshold := tm.ISRLagThreshold
	if lagThreshold == 0 {
		lagThreshold = DefaultISRLagThreshold
	}
	isr, ok := tm.coordinator.RecordReplicaFetch(topicName, replicaNodeID, leo, lagThreshold, localLEO)
	if !ok {
		return nil
	}
	if l != nil {
		l.SetHighWatermark(tm.coordinator.TopicMinISRLeo(topicName, localLEO))
	}
	return tm.coordinator.ApplyIsrUpdateEventInternal(topicName, replicaNodeID, isr)
}

// HandleMetadataEvent reacts to a metadata event that has already been applied to the
// canonical, Raft-replicated cluster metadata (see TopicCoordinator.SetOnMetadataEvent)
// with any local side effects that follow from it: opening or closing this node's
// on-disk log.
func (tm *TopicManager) HandleMetadataEvent(ev *raft.MetadataEvent) error {
	switch ev.EventType {
	case raft.MetadataEventTypeCreateTopic:
		e, err := raft.DecodeCreateTopicEvent(ev.Data)
		if err != nil {
			return err
		}
		tm.ensureLocalLogForTopic(e.Topic, e.LeaderNodeID, e.ReplicaNodeIds)
	case raft.MetadataEventTypeLeaderChange:
		e, err := raft.DecodeLeaderChangeEvent(ev.Data)
		if err != nil {
			return err
		}
		tm.applyLocalLeaderChange(e.Topic, e.LeaderNodeID)
	case raft.MetadataEventTypeDeleteTopic:
		e, err := raft.DecodeDeleteTopicEvent(ev.Data)
		if err != nil {
			return err
		}
		tm.removeLocalTopic(e.Topic)
	case raft.MetadataEventTypeIsrUpdate:
		// No local side effect: ISR membership is metadata-store-only.
	default:
		return fmt.Errorf("unknown event type: %d", ev.EventType)
	}
	return nil
}

// ensureLocalLogForTopic opens the local log for the topic if this node is leader or replica.
func (tm *TopicManager) ensureLocalLogForTopic(topicName, leaderNodeID string, replicaNodeIds []string) {
	currentNodeID := tm.currentNodeID()
	isLocal := currentNodeID == leaderNodeID
	if !isLocal {
		for _, rid := range replicaNodeIds {
			if rid == currentNodeID {
				isLocal = true
				break
			}
		}
	}
	if !isLocal {
		return
	}
	t := tm.ensureLocalTopic(topicName)
	if t.GetLog() != nil {
		return
	}
	logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topicName))
	if err != nil {
		tm.Logger.Warn("open log failed", zap.String("topic", topicName), zap.Error(err))
		return
	}
	t.SetLog(logManager)
	tm.Logger.Debug("local log opened", zap.String("topic", topicName), zap.String("leader_id", leaderNodeID))
}

// applyLocalLeaderChange opens the local log if this node was just promoted to leader.
func (tm *TopicManager) applyLocalLeaderChange(topicName, newLeaderNodeID string) {
	currentNodeID := tm.currentNodeID()
	_, wasLocal := tm.Topics[topicName]
	if currentNodeID == newLeaderNodeID {
		t := tm.ensureLocalTopic(topicName)
		if t.GetLog() == nil {
			logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topicName))
			if err != nil {
				tm.Logger.Warn("open leader log failed", zap.String("topic", topicName), zap.Error(err))
			} else {
				t.SetLog(logManager)
			}
		}
		tm.Logger.Info("promoted to leader", zap.String("topic", topicName))
	} else if wasLocal {
		tm.Logger.Info("leader changed", zap.String("topic", topicName), zap.String("new_leader", newLeaderNodeID))
	}
}

// removeLocalTopic closes the topic log, removes it from local bookkeeping, and deletes the topic dir.
func (tm *TopicManager) removeLocalTopic(topicName string) {
	tm.mu.Lock()
	t, ok := tm.Topics[topicName]
	if ok {
		delete(tm.Topics, topicName)
	}
	tm.mu.Unlock()
	if t != nil {
		if l := t.GetLog(); l != nil {
			l.Close()
			l.Delete()
		}
	}
	_ = os.RemoveAll(filepath.Join(tm.BaseDir, topicName))
	tm.Logger.Info("topic removed", zap.String("topic", topicName))
}

// ReassignLeadersForDeadNode reassigns leadership (to an ISR replica) for every topic
// nodeID was leading. Registered as the cluster's on-node-removed callback so it runs
// when Cluster observes a node drop out (explicit Leave, or reconciliation against
// Serf). Only the current Raft leader actually proposes anything — every other node's
// callback fires too but no-ops, since only the Raft leader can successfully propose
// the resulting leader-change event anyway.
func (tm *TopicManager) ReassignLeadersForDeadNode(nodeID string) {
	if !tm.coordinator.IsLeader() {
		return
	}
	type leaderChange struct {
		topic     string
		newLeader string
		epoch     int64
	}
	var changes []leaderChange
	for _, topicName := range tm.coordinator.TopicNames() {
		info, ok := tm.coordinator.TopicInfo(topicName)
		if !ok || info.LeaderNodeID != nodeID {
			continue
		}
		var newLeader string
		for _, rs := range info.Replicas {
			if rs.NodeID == nodeID || !rs.IsISR {
				continue
			}
			if !tm.coordinator.IsNodeAlive(rs.NodeID) {
				continue
			}
			newLeader = rs.NodeID
			break
		}
		if newLeader == "" {
			tm.Logger.Warn("no ISR replica for leadership", zap.String("topic", topicName), zap.String("old_leader_node_id", nodeID))
			continue
		}
		changes = append(changes, leaderChange{topic: topicName, newLeader: newLeader, epoch: info.LeaderEpoch + 1})
	}
	if len(changes) == 0 {
		return
	}
	// Applied asynchronously so the caller (e.g. the reconciliation loop) isn't
	// blocked for the duration of the resulting Raft round-trips.
	go func() {
		for _, ch := range changes {
			if err := tm.coordinator.ApplyLeaderChangeEvent(ch.topic, ch.newLeader, ch.epoch); err != nil {
				tm.Logger.Warn("leader change apply failed", zap.String("topic", ch.topic), zap.Error(err))
			}
		}
	}()
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
			leoBytes, _ := json.Marshal(localLEOs)
			tm.Logger.Info("topic manager local state", zap.String("local_leo", string(leoBytes)))
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
