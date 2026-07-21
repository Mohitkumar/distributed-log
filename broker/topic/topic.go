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
	"github.com/mohitkumar/mlog/broker/cluster"
	raft "github.com/mohitkumar/mlog/broker/cluster/raft"
	"github.com/mohitkumar/mlog/broker/log"
	"go.uber.org/zap"
)

const defaultMetadataLogInterval = 30 * time.Second

// Topic is the per-broker runtime state for a topic: its locally-open log (only
// present on nodes that are currently leader or replica for it) and nothing else.
// Cluster-wide state (leader, epoch, replica set, ISR) lives in
// cluster.ClusterMetadataStore, not here — see TopicManager.metadataStore.
type Topic struct {
	mu     sync.RWMutex
	Name   string          `json:"name"`
	Log    *log.LogManager `json:"-"`
	Logger *zap.Logger     `json:"-"`
}

var _ raft.MetadataStore = (*TopicManager)(nil)

type TopicManager struct {
	mu                   sync.RWMutex
	Topics               map[string]*Topic // local runtime state only: open log handles, keyed by topic name
	BaseDir              string
	Logger               *zap.Logger
	CurrentNodeID        string // Local node ID from config; not persisted.
	metadataStore        *cluster.ClusterMetadataStore
	coordinator          TopicCoordinator
	stopPeriodic         chan struct{}
	stopReplication      chan struct{}
	replicationBatchSize uint32
	ISRLagThreshold      uint64 // max record lag for ISR membership
}

// NewTopicManager creates a TopicManager. metadataStore holds the cluster-wide
// (Raft-replicated) topic metadata; if nil, an empty one is created (mainly for
// convenience in tests that don't care about it). Coordinator may be nil and set
// later via SetCoordinator (e.g. when TopicManager is used as MetadataStore for Cluster).
func NewTopicManager(baseDir string, metadataStore *cluster.ClusterMetadataStore, coord TopicCoordinator, logger *zap.Logger) (*TopicManager, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	if metadataStore == nil {
		metadataStore = cluster.NewClusterMetadataStore()
	}
	tm := &TopicManager{
		Topics:          make(map[string]*Topic),
		BaseDir:         baseDir,
		Logger:          logger,
		metadataStore:   metadataStore,
		coordinator:     coord,
		stopPeriodic:    make(chan struct{}),
		ISRLagThreshold: cluster.DefaultISRLagThreshold,
	}
	go tm.periodicLog(defaultMetadataLogInterval)
	tm.replicationBatchSize = DefaultReplicationBatchSize
	return tm, nil
}

// SetCoordinator sets the coordinator (e.g. after Cluster is created with this TopicManager as MetadataStore).
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

func (tm *TopicManager) currentNodeID() string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.CurrentNodeID
}

func (tm *TopicManager) IsLeader(topic string) (bool, error) {
	t := tm.metadataStore.GetTopic(topic)
	if t == nil {
		return false, ErrTopicNotFoundf(topic)
	}
	return t.LeaderID() == tm.currentNodeID(), nil
}

// GetTopicLeaderRPCAddr returns the RPC address of the current leader for the given topic.
func (tm *TopicManager) GetTopicLeaderRPCAddr(topic string) (string, error) {
	t := tm.metadataStore.GetTopic(topic)
	if t == nil {
		return "", ErrTopicNotFoundf(topic)
	}
	if tm.coordinator == nil {
		return "", fmt.Errorf("topic: no coordinator")
	}
	addr, ok := tm.coordinator.NodeRPCAddr(t.LeaderID())
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
		t = &Topic{Name: name, Logger: tm.Logger}
		tm.Topics[name] = t
	}
	return t
}

// GetRaftLeaderRPCAddr returns the RPC address of the current Raft (metadata) leader.
func (tm *TopicManager) GetRaftLeaderRPCAddr() (string, error) {
	if tm.coordinator == nil {
		return "", fmt.Errorf("topic: no coordinator")
	}
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
// when each node applies the event in Apply() via ensureLocalLogForTopic.
func (tm *TopicManager) CreateTopic(ctx context.Context, req *protocol.CreateTopicRequest) (*protocol.CreateTopicResponse, error) {
	if tm.coordinator == nil {
		return nil, fmt.Errorf("topic: no coordinator")
	}
	c := tm.coordinator
	if !c.IsLeader() {
		return nil, fmt.Errorf("create topic must be sent to Raft leader: %w", ErrCannotReachLeader)
	}
	if tm.metadataStore.TopicExists(req.Topic) {
		return nil, ErrTopicExistsf(req.Topic)
	}
	candidates := c.AliveNodeIDs()
	leaderNodeID, err := tm.metadataStore.NodeIDWithLeastTopics(candidates)
	if err != nil {
		return nil, err
	}
	replicaNodeIds, err := cluster.PickReplicaNodeIds(leaderNodeID, int(req.ReplicaCount), candidates)
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
	if tm.coordinator == nil {
		return nil, fmt.Errorf("topic: no coordinator")
	}
	c := tm.coordinator
	if !c.IsLeader() {
		return nil, fmt.Errorf("delete topic must be sent to Raft leader: %w", ErrCannotReachLeader)
	}
	if !tm.metadataStore.TopicExists(req.Topic) {
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
	tmeta := tm.metadataStore.GetTopic(topic)
	if tmeta == nil {
		return nil, ErrTopicNotFoundf(topic)
	}
	if tmeta.LeaderID() != tm.currentNodeID() {
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
	names := tm.metadataStore.TopicNames()
	out := make([]protocol.TopicInfo, 0, len(names))
	for _, name := range names {
		t := tm.metadataStore.GetTopic(name)
		if t == nil {
			continue
		}
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
			Name:         name,
			LeaderNodeID: leaderID,
			LeaderEpoch:  epoch,
			Replicas:     replicas,
		})
	}
	return &protocol.ListTopicsResponse{Topics: out}
}

// RestoreFromMetadata rebuilds local logs from cluster metadata. Call after Restore()
// has populated the metadata store from snapshot, so local logs are opened where this
// node is leader or replica.
func (tm *TopicManager) RestoreFromMetadata() error {
	currentNodeID := tm.currentNodeID()
	topicNames := tm.metadataStore.TopicNames()
	if len(topicNames) == 0 {
		return nil
	}
	tm.Logger.Info("restore from metadata", zap.Int("topic_count", len(topicNames)), zap.Strings("topics", topicNames))
	for _, topicName := range topicNames {
		tmeta := tm.metadataStore.GetTopic(topicName)
		if tmeta == nil {
			continue
		}
		leaderID := tmeta.LeaderID()
		if leaderID == "" {
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
					l.SetHighWatermark(tmeta.MinISRLeo(l.LEO()))
				}
			}
		} else if tmeta.HasReplica(currentNodeID) {
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
		tmeta := tm.metadataStore.GetTopic(names[i])
		if tmeta == nil {
			continue
		}
		leaderID := tmeta.LeaderID()
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
	tmeta := tm.metadataStore.GetTopic(topicName)
	if tmeta == nil {
		return nil
	}
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
		lagThreshold = cluster.DefaultISRLagThreshold
	}
	isr := tmeta.RecordReplicaFetch(replicaNodeID, leo, lagThreshold, localLEO)
	if l != nil {
		l.SetHighWatermark(tmeta.MinISRLeo(localLEO))
	}
	if tm.coordinator == nil {
		return nil
	}
	return tm.coordinator.ApplyIsrUpdateEventInternal(topicName, replicaNodeID, isr)
}

// Apply applies a Raft-committed metadata event: first to the cluster metadata store
// (canonical, Raft-replicated topic state), then any local side effects (opening or
// closing this node's on-disk log) that follow from it.
func (tm *TopicManager) Apply(ev *raft.MetadataEvent) error {
	if err := tm.metadataStore.Apply(ev); err != nil {
		return err
	}
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
	if tm.coordinator == nil || !tm.coordinator.IsLeader() {
		return
	}
	type leaderChange struct {
		topic     string
		newLeader string
		epoch     int64
	}
	var changes []leaderChange
	for _, topicName := range tm.metadataStore.TopicNames() {
		t := tm.metadataStore.GetTopic(topicName)
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
			if !tm.coordinator.IsNodeAlive(rs.ReplicaNodeID) {
				continue
			}
			newLeader = rs.ReplicaNodeID
			break
		}
		if newLeader == "" {
			tm.Logger.Warn("no ISR replica for leadership", zap.String("topic", topicName), zap.String("old_leader_node_id", nodeID))
			continue
		}
		changes = append(changes, leaderChange{topic: topicName, newLeader: newLeader, epoch: epoch + 1})
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

// Snapshot serializes the cluster metadata store, for Raft snapshot persistence.
func (tm *TopicManager) Snapshot() ([]byte, error) {
	return tm.metadataStore.Snapshot()
}

// Restore replaces the cluster metadata store's contents from a previously-taken Snapshot.
// Call RestoreFromMetadata() afterward to open local logs based on the restored state.
func (tm *TopicManager) Restore(data []byte) error {
	return tm.metadataStore.Restore(data)
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
