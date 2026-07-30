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
	"github.com/mohitkumar/mlog/broker/log"
	"go.uber.org/zap"
)

const defaultMetadataLogInterval = 30 * time.Second

type TopicManager struct {
	mu sync.RWMutex
	// Topics is local runtime state only: this node's open log handles, keyed by
	// topic name. Cluster-wide state (leader, epoch, replica set, ISR) lives behind
	// TopicCoordinator, not here. A name only ever appears here once its log.LogManager
	// is fully open — publishing happens in one step (see publishLocalLog), so tm.mu
	// alone is enough to guard it; there's no separate per-entry lock or "reserved but
	// not yet ready" state to worry about.
	Topics               map[string]*log.LogManager
	BaseDir              string
	Logger               *zap.Logger
	CurrentNodeID        string // Local node ID from config; not persisted.
	coordinator          TopicCoordinator
	stopPeriodic         chan struct{}
	replicationCancel    context.CancelFunc // non-nil while the replication/reconcile loops are running
	replConns            *replicationConnCache
	replicationBatchSize uint32
	ISRLagThreshold      uint64        // max record lag for ISR membership
	ISRLagTime           time.Duration // max time since last fetch for ISR membership (Kafka's replica.lag.time.max.ms)
}

// NewTopicManager creates a TopicManager. coord is the cluster coordinator (real
// *cluster.Cluster or a test fake) — the sole source of cluster-wide (Raft-replicated)
// topic metadata; TopicManager never stores that metadata itself, only queries it.
func NewTopicManager(baseDir string, coord TopicCoordinator, logger *zap.Logger) (*TopicManager, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	tm := &TopicManager{
		Topics:          make(map[string]*log.LogManager),
		BaseDir:         baseDir,
		Logger:          logger,
		coordinator:     coord,
		stopPeriodic:    make(chan struct{}),
		replConns:       newReplicationConnCache(),
		ISRLagThreshold: DefaultISRLagThreshold,
		ISRLagTime:      DefaultISRLagTime,
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

// lookupTopic returns the locally-open log for name, or nil if not open here. Safe for concurrent use.
func (tm *TopicManager) lookupTopic(name string) *log.LogManager {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.Topics[name]
}

// publishLocalLog makes l the locally-open log for name, visible to any goroutine via
// lookupTopic/GetLog from this point on. Call only once l is fully constructed —
// there's no "reserve then fill in" step here, so a single tm.mu-guarded map write
// is all the synchronization this needs.
func (tm *TopicManager) publishLocalLog(name string, l *log.LogManager) {
	tm.mu.Lock()
	tm.Topics[name] = l
	tm.mu.Unlock()
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
// Must be called on the Raft leader (client should use GetRaftLeader first). Leader/replica logs
// are opened locally by each node's periodic reconcileLocalTopics (see replication.go), not
// synchronously here — callers producing/consuming right after this returns should expect a brief
// window before the topic is locally ready (see client.RetryTopicNotReady).
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
	l := tm.lookupTopic(topic)
	if l == nil {
		return nil, ErrTopicNotFoundf(topic)
	}
	return l, nil
}

// GetLog returns the locally-open log for topic. Returns ErrTopicNotFound both when
// topic isn't known locally at all and when its log hasn't finished opening yet
// (reconcileLocalTopics is mid-flight) — either way there's nothing usable to hand
// back yet, and callers (e.g. Produce/Fetch) already treat ErrTopicNotFound as
// retriable (see client.RetryTopicNotReady).
func (tm *TopicManager) GetLog(topic string) (*log.LogManager, error) {
	l := tm.lookupTopic(topic)
	if l == nil {
		return nil, ErrTopicNotFoundf(topic)
	}
	return l, nil
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
			if l := tm.lookupTopic(topicName); l != nil {
				l.SetHighWatermark(tm.coordinator.TopicMinISRLeo(topicName, l.LEO()))
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
	if tm.lookupTopic(topic) != nil {
		return nil
	}
	logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topic))
	if err != nil {
		return ErrCreateLog(err)
	}
	tm.publishLocalLog(topic, logManager)
	tm.Logger.Info("leader topic restored", zap.String("topic", topic))
	return nil
}

// restoreReplicaTopic creates a replica for the topic on this node (called by leader via RPC).
func (tm *TopicManager) restoreReplicaTopic(topic string, leaderId string) error {
	if tm.lookupTopic(topic) != nil {
		return ErrTopicAlreadyReplicaf(topic)
	}
	logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topic))
	if err != nil {
		return ErrCreateLogReplica(err)
	}
	tm.publishLocalLog(topic, logManager)
	tm.Logger.Info("replica topic restored", zap.String("topic", topic), zap.String("leader_id", leaderId))
	return nil
}

// ReplicationTarget implementation (TopicManager runs the replication thread and uses these).

func (tm *TopicManager) ListReplicaTopics() []ReplicaTopicInfo {
	tm.mu.RLock()
	names := make([]string, 0, len(tm.Topics))
	currentNodeID := tm.CurrentNodeID
	for name, l := range tm.Topics {
		if l != nil {
			names = append(names, name)
		}
	}
	tm.mu.RUnlock()

	var out []ReplicaTopicInfo
	for _, name := range names {
		leaderID, ok := tm.coordinator.TopicLeaderNodeID(name)
		if !ok || leaderID == currentNodeID {
			continue
		}
		out = append(out, ReplicaTopicInfo{TopicName: name, LeaderNodeID: leaderID})
	}
	return out
}

func (tm *TopicManager) GetLEO(topicName string) (uint64, bool) {
	l := tm.lookupTopic(topicName)
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
	l := tm.lookupTopic(topicName)
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
	l := tm.lookupTopic(topicName)
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
	l := tm.lookupTopic(topicName)
	if l == nil {
		return nil
	}
	_, err := l.AppendBatch(values)
	return err
}

// RecordReplicaLEOFromFetch is called by the leader when it serves a Fetch from a replica (ReplicaNodeID set).
// It updates the replica's LEO and applies an ISR update via Raft.
func (tm *TopicManager) RecordReplicaLEOFromFetch(ctx context.Context, topicName, replicaNodeID string, leo int64) error {
	l := tm.lookupTopic(topicName)
	var localLEO uint64
	if l != nil {
		localLEO = l.LEO()
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

// expireStaleISR demotes ISR replicas that have gone silent — not just lagging on
// offset, which RecordReplicaLEOFromFetch already handles reactively, but stopped
// fetching entirely (dead, partitioned, wedged) — for every topic this node currently
// leads. Without this, a replica's last-known IsISR=true/LEO never gets re-evaluated
// once it stops calling Fetch, permanently pinning the high watermark at its stale LEO
// (see cluster.TopicMetadata.ExpireStaleISR). Mirrors Kafka's periodic
// isr-expiration task (replica.lag.time.max.ms) — called each replication tick, see
// runReplicateLoop.
func (tm *TopicManager) expireStaleISR() {
	lagTime := tm.ISRLagTime
	if lagTime <= 0 {
		lagTime = DefaultISRLagTime
	}
	for _, topicName := range tm.coordinator.TopicNames() {
		isLeader, err := tm.IsLeader(topicName)
		if err != nil || !isLeader {
			continue
		}
		expired := tm.coordinator.ExpireStaleISR(topicName, lagTime)
		if len(expired) == 0 {
			continue
		}
		if l := tm.lookupTopic(topicName); l != nil {
			l.SetHighWatermark(tm.coordinator.TopicMinISRLeo(topicName, l.LEO()))
		}
		for _, nodeID := range expired {
			tm.Logger.Warn("ISR expired: replica stopped fetching",
				zap.String("topic", topicName), zap.String("node_id", nodeID), zap.Duration("max_lag", lagTime))
			// Best-effort, like RecordReplicaLEOFromFetch's own Apply call: this
			// node's local view (what HandleProduce/HW computation actually reads) is
			// already correct regardless of whether Raft-propagating it to other
			// nodes succeeds here.
			_ = tm.coordinator.ApplyIsrUpdateEventInternal(topicName, nodeID, false)
		}
	}
}

// reconcileLocalTopics is TopicManager's periodic reaction to cluster metadata changes
// (called from the reconcile tick in runReplicationThread, see replication.go): it
// opens local logs for topics this node now leads or replicates, and closes/removes
// ones no longer present in cluster metadata. Runs on a fast poll rather than being
// pushed synchronously off Raft's apply path — see replication.go's package doc note.
func (tm *TopicManager) reconcileLocalTopics() {
	names := tm.coordinator.TopicNames()
	present := make(map[string]struct{}, len(names))
	for _, name := range names {
		present[name] = struct{}{}
		tm.reconcileLocalTopic(name)
	}

	tm.mu.RLock()
	localNames := make([]string, 0, len(tm.Topics))
	for name := range tm.Topics {
		localNames = append(localNames, name)
	}
	tm.mu.RUnlock()

	for _, name := range localNames {
		if _, ok := present[name]; !ok {
			tm.removeLocalTopic(name)
		}
	}
}

// reconcileLocalTopic opens the local log for topicName if this node is its leader or a
// replica and no log is open yet. No-op if topicName isn't leader/replica-local here.
func (tm *TopicManager) reconcileLocalTopic(topicName string) {
	// Cheap check first: once a topic's log is open, steady state (the overwhelming
	// majority of calls — this runs 20x/second per topic) never needs to touch cluster
	// metadata at all. Only pay for TopicInfo's snapshot+replica-list copy when there's
	// actually a chance of work to do.
	if tm.lookupTopic(topicName) != nil {
		return
	}
	info, ok := tm.coordinator.TopicInfo(topicName)
	if !ok {
		return
	}
	currentNodeID := tm.currentNodeID()
	isLocal := info.LeaderNodeID == currentNodeID
	if !isLocal {
		for _, r := range info.Replicas {
			if r.NodeID == currentNodeID {
				isLocal = true
				break
			}
		}
	}
	if !isLocal {
		return
	}
	logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topicName))
	if err != nil {
		tm.Logger.Warn("open log failed", zap.String("topic", topicName), zap.Error(err))
		return
	}
	tm.publishLocalLog(topicName, logManager)
	tm.Logger.Debug("local log opened", zap.String("topic", topicName), zap.String("leader_id", info.LeaderNodeID))
}

// removeLocalTopic closes the topic log, removes it from local bookkeeping, and deletes the topic dir.
func (tm *TopicManager) removeLocalTopic(topicName string) {
	tm.mu.Lock()
	l, ok := tm.Topics[topicName]
	if ok {
		delete(tm.Topics, topicName)
	}
	tm.mu.Unlock()
	if l != nil {
		l.Close()
		l.Delete()
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
			for name, l := range tm.Topics {
				if l != nil {
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
