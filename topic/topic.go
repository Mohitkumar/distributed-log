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

	"github.com/mohitkumar/mlog/coordinator"
	"github.com/mohitkumar/mlog/errs"
	"github.com/mohitkumar/mlog/log"
	"github.com/mohitkumar/mlog/protocol"
	"go.uber.org/zap"
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
	defer tm.mu.RUnlock()
	topicObj, ok := tm.Topics[topic]
	if !ok {
		return false, errs.ErrTopicNotFoundf(topic)
	}
	return topicObj.LeaderNodeID == tm.CurrentNodeID, nil
}

// GetTopicLeaderRPCAddr returns the RPC address of the current leader for the given topic.
func (tm *TopicManager) GetTopicLeaderRPCAddr(topic string) (string, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	topicObj, ok := tm.Topics[topic]
	if !ok {
		return "", errs.ErrTopicNotFoundf(topic)
	}
	node := tm.Nodes[topicObj.LeaderNodeID]
	if node == nil {
		return "", errs.ErrTopicNotFoundf(topic)
	}
	return node.RpcAddr, nil
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
		return nil, fmt.Errorf("create topic must be sent to Raft leader: %w", errs.ErrCannotReachLeader)
	}
	tm.mu.RLock()
	_, exists := tm.Topics[req.Topic]
	tm.mu.RUnlock()
	if exists {
		return nil, errs.ErrTopicExistsf(req.Topic)
	}
	leaderNodeID, err := tm.GetNodeIDWithLeastTopics()
	if err != nil {
		return nil, err
	}
	replicaNodeIds, err := tm.pickReplicaNodeIds(leaderNodeID, int(req.ReplicaCount))
	if err != nil {
		return nil, errs.ErrCreateTopic(err)
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
		if t != nil && t.LeaderNodeID != "" {
			countByNode[t.LeaderNodeID]++
		}
	}
	if len(countByNode) == 0 {
		return "", errs.ErrNoNodesInCluster
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
		return nil, errs.ErrNotEnoughNodesf(replicaCount, len(otherNodes))
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
		return nil, fmt.Errorf("delete topic must be sent to Raft leader: %w", errs.ErrCannotReachLeader)
	}
	tm.mu.RLock()
	_, exists := tm.Topics[req.Topic]
	tm.mu.RUnlock()
	if !exists {
		return nil, errs.ErrTopicNotFoundf(req.Topic)
	}
	tm.Logger.Info("delete topic via Raft", zap.String("topic", req.Topic))
	if err := c.ApplyDeleteTopicEventInternal(req.Topic); err != nil {
		return nil, errs.ErrApplyDeleteTopic(err)
	}
	return &protocol.DeleteTopicResponse{Topic: req.Topic}, nil
}

// GetLeader returns the leader log view for a topic (this node must be the leader).
func (tm *TopicManager) GetLeader(topic string) (*log.LogManager, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	topicObj, ok := tm.Topics[topic]
	if !ok {
		return nil, errs.ErrTopicNotFoundf(topic)
	}
	if topicObj.LeaderNodeID != tm.CurrentNodeID {
		return nil, errs.ErrThisNodeNotLeaderf(topic)
	}
	return topicObj.Log, nil
}

// GetTopic returns the topic object.
func (tm *TopicManager) GetTopic(topic string) (*Topic, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	topicObj, ok := tm.Topics[topic]
	if !ok {
		return nil, errs.ErrTopicNotFoundf(topic)
	}
	return topicObj, nil
}

// ListTopics returns topic names with leader and replica info. Any node can serve this (metadata is replicated).
func (tm *TopicManager) ListTopics() *protocol.ListTopicsResponse {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	out := make([]protocol.TopicInfo, 0, len(tm.Topics))
	for name, t := range tm.Topics {
		if t == nil {
			continue
		}
		t.mu.RLock()
		leaderID := t.LeaderNodeID
		epoch := t.LeaderEpoch
		replicas := make([]protocol.ReplicaInfo, 0, len(t.Replicas))
		for _, rs := range t.Replicas {
			if rs != nil {
				replicas = append(replicas, protocol.ReplicaInfo{
					NodeID: rs.ReplicaNodeID,
					IsISR:  rs.IsISR,
					LEO:    rs.LEO,
				})
			}
		}
		t.mu.RUnlock()
		out = append(out, protocol.TopicInfo{
			Name:         name,
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
		tm.mu.RLock()
		t := tm.Topics[topicName]
		leaderID := ""
		if t != nil {
			leaderID = t.LeaderNodeID
		}
		tm.mu.RUnlock()
		if leaderID == "" {
			continue
		}
		if leaderID == tm.CurrentNodeID {
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
	tm.mu.Lock()
	defer tm.mu.Unlock()
	t := tm.Topics[topic]
	if t == nil {
		return nil
	}
	if t.Log != nil {
		return nil
	}
	logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topic))
	if err != nil {
		return errs.ErrCreateLog(err)
	}
	t.Log = logManager
	tm.Logger.Info("leader topic restored", zap.String("topic", topic))
	return nil
}

// restoreReplicaTopic creates a replica for the topic on this node (called by leader via RPC).
func (tm *TopicManager) restoreReplicaTopic(topic string, leaderId string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
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
	if topicObj.Log != nil {
		return errs.ErrTopicAlreadyReplicaf(topic)
	}
	logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topic))
	if err != nil {
		return errs.ErrCreateLogReplica(err)
	}
	topicObj.Log = logManager
	tm.Logger.Info("replica topic restored", zap.String("topic", topic), zap.String("leader_id", leaderId))
	return nil
}

// ReplicationTarget implementation (TopicManager runs the replication thread and uses these).

func (tm *TopicManager) ListReplicaTopics() []ReplicaTopicInfo {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	var out []ReplicaTopicInfo
	for name, t := range tm.Topics {
		if t == nil || t.Log == nil || t.LeaderNodeID == tm.CurrentNodeID {
			continue
		}
		out = append(out, ReplicaTopicInfo{TopicName: name, LeaderNodeID: t.LeaderNodeID})
	}
	return out
}

func (tm *TopicManager) GetLEO(topicName string) (uint64, bool) {
	tm.mu.RLock()
	t := tm.Topics[topicName]
	tm.mu.RUnlock()
	if t == nil {
		return 0, false
	}
	t.mu.RLock()
	log := t.Log
	t.mu.RUnlock()
	if log == nil {
		return 0, false
	}
	return log.LEO(), true
}

func (tm *TopicManager) ApplyChunk(topicName string, rawChunk []byte) error {
	if len(rawChunk) == 0 {
		return nil
	}
	records, err := protocol.DecodeReplicationBatch(rawChunk)
	if err != nil {
		return err
	}
	tm.mu.RLock()
	t := tm.Topics[topicName]
	tm.mu.RUnlock()
	if t == nil {
		return nil
	}
	t.mu.RLock()
	log := t.Log
	t.mu.RUnlock()
	if log == nil {
		return nil
	}
	values := make([][]byte, len(records))
	for i, rec := range records {
		values[i] = rec.Value
	}
	_, err = log.AppendBatch(values)
	return err
}

// ApplyRecord appends a single record to the topic log (used by replica when replicating via Fetch).
func (tm *TopicManager) ApplyRecord(topicName string, value []byte) error {
	if len(value) == 0 {
		return nil
	}
	tm.mu.RLock()
	t := tm.Topics[topicName]
	tm.mu.RUnlock()
	if t == nil {
		return nil
	}
	t.mu.RLock()
	log := t.Log
	t.mu.RUnlock()
	if log == nil {
		return nil
	}
	_, err := log.Append(value)
	return err
}

// ApplyRecordBatch appends multiple records to the topic log in one batch (used by replication).
func (tm *TopicManager) ApplyRecordBatch(topicName string, values [][]byte) error {
	if len(values) == 0 {
		return nil
	}
	tm.mu.RLock()
	t := tm.Topics[topicName]
	tm.mu.RUnlock()
	if t == nil {
		return nil
	}
	t.mu.RLock()
	l := t.Log
	t.mu.RUnlock()
	if l == nil {
		return nil
	}
	_, err := l.AppendBatch(values)
	return err
}

// RecordReplicaLEOFromFetch is called by the leader when it serves a Fetch from a replica (ReplicaNodeID set).
// It updates the replica's LEO and applies an ISR update via Raft.
func (tm *TopicManager) RecordReplicaLEOFromFetch(ctx context.Context, topicName, replicaNodeID string, leo int64) error {
	tm.mu.Lock()
	t := tm.Topics[topicName]
	if t == nil {
		tm.mu.Unlock()
		return nil
	}
	leaderLEO := uint64(0)
	if t.Log != nil {
		leaderLEO = t.Log.LEO()
	}
	if t.Replicas == nil {
		t.Replicas = make(map[string]*ReplicaState)
	}
	rs := t.Replicas[replicaNodeID]
	if rs == nil {
		t.Replicas[replicaNodeID] = &ReplicaState{
			ReplicaNodeID: replicaNodeID,
			LEO:           leo,
			IsISR:         true,
		}
		rs = t.Replicas[replicaNodeID]
	} else {
		rs.LEO = leo
	}
	lagThreshold := tm.ISRLagThreshold
	if lagThreshold == 0 {
		lagThreshold = DefaultISRLagThreshold
	}
	var isr bool
	if leaderLEO > lagThreshold {
		isr = uint64(leo) >= leaderLEO-lagThreshold
	} else {
		isr = leo >= 0 // all replicas are in-sync for small topics
	}
	rs.IsISR = isr
	tm.mu.Unlock()
	tm.maybeAdvanceHW(t)
	if tm.coordinator == nil {
		return nil
	}
	return tm.coordinator.ApplyIsrUpdateEventInternal(topicName, replicaNodeID, isr)
}

// HandleProduce appends to the topic log (leader only). For ACK_ALL, waits for replicas to catch up.
func (tm *TopicManager) HandleProduce(ctx context.Context, t *Topic, logEntry *protocol.LogEntry, acks protocol.AckMode) (uint64, error) {
	offset, err := t.Log.Append(logEntry.Value)
	if err != nil {
		return 0, err
	}
	switch acks {
	case protocol.AckLeader:
		return offset, nil
	case protocol.AckAll:
		if err := tm.waitForAllFollowersToCatchUp(ctx, t, offset); err != nil {
			return 0, errs.ErrWaitFollowersCatchUp(err)
		}
		return offset, nil
	default:
		return 0, errs.ErrInvalidAckModef(int32(acks))
	}
}

// HandleProduceBatch appends multiple records (leader only).
func (tm *TopicManager) HandleProduceBatch(ctx context.Context, t *Topic, values [][]byte, acks protocol.AckMode) (uint64, uint64, error) {
	if len(values) == 0 {
		return 0, 0, errs.ErrValuesEmpty
	}

	base, err := t.Log.AppendBatch(values)
	if err != nil {
		return 0, 0, err
	}
	last := base + uint64(len(values)) - 1

	switch acks {
	case protocol.AckLeader:
		return base, last, nil
	case protocol.AckAll:
		if err := tm.waitForAllFollowersToCatchUp(ctx, t, last); err != nil {
			return 0, 0, errs.ErrWaitFollowersCatchUp(err)
		}
		return base, last, nil
	default:
		return 0, 0, errs.ErrInvalidAckModef(int32(acks))
	}
}

func (tm *TopicManager) waitForAllFollowersToCatchUp(ctx context.Context, t *Topic, offset uint64) error {
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	requiredLEO := offset + 1

	for {
		allCaughtUp := true
		candidates := 0

		t.mu.RLock()
		useISR := false
		for _, r := range t.Replicas {
			if r != nil && r.IsISR {
				useISR = true
				break
			}
		}
		for _, replica := range t.Replicas {
			if replica == nil {
				continue
			}
			if useISR && !replica.IsISR {
				continue
			}
			candidates++
			if uint64(replica.LEO) < requiredLEO {
				allCaughtUp = false
				break
			}
		}
		t.mu.RUnlock()

		if candidates == 0 {
			return nil
		}
		if allCaughtUp {
			return nil
		}

		select {
		case <-ticker.C:
			continue
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			if t.Logger != nil {
				t.Logger.Warn("followers catch-up timeout", zap.String("topic", t.Name), zap.Uint64("required_offset", offset))
			}
			return errs.ErrTimeoutCatchUp
		}
	}
}

// maybeAdvanceHW sets HW = min(leader LEO, all ISR replicas' LEO).
// Non-ISR replicas are excluded — a dead/lagging replica must not hold back consumer visibility.
func (tm *TopicManager) maybeAdvanceHW(t *Topic) {
	if t.Log == nil {
		return
	}
	minOffset := t.Log.LEO()
	for _, r := range t.Replicas {
		if r != nil && r.IsISR && uint64(r.LEO) < minOffset {
			minOffset = uint64(r.LEO)
		}
	}
	t.Log.SetHighWatermark(minOffset)
}

func (tm *TopicManager) Apply(ev *protocol.MetadataEvent) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	switch ev.EventType {
	case protocol.MetadataEventTypeCreateTopic:
		e := protocol.CreateTopicEvent{}
		if err := json.Unmarshal(ev.Data, &e); err != nil {
			return err
		}
		tm.createTopicFromEvent(e.Topic, e.LeaderNodeID, e.LeaderEpoch, e.ReplicaNodeIds)
	case protocol.MetadataEventTypeLeaderChange:
		e := protocol.LeaderChangeEvent{}
		if err := json.Unmarshal(ev.Data, &e); err != nil {
			return err
		}
		t := tm.Topics[e.Topic]
		if t != nil {
			oldLeaderID := t.LeaderNodeID
			t.LeaderNodeID = e.LeaderNodeID
			t.LeaderEpoch = e.LeaderEpoch
			// New leader is no longer a replica; old leader becomes a replica.
			delete(t.Replicas, e.LeaderNodeID)
			// Only add the old leader as a replica if it's still a live node in the cluster.
			// If the old leader was removed (node killed), it was already cleaned up from Replicas
			// by the RemoveNode handler. Adding a dead node would pin HW at 0.
			if oldLeaderID != e.LeaderNodeID && oldLeaderID != "" && tm.Nodes[oldLeaderID] != nil {
				if t.Replicas == nil {
					t.Replicas = make(map[string]*ReplicaState)
				}
				t.Replicas[oldLeaderID] = &ReplicaState{
					ReplicaNodeID: oldLeaderID,
					LEO:           0,
					IsISR:         false,
				}
			}
			tm.ensureLocalLogAfterLeaderChange(e.Topic, oldLeaderID, e.LeaderNodeID)
		}
	case protocol.MetadataEventTypeIsrUpdate:
		e := protocol.IsrUpdateEvent{}
		if err := json.Unmarshal(ev.Data, &e); err != nil {
			return err
		}
		t := tm.Topics[e.Topic]
		if t != nil {
			rs := t.Replicas[e.ReplicaNodeID]
			if rs != nil {
				rs.IsISR = e.Isr
			} else {
				// Replica not in map (e.g. node restarted, or event order); add with LEO 0 (updated on leader via Fetch)
				if t.Replicas == nil {
					t.Replicas = make(map[string]*ReplicaState)
				}
				t.Replicas[e.ReplicaNodeID] = &ReplicaState{
					ReplicaNodeID: e.ReplicaNodeID,
					LEO:           0,
					IsISR:         e.Isr,
				}
			}
			tm.maybeAdvanceHW(t)
		}
	case protocol.MetadataEventTypeDeleteTopic:
		e := protocol.DeleteTopicEvent{}
		if err := json.Unmarshal(ev.Data, &e); err != nil {
			return err
		}
		tm.deleteTopicFromEvent(e.Topic)
	case protocol.MetadataEventTypeAddNode:
		e := protocol.AddNodeEvent{}
		if err := json.Unmarshal(ev.Data, &e); err != nil {
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
		e := protocol.RemoveNodeEvent{}
		if err := json.Unmarshal(ev.Data, &e); err != nil {
			return err
		}
		delete(tm.Nodes, e.NodeID)
		// Dead node stays in Replicas so it can resume replication when it comes back.
		// HW is not affected because maybeAdvanceHW only considers ISR replicas.
		tm.maybeReassignTopicLeaders(e.NodeID)
	case protocol.MetadataEventTypeUpdateNode:
		e := protocol.UpdateNodeEvent{}
		if err := json.Unmarshal(ev.Data, &e); err != nil {
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
		if t.Log == nil {
			logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topicName))
			if err != nil {
				tm.Logger.Warn("open leader log failed", zap.String("topic", topicName), zap.Error(err))
				return
			}
			t.Log = logManager
			tm.Logger.Debug("leader log opened", zap.String("topic", topicName))
		}
		return
	}
	for _, rid := range replicaNodeIds {
		if rid == tm.CurrentNodeID {
			if t.Log == nil {
				logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topicName))
				if err != nil {
					tm.Logger.Warn("open replica log failed", zap.String("topic", topicName), zap.Error(err))
					return
				}
				t.Log = logManager
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
		if t.Log == nil {
			logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topicName))
			if err != nil {
				tm.Logger.Warn("open leader log failed", zap.String("topic", topicName), zap.Error(err))
				return
			}
			t.Log = logManager
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
	if t != nil && t.Log != nil {
		t.Log.Close()
		t.Log.Delete()
		t.Log = nil
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
		if t == nil || t.LeaderNodeID != nodeID {
			continue
		}
		var newLeader string
		for rid, rs := range t.Replicas {
			if rid == nodeID || rs == nil || !rs.IsISR {
				continue
			}
			n := tm.Nodes[rid]
			if n == nil {
				continue
			}
			newLeader = rid
			break
		}
		if newLeader == "" {
			tm.Logger.Warn("no ISR replica for leadership", zap.String("topic", topicName), zap.String("old_leader_node_id", nodeID))
			continue
		}
		changes = append(changes, leaderChange{
			topic:     topicName,
			newLeader: newLeader,
			epoch:     t.LeaderEpoch + 1,
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
		if t == nil || t.LeaderNodeID == nodeID {
			continue
		}
		if _, already := t.Replicas[nodeID]; already {
			continue
		}
		if len(t.Replicas) >= t.DesiredReplicaCount {
			continue
		}
		if t.Replicas == nil {
			t.Replicas = make(map[string]*ReplicaState)
		}
		t.Replicas[nodeID] = &ReplicaState{
			ReplicaNodeID: nodeID,
			LEO:           0,
			IsISR:         false,
		}
		tm.Logger.Info("added rejoined node as replica",
			zap.String("topic", t.Name),
			zap.String("node_id", nodeID),
		)
	}
}

func (tm *TopicManager) Restore(data []byte) error {
	var decoded struct {
		Topics map[string]*Topic        `json:"topics"`
		Nodes  map[string]*NodeMetadata `json:"nodes"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.Topics = decoded.Topics
	tm.Nodes = decoded.Nodes
	// CurrentNodeID is not restored; it is set from config via SetCurrentNodeID.
	for name, t := range tm.Topics {
		if t == nil {
			continue
		}
		t.Logger = tm.Logger
		if tm.CurrentNodeID == t.LeaderNodeID {
			if t.Log == nil {
				logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, name))
				if err != nil {
					tm.Logger.Warn("open leader log failed", zap.String("topic", name), zap.Error(err))
					continue
				}
				t.Log = logManager
			}
			// Initialize HW from local state so consumers can read immediately
			// if no replicas exist (otherwise HW stays 0 until replicas report in).
			tm.maybeAdvanceHW(t)
		} else if _, isReplica := t.Replicas[tm.CurrentNodeID]; isReplica {
			if t.Log == nil {
				logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, name))
				if err != nil {
					tm.Logger.Warn("open replica log failed", zap.String("topic", name), zap.Error(err))
					continue
				}
				t.Log = logManager
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
				if t != nil && t.Log != nil {
					localLEOs[name] = t.Log.LEO()
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
