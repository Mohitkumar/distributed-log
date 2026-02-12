package topic

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/coordinator"
	"github.com/mohitkumar/mlog/log"
	"github.com/mohitkumar/mlog/protocol"
	"go.uber.org/zap"
)

const defaultMetadataLogInterval = 30 * time.Second

type NodeMetadata struct {
	mu                sync.RWMutex         `json:"-"`
	NodeID            string               `json:"node_id"`
	Addr              string               `json:"addr"`
	RpcAddr           string               `json:"rpc_addr"`
	remoteClient      *client.RemoteClient `json:"-"`
	replicationClient *client.RemoteClient `json:"-"`
}

func (n *NodeMetadata) GetRpcClient() (*client.RemoteClient, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.remoteClient == nil {
		remoteClient, err := client.NewRemoteClient(n.RpcAddr)
		if err != nil {
			return nil, err
		}
		n.remoteClient = remoteClient
	}
	return n.remoteClient, nil
}

func (n *NodeMetadata) GetReplicationClient() (*client.RemoteClient, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.replicationClient == nil {
		replClient, err := client.NewRemoteClient(n.RpcAddr)
		if err != nil {
			return nil, err
		}
		n.replicationClient = replClient
	}
	return n.replicationClient, nil
}

type ReplicaState struct {
	ReplicaNodeID string `json:"replica_id"`
	LEO           int64  `json:"leo"`
	IsISR         bool   `json:"is_isr"`
}

// In memory representation of a topic
type Topic struct {
	mu           sync.RWMutex             `json:"-"`
	Name         string                   `json:"name"`
	LeaderNodeID string                   `json:"leader_id"`
	LeaderEpoch  int64                    `json:"leader_epoch"`
	Replicas     map[string]*ReplicaState `json:"replicas"`
	Log          *log.LogManager          `json:"-"`
	Logger       *zap.Logger              `json:"-"`
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
}

// NewTopicManager creates a TopicManager. Coordinator may be nil and set later via SetCoordinator
// (e.g. when TopicManager is used as MetadataStore for the Coordinator).
func NewTopicManager(baseDir string, coord TopicCoordinator, logger *zap.Logger) (*TopicManager, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	tm := &TopicManager{
		Topics:      make(map[string]*Topic),
		BaseDir:     baseDir,
		Logger:      logger,
		Nodes:       make(map[string]*NodeMetadata),
		coordinator: coord,
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
		return false, ErrTopicNotFoundf(topic)
	}
	return topicObj.LeaderNodeID == tm.CurrentNodeID, nil
}

// GetTopicLeaderRPCAddr returns the RPC address of the current leader for the given topic.
func (tm *TopicManager) GetTopicLeaderRPCAddr(topic string) (string, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	topicObj, ok := tm.Topics[topic]
	if !ok {
		return "", ErrTopicNotFoundf(topic)
	}
	node := tm.Nodes[topicObj.LeaderNodeID]
	if node == nil {
		return "", ErrTopicNotFoundf(topic)
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
		return nil, fmt.Errorf("create topic must be sent to Raft leader: %w", ErrCannotReachLeader)
	}
	tm.mu.RLock()
	_, exists := tm.Topics[req.Topic]
	if exists {
		tm.mu.RUnlock()
		return nil, ErrTopicExistsf(req.Topic)
	}
	leaderNodeID, err := tm.GetNodeIDWithLeastTopics()
	if err != nil {
		tm.mu.RUnlock()
		return nil, err
	}
	replicaNodeIds, err := tm.pickReplicaNodeIds(leaderNodeID, int(req.ReplicaCount))
	tm.mu.RUnlock()
	if err != nil {
		return nil, ErrCreateTopic(err)
	}
	tm.Logger.Info("applying create topic via Raft", zap.String("topic", req.Topic), zap.String("leader_node_id", leaderNodeID), zap.Strings("replica_node_ids", replicaNodeIds))
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
		countByNode[node.NodeID] = 0
	}
	for _, t := range tm.Topics {
		if t != nil && t.LeaderNodeID != "" {
			countByNode[t.LeaderNodeID]++
		}
	}
	var bestID string
	minCount := -1
	for id, c := range countByNode {
		if minCount < 0 || c < minCount {
			minCount = c
			bestID = id
		}
	}
	if bestID == "" {
		return "", ErrNoNodesInCluster
	}
	return bestID, nil
}

// pickReplicaNodeIds returns up to replicaCount node IDs from the cluster, excluding leaderNodeID.
// Caller should hold tm.mu at least for the duration of reading Nodes; used when applying create-topic via Raft.
func (tm *TopicManager) pickReplicaNodeIds(leaderNodeID string, replicaCount int) ([]string, error) {
	var otherNodes []*NodeMetadata
	for _, node := range tm.Nodes {
		if node != nil && node.NodeID != leaderNodeID {
			otherNodes = append(otherNodes, node)
		}
	}
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
	tm.Logger.Info("applying delete topic via Raft", zap.String("topic", req.Topic))
	if err := c.ApplyDeleteTopicEventInternal(req.Topic); err != nil {
		return nil, ErrApplyDeleteTopic(err)
	}
	return &protocol.DeleteTopicResponse{Topic: req.Topic}, nil
}

// GetLeader returns the leader log view for a topic (this node must be the leader).
func (tm *TopicManager) GetLeader(topic string) (*log.LogManager, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	topicObj, ok := tm.Topics[topic]
	if !ok {
		return nil, ErrTopicNotFoundf(topic)
	}
	if topicObj.LeaderNodeID != tm.CurrentNodeID {
		return nil, ErrThisNodeNotLeaderf(topic)
	}
	return topicObj.Log, nil
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
	tm.Logger.Info("restoring topic manager from metadata", zap.Int("topic_count", len(topicNames)), zap.Strings("topics", topicNames))
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
		return ErrCreateLog(err)
	}
	t.Log = logManager
	tm.Logger.Info("restored leader topic from metadata", zap.String("topic", topic))
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
		return ErrTopicAlreadyReplicaf(topic)
	}
	logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topic))
	if err != nil {
		return ErrCreateLogReplica(err)
	}
	topicObj.Log = logManager
	tm.Logger.Info("replica created for topic", zap.String("topic", topic), zap.String("leader_id", leaderId))
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
	for _, rec := range records {
		if _, appendErr := log.Append(rec.Value); appendErr != nil {
			return appendErr
		}
	}
	return nil
}

// ReportLEOViaRaft sends an ISR/LEO update to the Raft leader so it can be applied via the metadata log.
// Replication goroutine calls this with replica LEO and leader LEO (from ReplicateResponse.LeaderLEO).
func (tm *TopicManager) ReportLEOViaRaft(ctx context.Context, topicName string, replicaLEO, leaderLEO uint64) error {
	if tm.coordinator == nil {
		return nil
	}
	// Same ISR logic as previously in RecordLEORemote
	var isr bool
	if leaderLEO >= 100 {
		isr = replicaLEO >= leaderLEO-100
	} else {
		isr = replicaLEO >= leaderLEO
	}
	raftLeaderNodeID, err := tm.coordinator.GetRaftLeaderNodeID()
	if err != nil {
		tm.Logger.Debug("report LEO via Raft: get raft leader failed", zap.String("topic", topicName), zap.Error(err))
		return err
	}
	raftClient, err := tm.GetRPCClient(raftLeaderNodeID)
	if err != nil {
		tm.Logger.Debug("report LEO via Raft: dial raft leader failed", zap.String("topic", topicName), zap.Error(err))
		return err
	}
	_, err = raftClient.ApplyIsrUpdateEvent(ctx, &protocol.ApplyIsrUpdateEventRequest{
		Topic:         topicName,
		ReplicaNodeID: tm.CurrentNodeID,
		Isr:           isr,
		Leo:           int64(replicaLEO),
	})
	if err != nil {
		tm.Logger.Debug("apply ISR update on Raft leader failed", zap.String("topic", topicName), zap.Error(err))
		return err
	}
	return nil
}

// GetReplicationClient returns a client to the given node (for replication); part of ReplicationTarget.
func (tm *TopicManager) GetReplicationClient(nodeID string) (*client.RemoteClient, error) {
	tm.mu.RLock()
	node := tm.Nodes[nodeID]
	tm.mu.RUnlock()
	if node == nil {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}
	return node.GetReplicationClient()
}

func (tm *TopicManager) GetRPCClient(nodeID string) (*client.RemoteClient, error) {
	tm.mu.RLock()
	node := tm.Nodes[nodeID]
	tm.mu.RUnlock()
	if node == nil {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}
	return node.GetRpcClient()
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
			return 0, ErrWaitFollowersCatchUp(err)
		}
		return offset, nil
	default:
		return 0, ErrInvalidAckModef(int32(acks))
	}
}

// HandleProduceBatch appends multiple records (leader only).
func (tm *TopicManager) HandleProduceBatch(ctx context.Context, t *Topic, values [][]byte, acks protocol.AckMode) (uint64, uint64, error) {
	if len(values) == 0 {
		return 0, 0, ErrValuesEmpty
	}

	var base, last uint64
	for i, v := range values {
		off, err := t.Log.Append(v)
		if err != nil {
			return 0, 0, err
		}
		if i == 0 {
			base = off
		}
		last = off
	}

	switch acks {
	case protocol.AckLeader:
		return base, last, nil
	case protocol.AckAll:
		if err := tm.waitForAllFollowersToCatchUp(ctx, t, last); err != nil {
			return 0, 0, ErrWaitFollowersCatchUp(err)
		}
		return base, last, nil
	default:
		return 0, 0, ErrInvalidAckModef(int32(acks))
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
				t.Logger.Warn("timeout waiting for followers to catch up", zap.String("topic", t.Name), zap.Uint64("required_offset", offset))
			}
			return ErrTimeoutCatchUp
		}
	}
}

func (tm *TopicManager) maybeAdvanceHW(t *Topic) {
	t.mu.RLock()
	if t.Log == nil {
		t.mu.RUnlock()
		return
	}
	minOffset := t.Log.LEO()
	for _, r := range t.Replicas {
		if r != nil && uint64(r.LEO) < minOffset {
			minOffset = uint64(r.LEO)
		}
	}
	t.mu.RUnlock()
	t.mu.Lock()
	if t.Log != nil {
		t.Log.SetHighWatermark(minOffset)
	}
	t.mu.Unlock()
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
			t.LeaderNodeID = e.LeaderNodeID
			t.LeaderEpoch = e.LeaderEpoch
			delete(t.Replicas, e.LeaderNodeID)
			tm.ensureLocalLogAfterLeaderChange(e.Topic, e.LeaderNodeID)
		}
	case protocol.MetadataEventTypeIsrUpdate:
		e := protocol.IsrUpdateEvent{}
		if err := json.Unmarshal(ev.Data, &e); err != nil {
			return err
		}
		if tm := tm.Topics[e.Topic]; tm != nil {
			if rs := tm.Replicas[e.ReplicaNodeID]; rs != nil {
				rs.IsISR = e.Isr
				rs.LEO = e.Leo
			}
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
	case protocol.MetadataEventTypeRemoveNode:
		e := protocol.RemoveNodeEvent{}
		if err := json.Unmarshal(ev.Data, &e); err != nil {
			return err
		}
		delete(tm.Nodes, e.NodeID)
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
func (tm *TopicManager) createTopicFromEvent(topicName, leaderNodeID string, leaderEpoch int64, replicaNodeIds []string) {
	t := &Topic{
		Name:         topicName,
		LeaderNodeID: leaderNodeID,
		LeaderEpoch:  leaderEpoch,
		Replicas:     make(map[string]*ReplicaState),
		Logger:       tm.Logger,
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
				tm.Logger.Warn("ensureLocalLog: open leader log failed", zap.String("topic", topicName), zap.Error(err))
				return
			}
			t.Log = logManager
			tm.Logger.Info("opened leader log for topic", zap.String("topic", topicName))
		}
		return
	}
	for _, rid := range replicaNodeIds {
		if rid == tm.CurrentNodeID {
			if t.Log == nil {
				logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topicName))
				if err != nil {
					tm.Logger.Warn("ensureLocalLog: open replica log failed", zap.String("topic", topicName), zap.Error(err))
					return
				}
				t.Log = logManager
				tm.Logger.Info("opened replica log for topic", zap.String("topic", topicName), zap.String("leader_id", leaderNodeID))
			}
			return
		}
	}
}

// ensureLocalLogAfterLeaderChange updates local log after leader change (promote or demote). Caller holds tm.mu.
func (tm *TopicManager) ensureLocalLogAfterLeaderChange(topicName, newLeaderID string) {
	t := tm.Topics[topicName]
	if t == nil {
		return
	}
	if tm.CurrentNodeID == newLeaderID {
		if t.Log == nil {
			logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topicName))
			if err != nil {
				tm.Logger.Warn("ensureLocalLogAfterLeaderChange: open leader log failed", zap.String("topic", topicName), zap.Error(err))
				return
			}
			t.Log = logManager
		}
		tm.Logger.Info("promoted to leader for topic", zap.String("topic", topicName))
		return
	}
	if t.LeaderNodeID == tm.CurrentNodeID {
		t.LeaderNodeID = newLeaderID
		return
	}
	if _, isReplica := t.Replicas[tm.CurrentNodeID]; isReplica {
		t.LeaderNodeID = newLeaderID
		return
	}
	if t.LeaderNodeID == tm.CurrentNodeID {
		if t.Log != nil {
			t.Log.Delete()
			t.Log = nil
		}
		_ = os.RemoveAll(filepath.Join(tm.BaseDir, topicName))
		t.LeaderNodeID = newLeaderID
		logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topicName))
		if err != nil {
			tm.Logger.Warn("ensureLocalLogAfterLeaderChange: open replica log failed", zap.String("topic", topicName), zap.Error(err))
			return
		}
		t.Log = logManager
		tm.Logger.Info("demoted to replica for topic", zap.String("topic", topicName), zap.String("leader_id", newLeaderID))
	}
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
	tm.Logger.Info("removed topic locally", zap.String("topic", topicName))
}

func (tm *TopicManager) maybeReassignTopicLeaders(nodeID string) {
	if tm.coordinator == nil || !tm.coordinator.IsLeader() {
		return
	}
	tm.mu.RLock()
	topicsCopy := make(map[string]*Topic, len(tm.Topics))
	for k, v := range tm.Topics {
		topicsCopy[k] = v
	}
	tm.mu.RUnlock()

	for topicName, t := range topicsCopy {
		if t == nil || t.LeaderNodeID != nodeID {
			continue
		}
		var newLeader string
		t.mu.RLock()
		for rid, rs := range t.Replicas {
			if rid == nodeID || rs == nil || !rs.IsISR {
				continue
			}
			tm.mu.RLock()
			n := tm.Nodes[rid]
			tm.mu.RUnlock()
			if n == nil {
				continue
			}
			newLeader = rid
			break
		}
		t.mu.RUnlock()
		if newLeader == "" {
			tm.Logger.Warn("no ISR replica available to take leadership", zap.String("topic", topicName), zap.String("old_leader_node_id", nodeID))
			continue
		}
		nextEpoch := t.LeaderEpoch + 1
		if err := tm.coordinator.ApplyLeaderChangeEvent(topicName, newLeader, nextEpoch); err != nil {
			tm.Logger.Warn("apply leader change failed", zap.String("topic", topicName), zap.Error(err))
		}
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
					tm.Logger.Warn("restore: open leader log failed", zap.String("topic", name), zap.Error(err))
					continue
				}
				t.Log = logManager
			}
		} else if _, isReplica := t.Replicas[tm.CurrentNodeID]; isReplica {
			if t.Log == nil {
				logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, name))
				if err != nil {
					tm.Logger.Warn("restore: open replica log failed", zap.String("topic", name), zap.Error(err))
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
			tm.mu.RUnlock()
			if err != nil {
				fmt.Printf("[coordinator] metadata periodic log marshal error: %v\n", err)
				continue
			}
			fmt.Printf("[coordinator] metadata store (%s):\n%s\n", time.Now().Format(time.RFC3339), string(b))
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
