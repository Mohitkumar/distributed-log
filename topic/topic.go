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

// applyDeleteTopicEventOnRaftLeader applies the delete topic event via Raft (this node must be Raft leader).
func (tm *TopicManager) applyDeleteTopicEventOnRaftLeader(ctx context.Context, topicName string) error {
	if tm.coordinator == nil {
		return fmt.Errorf("topic: no coordinator")
	}
	return tm.coordinator.ApplyDeleteTopicEventInternal(topicName)
}

// applyIsrUpdateEventOnRaftLeader applies the ISR update event via Raft (this node must be Raft leader).
func (tm *TopicManager) applyIsrUpdateEventOnRaftLeader(ctx context.Context, topic, replicaNodeID string, isr bool, leo int64) error {
	if tm.coordinator == nil {
		return fmt.Errorf("topic: no coordinator")
	}
	return tm.coordinator.ApplyIsrUpdateEventInternal(topic, replicaNodeID, isr, leo)
}

// CreateTopic creates a new topic with this node as leader and optional replicas on other nodes.
// Returns the actual replica set (this node as leader + replica node IDs in order) for metadata.
func (tm *TopicManager) CreateTopic(topic string, replicaCount int) (replicaNodeIds []string, err error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.Topics[topic]; exists {
		return nil, ErrTopicExistsf(topic)
	}

	logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topic))
	if err != nil {
		return nil, ErrCreateLog(err)
	}

	topicObj := &Topic{
		Name:         topic,
		LeaderNodeID: tm.CurrentNodeID,
		LeaderEpoch:  1,
		Replicas:     make(map[string]*ReplicaState),
		Log:          logManager,
		Logger:       tm.Logger,
	}
	tm.Topics[topic] = topicObj

	var otherNodes []*NodeMetadata
	for _, node := range tm.Nodes {
		if node != nil && node.NodeID != tm.CurrentNodeID {
			otherNodes = append(otherNodes, node)
		}
	}
	if len(otherNodes) < replicaCount {
		delete(tm.Topics, topic)
		logManager.Close()
		return nil, ErrNotEnoughNodesf(replicaCount, len(otherNodes))
	}

	replicaNodeIds = []string{}
	tm.Logger.Info("creating topic with replicas", zap.String("topic", topic), zap.Int("replica_count", replicaCount))

	i := 0
	for _, node := range otherNodes {
		if i >= replicaCount {
			break
		}
		replClient, err := node.GetReplicationClient()
		if err != nil {
			delete(tm.Topics, topic)
			logManager.Close()
			return nil, ErrGetRpcClient(node.NodeID, err)
		}
		tm.Logger.Info("creating replica on node", zap.String("topic", topic), zap.String("node_id", node.NodeID), zap.String("node_addr", node.RpcAddr))
		_, err = replClient.CreateReplica(context.Background(), &protocol.CreateReplicaRequest{
			Topic:    topic,
			LeaderId: tm.CurrentNodeID,
		})
		if err != nil {
			delete(tm.Topics, topic)
			logManager.Close()
			return nil, ErrCreateReplicaOnNode(node.NodeID, err)
		}
		replicaNodeIds = append(replicaNodeIds, node.NodeID)
		i++
	}

	tm.Logger.Info("topic created", zap.String("topic", topic), zap.Strings("replica_node_ids", replicaNodeIds))
	return replicaNodeIds, nil
}

func (tm *TopicManager) CreateTopicWithForwarding(ctx context.Context, req *protocol.CreateTopicRequest) (*protocol.CreateTopicResponse, error) {
	if tm.coordinator == nil {
		return nil, fmt.Errorf("topic: no coordinator")
	}
	c := tm.coordinator

	// We are the designated topic leader: create locally and return replica set for Raft leader to apply.
	if req.DesignatedLeaderNodeID != "" && tm.CurrentNodeID == req.DesignatedLeaderNodeID {
		tm.Logger.Info("creating topic as designated leader", zap.String("topic", req.Topic))
		replicaNodeIds, err := tm.CreateTopic(req.Topic, int(req.ReplicaCount))
		if err != nil {
			return nil, ErrCreateTopic(err)
		}
		if err := c.ApplyCreateTopicEvent(req.Topic, req.ReplicaCount, tm.CurrentNodeID, replicaNodeIds); err != nil {
			return nil, err
		}
		return &protocol.CreateTopicResponse{Topic: req.Topic, ReplicaNodeIds: replicaNodeIds}, nil
	}

	// Not the designated leader: if we're not Raft leader, forward to Raft leader.
	if !c.IsLeader() {
		tm.Logger.Debug("forwarding create topic to Raft leader", zap.String("topic", req.Topic))
		leaderNodeID, err := c.GetRaftLeaderNodeID()
		if err != nil {
			return nil, ErrCannotReachLeader
		}
		node := tm.Nodes[leaderNodeID]
		if node == nil {
			return nil, ErrCannotReachLeader
		}
		cl, err := node.GetRpcClient()
		if err != nil {
			return nil, ErrCannotReachLeader
		}
		return cl.CreateTopic(ctx, req)
	}

	// We are the Raft leader: decide topic leader from metadata, set designated leader, forward to that node.
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

	if leaderNodeID == tm.CurrentNodeID {
		tm.Logger.Info("selected node is current node, creating topic locally", zap.String("topic", req.Topic))
		replicaNodeIds, err := tm.CreateTopic(req.Topic, int(req.ReplicaCount))
		if err != nil {
			return nil, ErrCreateTopic(err)
		}
		if err := c.ApplyCreateTopicEvent(req.Topic, req.ReplicaCount, leaderNodeID, replicaNodeIds); err != nil {
			return nil, err
		}
		return &protocol.CreateTopicResponse{Topic: req.Topic, ReplicaNodeIds: replicaNodeIds}, nil
	}
	leaderNode := tm.Nodes[leaderNodeID]
	if leaderNode == nil {
		return nil, ErrCannotReachLeader
	}
	tm.Logger.Info("forwarding create topic to topic leader", zap.String("topic", req.Topic), zap.String("topic_leader_id", leaderNodeID), zap.String("topic_leader_addr", leaderNode.RpcAddr))

	cl, err := leaderNode.GetRpcClient()
	if err != nil {
		return nil, err
	}
	forwardReq := *req
	forwardReq.DesignatedLeaderNodeID = leaderNodeID
	resp, err := cl.CreateTopic(ctx, &forwardReq)
	if err != nil {
		return nil, ErrForwardToTopicLeader(err)
	}
	if err := c.ApplyCreateTopicEvent(req.Topic, req.ReplicaCount, leaderNodeID, resp.ReplicaNodeIds); err != nil {
		return nil, err
	}
	return resp, nil
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

// DeleteTopic deletes a topic (caller must be leader). Closes and deletes leader log; deletes replicas on remote nodes.
func (tm *TopicManager) DeleteTopic(topic string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	topicObj, ok := tm.Topics[topic]
	if !ok {
		return ErrTopicNotFoundf(topic)
	}

	tm.Logger.Info("deleting topic", zap.String("topic", topic))

	if topicObj.Log != nil {
		topicObj.Log.Close()
		topicObj.Log.Delete()
		topicObj.Log = nil
	}

	ctx := context.Background()
	topicObj.mu.RLock()
	replicaIDs := make([]string, 0, len(topicObj.Replicas))
	for rid := range topicObj.Replicas {
		replicaIDs = append(replicaIDs, rid)
	}
	topicObj.mu.RUnlock()
	for _, replicaID := range replicaIDs {
		node := tm.Nodes[replicaID]
		if node == nil {
			continue
		}
		replicaClient, err := node.GetRpcClient()
		if err != nil {
			continue
		}
		_, _ = replicaClient.DeleteReplica(ctx, &protocol.DeleteReplicaRequest{
			Topic: topic,
		})
	}

	delete(tm.Topics, topic)
	return nil
}

// DeleteTopicWithForwarding deletes a topic. If this node is the topic leader, deletes locally and applies DeleteTopicEvent.
// Otherwise, when node is set, forwards the request to the topic leader. When node is nil, deletes locally only (fails if not leader).
func (tm *TopicManager) DeleteTopicWithForwarding(ctx context.Context, req *protocol.DeleteTopicRequest) (*protocol.DeleteTopicResponse, error) {
	// If we have the topic and we're the leader, delete locally and apply metadata event.
	topicObj, err := tm.GetTopic(req.Topic)
	if err == nil && topicObj != nil {
		isLeader, _ := tm.IsLeader(req.Topic)
		if isLeader {
			if err := tm.DeleteTopic(req.Topic); err != nil {
				return nil, ErrDeleteTopic(err)
			}
			if err := tm.applyDeleteTopicEventOnRaftLeader(ctx, req.Topic); err != nil {
				return nil, ErrApplyDeleteTopic(err)
			}
			return &protocol.DeleteTopicResponse{Topic: req.Topic}, nil
		}
	}
	// Forward to topic leader.
	tm.mu.RLock()
	t := tm.Topics[req.Topic]
	tm.mu.RUnlock()
	if t == nil {
		return nil, ErrTopicNotFoundf(req.Topic)
	}
	leaderNode := tm.Nodes[t.LeaderNodeID]
	if leaderNode == nil {
		return nil, ErrTopicNotFoundf(req.Topic)
	}
	leaderClient, err := leaderNode.GetRpcClient()
	if err != nil {
		return nil, err
	}
	return leaderClient.DeleteTopic(ctx, req)
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
			if err := tm.CreateReplicaRemote(topicName, leaderID); err != nil {
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

// CreateReplicaRemote creates a replica for the topic on this node (called by leader via RPC).
func (tm *TopicManager) CreateReplicaRemote(topic string, leaderId string) error {
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

// DeleteReplicaRemote deletes a replica for the topic on this node.
func (tm *TopicManager) DeleteReplicaRemote(topic string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	topicObj, ok := tm.Topics[topic]
	if !ok {
		return ErrTopicNotFoundf(topic)
	}
	tm.Logger.Info("deleting replica for topic", zap.String("topic", topic))
	if topicObj.Log != nil {
		topicObj.Log.Delete()
		topicObj.Log = nil
	}
	delete(tm.Topics, topic)
	topicDir := filepath.Join(tm.BaseDir, topic)
	if err := os.RemoveAll(topicDir); err != nil {
		return ErrRemoveTopicDir(err)
	}
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

func (tm *TopicManager) ReportLEO(ctx context.Context, topicName string, leaderNodeID string) error {
	tm.mu.RLock()
	leaderNode := tm.Nodes[leaderNodeID]
	t := tm.Topics[topicName]
	tm.mu.RUnlock()
	if leaderNode == nil || t == nil {
		return nil
	}
	return tm.reportLEOLocked(ctx, t, leaderNode)
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

func (tm *TopicManager) reportLEOLocked(ctx context.Context, t *Topic, leaderNode *NodeMetadata) error {
	t.mu.RLock()
	log := t.Log
	t.mu.RUnlock()
	if log == nil {
		return nil
	}
	req := &protocol.RecordLEORequest{
		NodeID: tm.CurrentNodeID,
		Topic:  t.Name,
		Leo:    int64(log.LEO()),
	}
	leaderClient, err := leaderNode.GetRpcClient()
	if err != nil {
		return err
	}
	_, err = leaderClient.RecordLEO(ctx, req)
	return err
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

// RecordLEORemote records the LEO of a replica (topic leader only). Called by RPC when a replica reports its LEO.
func (tm *TopicManager) RecordLEORemote(nodeId string, topic string, leo uint64, leoTime time.Time) error {
	if nodeId == "" {
		return ErrNodeIDRequired
	}
	tm.mu.RLock()
	topicObj, ok := tm.Topics[topic]
	if !ok {
		tm.mu.RUnlock()
		return ErrTopicNotFoundf(topic)
	}
	topicObj.mu.RLock()
	leaderLog := topicObj.Log
	replicaStates := topicObj.Replicas
	topicObj.mu.RUnlock()
	tm.mu.RUnlock()
	if replicaStates[nodeId] == nil {
		return ErrReplicaNotFoundf(nodeId, topic)
	}

	var isr bool
	if leaderLog != nil {
		leaderLEO := leaderLog.LEO()
		if leaderLEO >= 100 && leo >= leaderLEO-100 {
			isr = true
		} else if leaderLEO < 100 {
			isr = leo >= leaderLEO
		}
	}

	tm.mu.Lock()
	if t := tm.Topics[topic]; t != nil && t.Replicas[nodeId] != nil {
		t.Replicas[nodeId].LEO = int64(leo)
		t.Replicas[nodeId].IsISR = isr
	}
	tm.mu.Unlock()
	tm.maybeAdvanceHW(topicObj)
	if err := tm.applyIsrUpdateEventOnRaftLeader(context.Background(), topic, nodeId, isr, int64(leo)); err != nil {
		tm.Logger.Debug("apply ISR update on Raft leader failed", zap.String("topic", topic), zap.String("replica", nodeId), zap.Error(err))
	}
	return nil
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
		t := &Topic{
			Name:         e.Topic,
			LeaderNodeID: e.LeaderNodeID,
			LeaderEpoch:  e.LeaderEpoch,
			Replicas:     make(map[string]*ReplicaState),
			Logger:       tm.Logger,
		}
		for _, replica := range e.ReplicaNodeIds {
			t.Replicas[replica] = &ReplicaState{
				ReplicaNodeID: replica,
				LEO:           0,
				IsISR:         true,
			}
		}
		tm.Topics[e.Topic] = t
		tm.ensureLocalLogForTopicLocked(e.Topic, e.LeaderNodeID, e.ReplicaNodeIds)
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
			tm.ensureLocalLogAfterLeaderChangeLocked(e.Topic, e.LeaderNodeID)
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
		tm.removeTopicLocalLocked(e.Topic)
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

// ensureLocalLogForTopicLocked opens the local log for the topic if this node is leader or replica. Caller holds tm.mu.
func (tm *TopicManager) ensureLocalLogForTopicLocked(topicName, leaderNodeID string, replicaNodeIds []string) {
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

// ensureLocalLogAfterLeaderChangeLocked updates local log after leader change (promote or demote). Caller holds tm.mu.
func (tm *TopicManager) ensureLocalLogAfterLeaderChangeLocked(topicName, newLeaderID string) {
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

// removeTopicLocalLocked closes the topic log, removes from map, and deletes the topic dir. Caller holds tm.mu.
func (tm *TopicManager) removeTopicLocalLocked(topicName string) {
	t, ok := tm.Topics[topicName]
	if !ok {
		return
	}
	if t != nil && t.Log != nil {
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
