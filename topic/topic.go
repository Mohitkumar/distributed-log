package topic

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/common"
	"github.com/mohitkumar/mlog/coordinator"
	"github.com/mohitkumar/mlog/log"
	"github.com/mohitkumar/mlog/protocol"
	"go.uber.org/zap"
)

const defaultMetadataLogInterval = 30 * time.Second

type NodeMetadata struct {
	mu                sync.RWMutex
	NodeID            string `json:"node_id"`
	Addr              string `json:"addr"`
	RpcAddr           string `json:"rpc_addr"`
	remoteClient      *client.RemoteClient
	replicationClient *client.RemoteClient
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
	mu           sync.RWMutex
	Name         string                   `json:"name"`
	LeaderNodeID string                   `json:"leader_id"`
	LeaderEpoch  int64                    `json:"leader_epoch"`
	Replicas     map[string]*ReplicaState `json:"replicas"`
	Log          *log.LogManager
	Logger       *zap.Logger
}

var _ coordinator.MetadataStore = (*TopicManager)(nil)

type TopicManager struct {
	mu            sync.RWMutex
	Topics        map[string]*Topic `json:"topics"`
	BaseDir       string
	Logger        *zap.Logger
	Nodes         map[string]*NodeMetadata `json:"nodes"`
	CurrentNodeID string                   `json:"current_node_id"`
	coordinator   *coordinator.Coordinator
	stopPeriodic  chan struct{}
}

func NewTopicManager(baseDir string, coordinator *coordinator.Coordinator, logger *zap.Logger) (*TopicManager, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	tm := &TopicManager{
		Topics:      make(map[string]*Topic),
		BaseDir:     baseDir,
		Logger:      logger,
		Nodes:       make(map[string]*NodeMetadata),
		coordinator: coordinator,
	}
	go tm.periodicLog(defaultMetadataLogInterval)
	return tm, nil
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
	return topicObj.LeaderNodeID, nil
}

// applyDeleteTopicEventOnRaftLeader gets the Raft leader RPC address and calls ApplyDeleteTopicEvent there.
func (tm *TopicManager) applyDeleteTopicEventOnRaftLeader(ctx context.Context, topic string) error {
	leaderNodeID, err := tm.coordinator.GetRaftLeaderNodeID()
	if err != nil {
		return err
	}

	client, err := tm.Nodes[leaderNodeID].GetRpcClient()
	if err != nil {
		return err
	}
	_, err = client.ApplyDeleteTopicEvent(ctx, &protocol.ApplyDeleteTopicEventRequest{Topic: topic})
	if err != nil {
		return err
	}
	return nil
}

// applyIsrUpdateEventOnRaftLeader gets the Raft leader RPC address and calls ApplyIsrUpdateEvent there.
func (tm *TopicManager) applyIsrUpdateEventOnRaftLeader(ctx context.Context, topic, replicaNodeID string, isr bool, leo int64) error {
	leaderNodeID, err := tm.coordinator.GetRaftLeaderNodeID()
	if err != nil {
		return err
	}

	client, err := tm.Nodes[leaderNodeID].GetRpcClient()
	if err != nil {
		return err
	}
	_, err = client.ApplyIsrUpdateEvent(ctx, &protocol.ApplyIsrUpdateEventRequest{Topic: topic, ReplicaNodeID: replicaNodeID, Isr: isr, Leo: leo})
	if err != nil {
		return err
	}
	return nil
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
		Log:          logManager,
		Logger:       tm.Logger,
		LeaderNodeID: tm.CurrentNodeID,
	}
	tm.Topics[topic] = topicObj

	otherNodes := tm.Nodes

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
		tm.Logger.Info("creating replica on node", zap.String("topic", topic), zap.String("node_id", node.NodeID), zap.String("node_addr", node.RPCAddr), zap.Error(err))
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
	c := tm.coordinator

	// We are the designated topic leader: create locally and return replica set for Raft leader to apply.
	if req.DesignatedLeaderNodeID != "" && tm.CurrentNodeID == req.DesignatedLeaderNodeID {
		tm.Logger.Info("creating topic as designated leader", zap.String("topic", req.Topic))
		replicaNodeIds, err := tm.CreateTopic(req.Topic, int(req.ReplicaCount))
		if err != nil {
			return nil, ErrCreateTopic(err)
		}
		return &protocol.CreateTopicResponse{Topic: req.Topic, ReplicaNodeIds: replicaNodeIds}, nil
	}

	// Not the designated leader: if we're not Raft leader, forward to Raft leader.
	if !c.IsLeader() {
		tm.Logger.Debug("forwarding create topic to Raft leader", zap.String("topic", req.Topic))
		leaderNodeID, err := tm.coordinator.GetRaftLeaderNodeID()
		if err != nil {
			return nil, ErrCannotReachLeader
		}
		client, err := tm.Nodes[leaderNodeID].GetRpcClient()
		if err != nil {
			return nil, ErrCannotReachLeader
		}
		return client.CreateTopic(ctx, req)
	}

	// We are the Raft leader: decide topic leader from metadata, set designated leader, forward to that node.
	if _, exists := tm.Topics[req.Topic]; exists {
		return nil, ErrTopicExistsf(req.Topic)
	}
	topicLeaderNode, err := c.GetNodeIDWithLeastTopics()
	if err != nil {
		return nil, err
	}

	if topicLeaderNode.NodeID == tm.currentNodeID() {
		tm.Logger.Info("selected node is current node, creating topic locally", zap.String("topic", req.Topic))
		replicaNodeIds, err := tm.CreateTopic(req.Topic, int(req.ReplicaCount))
		if err != nil {
			return nil, ErrCreateTopic(err)
		}
		c.ApplyEvent(NewCreateTopicApplyEvent(req.Topic, req.ReplicaCount, topicLeaderNode.NodeID, replicaNodeIds))
		return &protocol.CreateTopicResponse{Topic: req.Topic, ReplicaNodeIds: replicaNodeIds}, nil
	}
	tm.Logger.Info("forwarding create topic to topic leader", zap.String("topic", req.Topic), zap.String("topic_leader_id", topicLeaderNode.NodeID), zap.String("topic_leader_addr", topicLeaderNode.RPCAddr))

	// Forward to topic leader with DesignatedLeaderNodeID so it creates locally instead of forwarding back.
	topicLeaderClient, err := c.GetRpcClient(topicLeaderNode.NodeID)
	if err != nil {
		return nil, err
	}
	forwardReq := *req
	forwardReq.DesignatedLeaderNodeID = topicLeaderNode.NodeID
	resp, err := topicLeaderClient.CreateTopic(ctx, &forwardReq)
	if err != nil {
		return nil, ErrForwardToTopicLeader(err)
	}
	replicaNodeIds := resp.ReplicaNodeIds
	c.ApplyEvent(NewCreateTopicApplyEvent(req.Topic, req.ReplicaCount, topicLeaderNode.NodeID, replicaNodeIds))
	return resp, nil
}

func (tm *TopicManager) GetNodeIDWithLeastTopics() (string, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	leastTopics := len(tm.Topics)
	for _, topic := range tm.Topics {
		if topicCount < leastTopics {
			leastTopics = len(topic.Replicas)
		}
	}
	return leastTopics.NodeID, nil
}

// DeleteTopic deletes a topic (caller must be leader). Closes and deletes leader log; deletes replicas on remote nodes.
func (tm *TopicManager) DeleteTopic(topic string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	topicObj, ok := tm.topics[topic]
	if !ok {
		return ErrTopicNotFoundf(topic)
	}

	tm.Logger.Info("deleting topic", zap.String("topic", topic))

	if topicObj.Log != nil {
		topicObj.Log.Close()
		topicObj.Log.Delete()
	}

	ctx := context.Background()
	replicaNodes, err := tm.coordinator.GetTopicReplicaNodes(topic)
	if err != nil {
		return err
	}
	for _, replica := range replicaNodes {
		replicaClient, err := tm.coordinator.GetRpcClient(replica.NodeID)
		if err != nil {
			continue
		}
		_, _ = replicaClient.DeleteReplica(ctx, &protocol.DeleteReplicaRequest{
			Topic: topic,
		})
	}

	delete(tm.topics, topic)
	return nil
}

// DeleteTopicWithForwarding deletes a topic. If this node is the topic leader, deletes locally and applies DeleteTopicEvent.
// Otherwise, when node is set, forwards the request to the topic leader. When node is nil, deletes locally only (fails if not leader).
func (tm *TopicManager) DeleteTopicWithForwarding(ctx context.Context, req *protocol.DeleteTopicRequest) (*protocol.DeleteTopicResponse, error) {
	// If we have the topic and we're the leader, delete locally and apply metadata event.
	topicObj, err := tm.GetTopic(req.Topic)
	if err == nil && topicObj != nil {
		if tm.IsLeader(req.Topic) {
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
	leaderNode, err := tm.coordinator.GetTopicLeaderNode(req.Topic)
	if err != nil {
		return nil, ErrTopicNotFoundf(req.Topic)
	}
	leaderClient, err := tm.coordinator.GetRpcClient(leaderNode.NodeID)
	if err != nil {
		return nil, err
	}
	return leaderClient.DeleteTopic(ctx, req)
}

// GetLeader returns the leader log view for a topic (this node must be the leader).
func (tm *TopicManager) GetLeader(topic string) (*log.LogManager, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	if !tm.IsLeader(topic) {
		return nil, ErrThisNodeNotLeaderf(topic)
	}
	return tm.topics[topic].Log, nil
}

// GetTopic returns the topic object.
func (tm *TopicManager) GetTopic(topic string) (*Topic, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	topicObj, ok := tm.topics[topic]
	if !ok {
		return nil, ErrTopicNotFoundf(topic)
	}
	return topicObj, nil
}

// RestoreFromMetadata rebuilds the topic manager from Raft-backed metadata (after node restart).
// Call once after Raft has replayed log/snapshot so metadata is up to date.
func (tm *TopicManager) RestoreFromMetadata() error {
	topicNames := tm.coordinator.ListTopicNames()
	if len(topicNames) == 0 {
		return nil
	}
	tm.Logger.Info("restoring topic manager from metadata", zap.Int("topic_count", len(topicNames)), zap.Strings("topics", topicNames))
	for _, topic := range topicNames {
		leaderNode, err := tm.coordinator.GetTopicLeaderNode(topic)
		if err != nil {
			continue
		}
		leaderID := leaderNode.NodeID
		if leaderID == tm.currentNodeID() {
			if err := tm.restoreLeaderTopic(topic); err != nil {
				tm.Logger.Warn("restore leader topic failed", zap.String("topic", topic), zap.Error(err))
				continue
			}
		} else {
			if err := tm.CreateReplicaRemote(topic, leaderID); err != nil {
				tm.Logger.Warn("restore replica topic failed", zap.String("topic", topic), zap.Error(err))
				continue
			}
		}
	}
	return nil
}

// restoreLeaderTopic rebuilds a leader topic from metadata: open local log and reconnect to replica clients (no RPC to replicas).
func (tm *TopicManager) restoreLeaderTopic(topic string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.topics[topic]; exists {
		return nil
	}

	logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topic))
	if err != nil {
		return ErrCreateLog(err)
	}

	topicObj := &Topic{
		Name:   topic,
		Log:    logManager,
		Logger: tm.Logger,
	}
	tm.topics[topic] = topicObj

	tm.Logger.Info("restored leader topic from metadata", zap.String("topic", topic))
	return nil
}

// CreateReplicaRemote creates a replica for the topic on this node (called by leader via RPC).
func (tm *TopicManager) CreateReplicaRemote(topic string, leaderId string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	topicObj, ok := tm.topics[topic]
	if !ok {
		topicObj = &Topic{
			Name:         topic,
			Logger:       tm.Logger,
			LeaderNodeID: leaderId,
		}
		tm.topics[topic] = topicObj
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

	// Replication is driven by the coordinator replication thread (connection pipelining per leader).
	return nil
}

// DeleteReplicaRemote deletes a replica for the topic on this node.
func (tm *TopicManager) DeleteReplicaRemote(topic string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	topicObj, ok := tm.topics[topic]
	if !ok {
		return ErrTopicNotFoundf(topic)
	}

	tm.Logger.Info("deleting replica for topic", zap.String("topic", topic))

	if topicObj.Log != nil {
		topicObj.Log.Delete()
		topicObj.Log = nil
	}

	delete(tm.topics, topic)

	topicDir := filepath.Join(tm.BaseDir, topic)
	if err := os.RemoveAll(topicDir); err != nil {
		return ErrRemoveTopicDir(err)
	}
	return nil
}

// ReplicationTarget implementation (coordinator calls these from its replication thread).

func (tm *TopicManager) ListReplicaTopics() []ReplicaTopicInfo {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	currentNodeID := tm.currentNodeID()
	var out []ReplicaTopicInfo
	for name, t := range tm.topics {
		if t == nil || t.Log == nil || t.LeaderNodeID == currentNodeID {
			continue
		}
		out = append(out, ReplicaTopicInfo{TopicName: name, LeaderNodeID: t.LeaderNodeID})
	}
	return out
}

func (tm *TopicManager) GetLEO(topicName string) (uint64, bool) {
	tm.mu.RLock()
	t := tm.topics[topicName]
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
	t := tm.topics[topicName]
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
	leaderNode, err := tm.coordinator.GetNode(leaderNodeID)
	if err != nil || leaderNode == nil {
		return err
	}
	tm.mu.RLock()
	t := tm.topics[topicName]
	tm.mu.RUnlock()
	if t == nil {
		return nil
	}
	return tm.reportLEO(ctx, t, leaderNode)
}

func (tm *TopicManager) reportLEO(ctx context.Context, t *Topic, leaderNode *common.Node) error {
	t.mu.RLock()
	log := t.Log
	t.mu.RUnlock()
	if log == nil {
		return nil
	}
	req := &protocol.RecordLEORequest{
		NodeID: tm.currentNodeID(),
		Topic:  t.Name,
		Leo:    int64(log.LEO()),
	}
	leaderClient, err := tm.coordinator.GetRpcClient(leaderNode.NodeID)
	if err != nil {
		return err
	}
	_, err = leaderClient.RecordLEO(ctx, req)
	if err != nil {
		return err
	}
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
		replicaStates := tm.coordinator.GetTopicReplicaStates(t.Name)
		for _, r := range replicaStates {
			if r.IsISR {
				useISR = true
			}
		}
		for _, replica := range replicaStates {
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
	t.mu.RUnlock()
	for _, r := range tm.coordinator.GetTopicReplicaStates(t.Name) {
		if r != nil && uint64(r.LEO) < minOffset {
			minOffset = uint64(r.LEO)
		}
	}
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
	topicObj, ok := tm.topics[topic]
	if !ok {
		tm.mu.RUnlock()
		return ErrTopicNotFoundf(topic)
	}
	topicObj.mu.RLock()
	leaderLog := topicObj.Log
	topicObj.mu.RUnlock()
	tm.mu.RUnlock()

	replicaStates := tm.coordinator.GetTopicReplicaStates(topic)
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
		tm.Topics[e.Topic] = &Topic{
			LeaderNodeID: e.LeaderNodeID,
			LeaderEpoch:  e.LeaderEpoch,
			Replicas:     make(map[string]*ReplicaState),
		}
		for _, replica := range e.ReplicaNodeIds {
			tm.Topics[e.Topic].Replicas[replica] = &ReplicaState{
				ReplicaNodeID: replica,
				LEO:           0,
				IsISR:         true,
			}
		}
	case protocol.MetadataEventTypeLeaderChange:
		e := protocol.LeaderChangeEvent{}
		if err := json.Unmarshal(ev.Data, &e); err != nil {
			return err
		}
		tm := tm.Topics[e.Topic]
		if tm != nil {
			tm.LeaderNodeID = e.LeaderNodeID
			tm.LeaderEpoch = e.LeaderEpoch
			delete(tm.Replicas, e.LeaderNodeID)
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
		delete(tm.Topics, e.Topic)
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

func (c *TopicManager) maybeReassignTopicLeaders(nodeID string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if c.raft.State() != raft.Leader {
		return
	}
	topicsCopy := c.metadataStore.GetTopicsCopy()
	for topic, tm := range topicsCopy {
		if tm == nil || tm.LeaderNodeID != nodeID {
			continue
		}
		var newLeader string
		for rid, rs := range tm.Replicas {
			if rid == nodeID || rs == nil || !rs.IsISR {
				continue
			}
			if c.metadataStore.GetNodeMetadata(rid) == nil {
				continue
			}
			newLeader = rid
			break
		}
		if newLeader == "" {
			c.Logger.Warn("no ISR replica available to take leadership", zap.String("topic", topic), zap.String("old_leader_node_id", nodeID))
			continue
		}
		nextEpoch := tm.LeaderEpoch + 1
		if err := c.ApplyLeaderChangeEvent(topic, newLeader, nextEpoch); err != nil {
			c.Logger.Warn("apply leader change failed", zap.String("topic", topic), zap.Error(err))
		}
	}
}

func (tm *TopicManager) Restore(data []byte) error {
	var decoded TopicManager
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.Topics = decoded.Topics
	tm.Nodes = decoded.Nodes
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
