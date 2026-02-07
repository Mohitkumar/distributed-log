package topic

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/log"
	"github.com/mohitkumar/mlog/node"
	"github.com/mohitkumar/mlog/protocol"
	"go.uber.org/zap"
)

// In memory representation of a topic
type Topic struct {
	mu                sync.RWMutex
	Name              string
	Log               *log.LogManager
	Logger            *zap.Logger
	stopChan          chan struct{}
	stopOnce          sync.Once
	replicationCancel context.CancelFunc
}
type TopicManager struct {
	mu      sync.RWMutex
	topics  map[string]*Topic
	BaseDir string
	Logger  *zap.Logger
	node    *node.Node
}

func NewTopicManager(baseDir string, n *node.Node, logger *zap.Logger) (*TopicManager, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	tm := &TopicManager{
		topics:  make(map[string]*Topic),
		BaseDir: baseDir,
		Logger:  logger,
		node:    n,
	}
	return tm, nil
}

func (tm *TopicManager) currentNodeID() string {
	return tm.node.GetNodeID()
}

func (tm *TopicManager) currentNodeAddr() string {
	return tm.node.GetNodeAddr()
}

func (tm *TopicManager) IsLeader(topic string) bool {
	return tm.node.GetTopicLeaderNodeID(topic) == tm.currentNodeID()
}

// applyDeleteTopicEventOnRaftLeader gets the Raft leader RPC address and calls ApplyDeleteTopicEvent there.
func (tm *TopicManager) applyDeleteTopicEventOnRaftLeader(ctx context.Context, topic string) error {
	raftLeaderAddr, err := tm.node.GetRaftLeaderRpcAddr()
	if err != nil {
		return err
	}
	cl, err := client.NewRemoteClient(raftLeaderAddr)
	if err != nil {
		return err
	}
	defer cl.Close()
	_, err = cl.ApplyDeleteTopicEvent(ctx, &protocol.ApplyDeleteTopicEventRequest{Topic: topic})
	return err
}

// applyIsrUpdateEventOnRaftLeader gets the Raft leader RPC address and calls ApplyIsrUpdateEvent there.
func (tm *TopicManager) applyIsrUpdateEventOnRaftLeader(ctx context.Context, topic, replicaNodeID string, isr bool, leo int64) error {
	raftLeaderAddr, err := tm.node.GetRaftLeaderRpcAddr()
	if err != nil {
		return err
	}
	cl, err := client.NewRemoteClient(raftLeaderAddr)
	if err != nil {
		return err
	}
	defer cl.Close()
	_, err = cl.ApplyIsrUpdateEvent(ctx, &protocol.ApplyIsrUpdateEventRequest{Topic: topic, ReplicaNodeID: replicaNodeID, Isr: isr, Leo: leo})
	return err
}

// CreateTopic creates a new topic with this node as leader and optional replicas on other nodes.
// Returns the actual replica set (this node as leader + replica node IDs in order) for metadata.
func (tm *TopicManager) CreateTopic(topic string, replicaCount int) (replicaNodeIds []string, err error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.topics[topic]; exists {
		return nil, ErrTopicExistsf(topic)
	}

	logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topic))
	if err != nil {
		return nil, ErrCreateLog(err)
	}

	topicObj := &Topic{
		Name:   topic,
		Log:    logManager,
		Logger: tm.Logger,
	}
	tm.topics[topic] = topicObj

	otherNodes := tm.node.GetOtherNodes()

	if len(otherNodes) < replicaCount {
		delete(tm.topics, topic)
		logManager.Close()
		return nil, ErrNotEnoughNodesf(replicaCount, len(otherNodes))
	}

	replicaNodeIds = []string{}
	tm.Logger.Info("creating topic with replicas", zap.String("topic", topic), zap.Int("replica_count", replicaCount))

	for i := 0; i < replicaCount; i++ {
		node := otherNodes[i]

		replClient, err := client.NewRemoteClient(node.RpcAddr)
		tm.Logger.Info("creating replica on node", zap.String("topic", topic), zap.String("node_id", node.NodeID), zap.String("node_addr", node.RpcAddr), zap.Error(err))
		if err != nil {
			delete(tm.topics, topic)
			logManager.Close()
			return nil, ErrCreateReplicationClient(node.RpcAddr, err)
		}

		_, err = replClient.CreateReplica(context.Background(), &protocol.CreateReplicaRequest{
			Topic:      topic,
			LeaderAddr: tm.currentNodeAddr(),
		})
		if err != nil {
			delete(tm.topics, topic)
			logManager.Close()
			return nil, ErrCreateReplicaOnNode(node.NodeID, err)
		}
		replicaNodeIds = append(replicaNodeIds, node.NodeID)
	}

	tm.Logger.Info("topic created", zap.String("topic", topic), zap.Strings("replica_node_ids", replicaNodeIds))
	return replicaNodeIds, nil
}

func (tm *TopicManager) CreateTopicWithForwarding(ctx context.Context, req *protocol.CreateTopicRequest) (*protocol.CreateTopicResponse, error) {
	n := tm.node

	// We are the designated topic leader: create locally and return replica set for Raft leader to apply.
	if req.DesignatedLeaderNodeID != "" && req.DesignatedLeaderNodeID == n.NodeID {
		tm.Logger.Info("creating topic as designated leader", zap.String("topic", req.Topic))
		replicaNodeIds, err := tm.CreateTopic(req.Topic, int(req.ReplicaCount))
		if err != nil {
			return nil, ErrCreateTopic(err)
		}
		return &protocol.CreateTopicResponse{Topic: req.Topic, ReplicaNodeIds: replicaNodeIds}, nil
	}

	// Not the designated leader: if we're not Raft leader, forward to Raft leader.
	if !n.IsLeader() {
		tm.Logger.Debug("forwarding create topic to Raft leader", zap.String("topic", req.Topic))
		leaderAddr, err := n.GetRaftLeaderRpcAddr()
		if err != nil {
			return nil, ErrCannotReachLeader
		}
		remote, err := client.NewRemoteClient(leaderAddr)
		if err != nil {
			return nil, ErrForwardToLeader(err)
		}
		defer remote.Close()
		return remote.CreateTopic(ctx, req)
	}

	// We are the Raft leader: decide topic leader from metadata, set designated leader, forward to that node.
	if n.TopicExists(req.Topic) {
		return nil, ErrTopicExistsf(req.Topic)
	}
	topicLeaderID, err := n.GetNodeIDWithLeastTopics()
	if err != nil {
		return nil, err
	}
	topicLeaderAddr, err := n.GetRpcAddrForNodeID(topicLeaderID)
	if err != nil {
		return nil, ErrNoRPCForTopicLeaderf(topicLeaderID)
	}

	tm.Logger.Info("forwarding create topic to topic leader", zap.String("topic", req.Topic), zap.String("topic_leader_id", topicLeaderID), zap.String("topic_leader_addr", topicLeaderAddr))

	// Forward to topic leader with DesignatedLeaderNodeID so it creates locally instead of forwarding back.
	forwardReq := *req
	forwardReq.DesignatedLeaderNodeID = topicLeaderID
	remote, err := client.NewRemoteClient(topicLeaderAddr)
	if err != nil {
		return nil, ErrForwardToTopicLeader(err)
	}
	defer remote.Close()
	resp, err := remote.CreateTopic(ctx, &forwardReq)
	if err != nil {
		return nil, ErrForwardToTopicLeader(err)
	}
	// Use replica set from topic leader so metadata matches actual replicas.
	replicaNodeIds := resp.ReplicaNodeIds
	if len(replicaNodeIds) == 0 {
		replicaNodeIds = []string{topicLeaderID}
		for _, o := range n.GetOtherNodes() {
			if len(replicaNodeIds) >= int(req.ReplicaCount) {
				break
			}
			replicaNodeIds = append(replicaNodeIds, o.NodeID)
		}
	}
	if err := n.ApplyCreateTopicEvent(req.Topic, req.ReplicaCount, topicLeaderID, replicaNodeIds); err != nil {
		return nil, ErrApplyCreateTopic(err)
	}
	return resp, nil
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
	for _, replica := range tm.node.GetTopicReplicaClients(topic) {
		if replica == nil {
			continue
		}
		_, _ = replica.DeleteReplica(ctx, &protocol.DeleteReplicaRequest{
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
	leaderAddr, err := tm.node.GetTopicLeaderRpcAddr(req.Topic)
	if err != nil {
		return nil, ErrTopicNotFoundf(req.Topic)
	}
	remote, err := client.NewRemoteClient(leaderAddr)
	if err != nil {
		return nil, ErrForwardToTopicLeader(err)
	}
	defer remote.Close()
	return remote.DeleteTopic(ctx, req)
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
	topicNames := tm.node.ListTopicNames()
	if len(topicNames) == 0 {
		return nil
	}
	tm.Logger.Info("restoring topic manager from metadata", zap.Int("topic_count", len(topicNames)), zap.Strings("topics", topicNames))
	for _, topic := range topicNames {
		leaderID := tm.node.GetTopicLeaderNodeID(topic)
		if leaderID == "" {
			continue
		}
		if leaderID == tm.currentNodeID() {
			if err := tm.restoreLeaderTopic(topic); err != nil {
				tm.Logger.Warn("restore leader topic failed", zap.String("topic", topic), zap.Error(err))
				continue
			}
		} else {
			leaderAddr, err := tm.node.GetTopicLeaderRpcAddr(topic)
			if err != nil {
				tm.Logger.Warn("restore replica: cannot get leader addr", zap.String("topic", topic), zap.Error(err))
				continue
			}
			if err := tm.CreateReplicaRemote(topic, leaderAddr); err != nil {
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
func (tm *TopicManager) CreateReplicaRemote(topic string, leaderAddr string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	topicObj, ok := tm.topics[topic]
	if !ok {
		topicObj = &Topic{
			Name:   topic,
			Logger: tm.Logger,
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
	tm.Logger.Info("replica created for topic", zap.String("topic", topic), zap.String("leader_addr", leaderAddr))

	if err := tm.StartReplication(topicObj); err != nil {
		return ErrStartReplication(err)
	}

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

	tm.StopReplication(topicObj)
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

// StartReplication starts the replication loop (topic must be a replica). Fetches from leader and appends to log; reports LEO.
func (tm *TopicManager) StartReplication(t *Topic) error {
	t.mu.Lock()
	if t.stopChan != nil {
		t.mu.Unlock()
		return ErrReplicationStartedf(t.Name)
	}
	t.stopChan = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	t.replicationCancel = cancel
	t.mu.Unlock()

	if t.Logger != nil {
		t.Logger.Info("replication started for topic", zap.String("topic", t.Name))
	}
	go tm.runReplication(t, ctx)
	return nil
}

// StopReplication stops the replication loop.
func (tm *TopicManager) StopReplication(t *Topic) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	t.stopOnce.Do(func() {
		if t.Logger != nil {
			t.Logger.Info("stopping replication for topic", zap.String("topic", t.Name))
		}
		t.mu.Lock()
		if t.replicationCancel != nil {
			t.replicationCancel()
			t.replicationCancel = nil
		}
		ch := t.stopChan
		t.stopChan = nil
		t.mu.Unlock()
		if ch != nil {
			close(ch)
		}
	})
}

func (tm *TopicManager) runReplication(t *Topic, ctx context.Context) {
	backoff := 50 * time.Millisecond
	maxBackoff := 1 * time.Second

	for {
		select {
		case <-t.stopChan:
			return
		case <-ctx.Done():
			return
		default:
		}

		// Send one request to leader (non-blocking). Then block in apply loop until stream is done.
		currentOffset := t.Log.LEO()
		req := &protocol.ReplicateRequest{
			Topic:     t.Name,
			Offset:    currentOffset,
			BatchSize: 1000,
		}
		leaderStreamClient := tm.node.GetTopicLeaderStreamClient(t.Name)
		if leaderStreamClient == nil {
			if t.Logger != nil {
				t.Logger.Warn("leader stream client not found, reconnecting", zap.String("topic", t.Name))
			}
			time.Sleep(backoff)
			continue
		}
		if err := leaderStreamClient.ReplicateStream(ctx, req); err != nil {
			if t.Logger != nil {
				t.Logger.Warn("replicate stream error, reconnecting", zap.String("topic", t.Name), zap.Error(err))
			}
			time.Sleep(backoff)
			if backoff < maxBackoff {
				backoff *= 2
			}
			continue
		}
		backoff = 50 * time.Millisecond
		t.replicateStream(ctx)
	}
}

func (t *Topic) replicateStream(leaderStreamClient *client.ReplicationStreamClient, ctx context.Context) {
	for {
		select {
		case <-t.stopChan:
			return
		case <-ctx.Done():
			return
		default:
		}
		if t.Log == nil {
			return
		}

		resp, err := leaderStreamClient.Recv()
		if err != nil {
			break
		}
		if len(resp.RawChunk) > 0 {
			records, decodeErr := protocol.DecodeReplicationBatch(resp.RawChunk)
			if decodeErr != nil {
				break
			}
			var appendErr error
			for _, rec := range records {
				_, appendErr = t.Log.Append(rec.Value)
				if appendErr != nil {
					break
				}
			}
			if appendErr != nil {
				break
			}
		}
		if resp.EndOfStream {
			if t.Log != nil {
				_ = t.reportLEO(ctx, t.Log.LEO())
			}
			break
		}
	}
}

func (tm *TopicManager) reportLEO(ctx context.Context, t *Topic) error {
	req := &protocol.RecordLEORequest{
		NodeID: tm.currentNodeID(),
		Topic:  t.Name,
		Leo:    int64(t.Log.LEO()),
	}
	leaderClient := tm.node.GetTopicLeaderClient(t.Name)
	if leaderClient == nil {
		return ErrNoRPCForTopicLeaderf(t.Name)
	}
	_, err := leaderClient.RecordLEO(ctx, req)
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
		replicaStates := tm.node.GetTopicReplicaStates(t.Name)
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
	for _, r := range tm.node.GetTopicReplicaStates(t.Name) {
		if uint64(r.LEO) < minOffset {
			minOffset = uint64(r.LEO)
		}
	}
	t.mu.RUnlock()
	t.Log.SetHighWatermark(minOffset)
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
	tm.mu.RUnlock()
	replicaStates := tm.node.GetTopicReplicaStates(topic)
	replicaState, ok := replicaStates[nodeId]
	if !ok || replicaState == nil {
		return ErrReplicaNotFoundf(nodeId, topic)
	}
	replicaState.LEO = int64(leo)
	if topicObj.Log.LEO() >= 100 && leo >= topicObj.Log.LEO()-100 {
		replicaState.IsISR = true
	} else if topicObj.Log.LEO() < 100 {
		if leo >= topicObj.Log.LEO() {
			replicaState.IsISR = true
		} else {
			replicaState.IsISR = false
		}
	} else {
		replicaState.IsISR = false
	}
	tm.maybeAdvanceHW(topicObj)
	if err := tm.applyIsrUpdateEventOnRaftLeader(context.Background(), topic, nodeId, replicaState.IsISR, int64(replicaState.LEO)); err != nil {
		tm.Logger.Debug("apply ISR update on Raft leader failed", zap.String("topic", topic), zap.String("replica", nodeId), zap.Error(err))
	}
	return nil
}
