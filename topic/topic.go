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
	mu       sync.RWMutex
	Name     string
	Log      *log.LogManager
	Logger   *zap.Logger
	replicas map[string]*ReplicaInfo

	// nodeID is set for replica topics so RecordLEO can identify this node to the leader.
	nodeID string

	streamClient      *client.ReplicationStreamClient // replication stream only (send request, Recv until EndOfStream)
	rpcClient         *client.RemoteClient            // RecordLEO and other request-response RPCs to leader
	stopChan          chan struct{}
	stopOnce          sync.Once
	replicationCancel context.CancelFunc
}

// ReplicaInfo holds metadata and client for a remote replica (leader's view).
type ReplicaInfo struct {
	NodeID string
	State  ReplicaState
	client *client.RemoteClient
}

// ReplicaState tracks a replica's progress (LEO, ISR, etc.).
type ReplicaState struct {
	LastFetchTime time.Time
	LEO           uint64
	IsISR         bool
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

func (m *TopicManager) IsLeader(topic string) bool {
	return m.node.GetTopicLeaderNodeID(topic) == m.currentNodeID()
}

// CreateTopic creates a new topic with this node as leader and optional replicas on other nodes.
func (tm *TopicManager) CreateTopic(topic string, replicaCount int) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.topics[topic]; exists {
		return ErrTopicExistsf(topic)
	}

	logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topic))
	if err != nil {
		return ErrCreateLog(err)
	}

	topicObj := &Topic{
		Name:     topic,
		Log:      logManager,
		Logger:   tm.Logger,
		nodeID:   tm.currentNodeID(),
		replicas: make(map[string]*ReplicaInfo),
	}
	tm.topics[topic] = topicObj

	otherNodes := tm.node.GetOtherNodes()

	if len(otherNodes) < replicaCount {
		delete(tm.topics, topic)
		logManager.Close()
		return ErrNotEnoughNodesf(replicaCount, len(otherNodes))
	}

	tm.Logger.Info("creating topic with replicas", zap.String("topic", topic), zap.Int("replica_count", replicaCount))

	for i := 0; i < replicaCount; i++ {
		node := otherNodes[i]

		replClient, err := client.NewRemoteClient(node.RpcAddr)
		if err != nil {
			delete(tm.topics, topic)
			logManager.Close()
			return ErrCreateReplicationClient(node.RpcAddr, err)
		}

		_, err = replClient.CreateReplica(context.Background(), &protocol.CreateReplicaRequest{
			Topic:      topic,
			LeaderAddr: tm.currentNodeAddr(),
		})
		if err != nil {
			delete(tm.topics, topic)
			logManager.Close()
			return ErrCreateReplicaOnNode(node.NodeID, err)
		}

		topicObj.replicas[node.NodeID] = &ReplicaInfo{
			NodeID: node.NodeID,
			State: ReplicaState{
				LastFetchTime: time.Now(),
				LEO:           0,
				IsISR:         false,
			},
			client: replClient,
		}
	}

	tm.Logger.Info("topic created", zap.String("topic", topic))
	return nil
}

func (tm *TopicManager) CreateTopicWithForwarding(ctx context.Context, req *protocol.CreateTopicRequest) (*protocol.CreateTopicResponse, error) {
	n := tm.node

	// We are the designated topic leader: create locally and return (stops the forward loop).
	if req.DesignatedLeaderNodeID != "" && req.DesignatedLeaderNodeID == n.NodeID {
		tm.Logger.Info("creating topic as designated leader", zap.String("topic", req.Topic))
		if err := tm.CreateTopic(req.Topic, int(req.ReplicaCount)); err != nil {
			return nil, ErrCreateTopic(err)
		}
		return &protocol.CreateTopicResponse{Topic: req.Topic}, nil
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

	tm.Logger.Info("forwarding create topic to topic leader", zap.String("topic", req.Topic), zap.String("topic_leader_id", topicLeaderID))

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
	ev := &protocol.MetadataEvent{
		CreateTopicEvent: &protocol.CreateTopicEvent{
			Topic:        req.Topic,
			ReplicaCount: req.ReplicaCount,
			LeaderNodeID: topicLeaderID,
			LeaderEpoch:  1,
		},
	}
	if err := n.ApplyCreateTopicEvent(ev); err != nil {
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
	for _, replica := range topicObj.replicas {
		if replica.client != nil {
			_, _ = replica.client.DeleteReplica(ctx, &protocol.DeleteReplicaRequest{
				Topic: topic,
			})
		}
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
			ev := &protocol.MetadataEvent{
				DeleteTopicEvent: &protocol.DeleteTopicEvent{Topic: req.Topic},
			}
			if err := tm.node.ApplyDeleteTopicEvent(ev); err != nil {
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
		Name:     topic,
		Log:      logManager,
		Logger:   tm.Logger,
		nodeID:   tm.currentNodeID(),
		replicas: make(map[string]*ReplicaInfo),
	}
	tm.topics[topic] = topicObj

	replicaNodeIDs := tm.node.GetTopicReplicaNodeIDs(topic)
	for _, nodeID := range replicaNodeIDs {
		if nodeID == tm.currentNodeID() {
			continue
		}
		rpcAddr, err := tm.node.GetRpcAddrForNodeID(nodeID)
		if err != nil {
			tm.Logger.Debug("restore: skip replica (no rpc addr)", zap.String("topic", topic), zap.String("node_id", nodeID))
			continue
		}
		replClient, err := client.NewRemoteClient(rpcAddr)
		if err != nil {
			tm.Logger.Debug("restore: skip replica (client failed)", zap.String("topic", topic), zap.String("node_id", nodeID), zap.Error(err))
			continue
		}
		leo, isISR := tm.node.GetTopicReplicaState(topic, nodeID)
		topicObj.replicas[nodeID] = &ReplicaInfo{
			NodeID: nodeID,
			State: ReplicaState{
				LastFetchTime: time.Now(),
				LEO:           leo,
				IsISR:         isISR,
			},
			client: replClient,
		}
	}

	tm.Logger.Info("restored leader topic from metadata", zap.String("topic", topic), zap.Int("replicas", len(topicObj.replicas)))
	return nil
}

// CreateReplicaRemote creates a replica for the topic on this node (called by leader via RPC).
func (tm *TopicManager) CreateReplicaRemote(topic string, leaderAddr string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	topicObj, ok := tm.topics[topic]
	if !ok {
		topicObj = &Topic{
			Name:     topic,
			Logger:   tm.Logger,
			replicas: nil,
		}
		tm.topics[topic] = topicObj
	}

	if topicObj.Log != nil {
		return ErrTopicAlreadyReplicaf(topic)
	}

	topicObj.nodeID = tm.node.GetNodeID()
	logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topic))
	if err != nil {
		return ErrCreateLogReplica(err)
	}

	streamClient, err := client.NewReplicationStreamClient(leaderAddr)
	if err != nil {
		return ErrCreateStreamClient(err)
	}
	rpcClient, err := client.NewRemoteClient(leaderAddr)
	if err != nil {
		return ErrCreateRPCClient(err)
	}

	topicObj.Log = logManager
	topicObj.streamClient = streamClient
	topicObj.rpcClient = rpcClient

	tm.Logger.Info("replica created for topic", zap.String("topic", topic), zap.String("leader_addr", leaderAddr))

	if err := topicObj.StartReplication(); err != nil {
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

	topicObj.StopReplication()
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
func (t *Topic) StartReplication() error {

	if t.streamClient == nil {
		return ErrStreamClientNotSetf(t.Name)
	}

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
	go t.runReplication(ctx)
	return nil
}

// StopReplication stops the replication loop.
func (t *Topic) StopReplication() {
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

func (t *Topic) runReplication(ctx context.Context) {
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
		if err := t.streamClient.ReplicateStream(ctx, req); err != nil {
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

func (t *Topic) replicateStream(ctx context.Context) {
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

		resp, err := t.streamClient.Recv()
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

func (t *Topic) reportLEO(ctx context.Context, leo uint64) error {
	if t.nodeID == "" {
		return nil // not a replica or node ID not set, skip reporting
	}
	req := &protocol.RecordLEORequest{
		NodeID: t.nodeID,
		Topic:  t.Name,
		Leo:    int64(leo),
	}
	_, err := t.rpcClient.RecordLEO(ctx, req)
	return err
}

// HandleProduce appends to the topic log (leader only). For ACK_ALL, waits for replicas to catch up.
func (t *Topic) HandleProduce(ctx context.Context, logEntry *protocol.LogEntry, acks protocol.AckMode) (uint64, error) {
	offset, err := t.Log.Append(logEntry.Value)
	if err != nil {
		return 0, err
	}
	switch acks {
	case protocol.AckLeader:
		return offset, nil
	case protocol.AckAll:
		if err := t.waitForAllFollowersToCatchUp(ctx, offset); err != nil {
			return 0, ErrWaitFollowersCatchUp(err)
		}
		return offset, nil
	default:
		return 0, ErrInvalidAckModef(int32(acks))
	}
}

// HandleProduceBatch appends multiple records (leader only).
func (t *Topic) HandleProduceBatch(ctx context.Context, values [][]byte, acks protocol.AckMode) (uint64, uint64, error) {
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
		if err := t.waitForAllFollowersToCatchUp(ctx, last); err != nil {
			return 0, 0, ErrWaitFollowersCatchUp(err)
		}
		return base, last, nil
	default:
		return 0, 0, ErrInvalidAckModef(int32(acks))
	}
}

func (t *Topic) waitForAllFollowersToCatchUp(ctx context.Context, offset uint64) error {
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	requiredLEO := offset + 1

	for {
		allCaughtUp := true
		candidates := 0

		t.mu.RLock()
		useISR := false
		for _, r := range t.replicas {
			if r.State.IsISR {
				useISR = true
				break
			}
		}
		for _, replica := range t.replicas {
			if useISR && !replica.State.IsISR {
				continue
			}
			candidates++
			if replica.State.LEO < requiredLEO {
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

func (t *Topic) maybeAdvanceHW() {
	t.mu.RLock()
	if t.Log == nil {
		t.mu.RUnlock()
		return
	}
	minOffset := t.Log.LEO()
	for _, r := range t.replicas {
		if r.State.LEO < minOffset {
			minOffset = r.State.LEO
		}
	}
	t.mu.RUnlock()
	t.Log.SetHighWatermark(minOffset)
}

// RecordLEORemote records the LEO of a replica (leader only). Called by RPC when a replica reports its LEO.
func (t *TopicManager) RecordLEORemote(nodeId string, topic string, leo uint64, leoTime time.Time) error {
	if nodeId == "" {
		return ErrNodeIDRequired
	}
	t.mu.RLock()
	topicObj, ok := t.topics[topic]
	if !ok {
		t.mu.RUnlock()
		return ErrTopicNotFoundf(topic)
	}
	t.mu.RUnlock()
	topicObj.mu.Lock()
	repl, ok := topicObj.replicas[nodeId]
	if !ok || repl == nil {
		topicObj.mu.Unlock()
		return ErrReplicaNotFoundf(nodeId, topic)
	}
	repl.State.LEO = leo
	repl.State.LastFetchTime = leoTime
	if topicObj.Log.LEO() >= 100 && leo >= topicObj.Log.LEO()-100 {
		repl.State.IsISR = true
	} else if topicObj.Log.LEO() < 100 {
		if leo >= topicObj.Log.LEO() {
			repl.State.IsISR = true
		}
	} else {
		repl.State.IsISR = false
	}
	topicObj.mu.Unlock()
	topicObj.maybeAdvanceHW()
	t.node.ApplyIsrUpdateEvent(&protocol.MetadataEvent{IsrUpdateEvent: &protocol.IsrUpdateEvent{
		Topic:         topic,
		ReplicaNodeID: nodeId,
		Isr:           repl.State.IsISR,
	}})
	return nil
}
