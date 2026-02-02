package topic

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/common"
	"github.com/mohitkumar/mlog/log"
	"github.com/mohitkumar/mlog/protocol"
)

// Topic represents a topic on this node: either we are the leader (with log and replica state)
// or we host a replica (with log and client to leader for replication).
type Topic struct {
	mu   sync.RWMutex
	Name string
	Log  *log.LogManager

	// Leader: this node is the leader for this topic
	isLeader     bool
	leaderNodeID string // when leader, equals current node; when replica, leader's node ID
	replicas     map[string]*ReplicaInfo

	// Replica: this node hosts a replica for this topic
	replicaID         string
	leaderAddr        string
	streamClient      *client.ReplicationStreamClient // replication stream only (send request, Recv until EndOfStream)
	rpcClient         *client.RemoteClient            // RecordLEO and other request-response RPCs to leader
	stopChan          chan struct{}
	stopOnce          sync.Once
	replicationCancel context.CancelFunc
}

// ReplicaInfo holds metadata and client for a remote replica (leader's view).
type ReplicaInfo struct {
	NodeID    string
	Addr      string
	ReplicaID string
	State     ReplicaState
	client    *client.RemoteClient
}

// ReplicaState tracks a replica's progress (LEO, ISR, etc.).
type ReplicaState struct {
	ReplicaID     string
	LastFetchTime time.Time
	LEO           uint64
	IsISR         bool
}

// LeaderView is returned by GetLeader for read/replicate access to the leader log.
type LeaderView struct {
	Log *log.LogManager
}

// ReplicaView is returned by GetReplica for access to a replica's log.
type ReplicaView struct {
	Log       *log.LogManager
	ReplicaID string
}

// TopicManager manages topics on this node.
type TopicManager struct {
	mu              sync.RWMutex
	topics          map[string]*Topic
	BaseDir         string
	CurrentNodeID   string
	CurrentNodeAddr string
	GetOtherNodes   func() []common.NodeInfo
}

// NewTopicManager creates a topic manager. GetOtherNodes returns other nodes in the cluster (used when creating topics with replicas).
func NewTopicManager(baseDir string, currentNodeID string, currentNodeAddr string, getOtherNodes func() []common.NodeInfo) (*TopicManager, error) {
	return &TopicManager{
		topics:          make(map[string]*Topic),
		BaseDir:         baseDir,
		CurrentNodeID:   currentNodeID,
		CurrentNodeAddr: currentNodeAddr,
		GetOtherNodes:   getOtherNodes,
	}, nil
}

// CreateTopic creates a new topic with this node as leader and optional replicas on other nodes.
func (tm *TopicManager) CreateTopic(topic string, replicaCount int) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.topics[topic]; exists {
		return fmt.Errorf("topic %s already exists", topic)
	}

	logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topic))
	if err != nil {
		return fmt.Errorf("failed to create log manager: %w", err)
	}

	topicObj := &Topic{
		Name:         topic,
		Log:          logManager,
		isLeader:     true,
		leaderNodeID: tm.CurrentNodeID,
		replicas:     make(map[string]*ReplicaInfo),
	}
	tm.topics[topic] = topicObj

	otherNodes := tm.GetOtherNodes()
	if len(otherNodes) < replicaCount {
		delete(tm.topics, topic)
		logManager.Close()
		return fmt.Errorf("not enough nodes: need %d, have %d", replicaCount, len(otherNodes))
	}

	for i := 0; i < replicaCount; i++ {
		node := otherNodes[i]
		replicaID := fmt.Sprintf("replica-%d", i)

		replClient, err := client.NewRemoteClient(node.RpcAddr)
		if err != nil {
			delete(tm.topics, topic)
			logManager.Close()
			return fmt.Errorf("failed to create replication client to %s: %w", node.RpcAddr, err)
		}

		_, err = replClient.CreateReplica(context.Background(), &protocol.CreateReplicaRequest{
			Topic:      topic,
			ReplicaId:  replicaID,
			LeaderAddr: tm.CurrentNodeAddr,
		})
		if err != nil {
			delete(tm.topics, topic)
			logManager.Close()
			return fmt.Errorf("failed to create replica %s on %s: %w", replicaID, node.NodeID, err)
		}

		topicObj.replicas[replicaID] = &ReplicaInfo{
			NodeID:    node.NodeID,
			Addr:      node.RpcAddr,
			ReplicaID: replicaID,
			State: ReplicaState{
				ReplicaID:     replicaID,
				LastFetchTime: time.Now(),
				LEO:           0,
				IsISR:         false,
			},
			client: replClient,
		}
	}

	return nil
}

// DeleteTopic deletes a topic (caller must be leader). Closes and deletes leader log; deletes replicas on remote nodes.
func (tm *TopicManager) DeleteTopic(topic string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	topicObj, ok := tm.topics[topic]
	if !ok {
		return fmt.Errorf("topic %s not found", topic)
	}

	if !topicObj.isLeader {
		return fmt.Errorf("topic %s: only leader can delete topic", topic)
	}

	if topicObj.Log != nil {
		topicObj.Log.Close()
		topicObj.Log.Delete()
	}

	ctx := context.Background()
	for _, replica := range topicObj.replicas {
		if replica.client != nil {
			_, _ = replica.client.DeleteReplica(ctx, &protocol.DeleteReplicaRequest{
				Topic:     topic,
				ReplicaId: replica.ReplicaID,
			})
		}
	}

	delete(tm.topics, topic)
	return nil
}

// GetLeader returns the leader log view for a topic (this node must be the leader).
func (tm *TopicManager) GetLeader(topic string) (*LeaderView, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	topicObj, ok := tm.topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic %s not found", topic)
	}
	if !topicObj.isLeader {
		return nil, fmt.Errorf("topic %s has no leader on this node", topic)
	}
	return &LeaderView{Log: topicObj.Log}, nil
}

// GetReplica returns a replica's log view (this node must host that replica).
func (tm *TopicManager) GetReplica(topic string, replicaID string) (*ReplicaView, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	topicObj, ok := tm.topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic %s not found", topic)
	}
	if topicObj.isLeader {
		return nil, fmt.Errorf("topic %s: this node is leader, replica %s is remote", topic, replicaID)
	}
	if topicObj.replicaID != replicaID {
		return nil, fmt.Errorf("replica %s not found for topic %s", replicaID, topic)
	}
	return &ReplicaView{Log: topicObj.Log, ReplicaID: topicObj.replicaID}, nil
}

// GetTopic returns the topic object.
func (tm *TopicManager) GetTopic(topic string) (*Topic, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	topicObj, ok := tm.topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic %s not found", topic)
	}
	return topicObj, nil
}

// CreateReplicaRemote creates a replica for the topic on this node (called by leader via RPC).
func (tm *TopicManager) CreateReplicaRemote(topic string, replicaID string, leaderAddr string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	topicObj, ok := tm.topics[topic]
	if !ok {
		topicObj = &Topic{
			Name:     topic,
			isLeader: false,
			replicas: nil,
		}
		tm.topics[topic] = topicObj
	}

	if topicObj.Log != nil {
		return fmt.Errorf("topic %s already has replica %s", topic, replicaID)
	}

	logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topic, replicaID))
	if err != nil {
		return fmt.Errorf("failed to create log manager for replica: %w", err)
	}

	streamClient, err := client.NewReplicationStreamClient(leaderAddr)
	if err != nil {
		return fmt.Errorf("failed to create replication stream client to leader: %w", err)
	}
	rpcClient, err := client.NewRemoteClient(leaderAddr)
	if err != nil {
		return fmt.Errorf("failed to create replication RPC client to leader: %w", err)
	}

	topicObj.Log = logManager
	topicObj.replicaID = replicaID
	topicObj.leaderAddr = leaderAddr
	topicObj.streamClient = streamClient
	topicObj.rpcClient = rpcClient

	if err := topicObj.StartReplication(); err != nil {
		return fmt.Errorf("failed to start replication for replica %s: %w", replicaID, err)
	}

	return nil
}

// DeleteReplicaRemote deletes a replica for the topic on this node.
func (tm *TopicManager) DeleteReplicaRemote(topic string, replicaID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	topicObj, ok := tm.topics[topic]
	if !ok {
		return fmt.Errorf("topic %s not found", topic)
	}
	if topicObj.replicaID != replicaID {
		return fmt.Errorf("replica %s not found", replicaID)
	}

	topicObj.StopReplication()
	if topicObj.Log != nil {
		topicObj.Log.Delete()
		topicObj.Log = nil
	}

	delete(tm.topics, topic)

	topicDir := filepath.Join(tm.BaseDir, topic)
	if err := os.RemoveAll(topicDir); err != nil {
		return fmt.Errorf("failed to remove topic directory: %w", err)
	}
	return nil
}

// StartReplication starts the replication loop (topic must be a replica). Fetches from leader and appends to log; reports LEO.
func (t *Topic) StartReplication() error {
	if t.isLeader {
		return fmt.Errorf("cannot start replication on leader topic")
	}
	if t.leaderAddr == "" || t.streamClient == nil {
		return fmt.Errorf("leader address or stream client not set for replica %s", t.replicaID)
	}

	t.mu.Lock()
	if t.stopChan != nil {
		t.mu.Unlock()
		return fmt.Errorf("replication already started for replica %s", t.replicaID)
	}
	t.stopChan = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	t.replicationCancel = cancel
	t.mu.Unlock()

	go t.runReplication(ctx)
	return nil
}

// StopReplication stops the replication loop.
func (t *Topic) StopReplication() {
	t.stopOnce.Do(func() {
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
			fmt.Println("Error in replicate stream, reconnecting", err)
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
	req := &protocol.RecordLEORequest{
		Topic:     t.Name,
		ReplicaId: t.replicaID,
		Leo:       int64(leo),
	}
	_, err := t.rpcClient.RecordLEO(ctx, req)
	return err
}

// HandleProduce appends to the topic log (leader only). For ACK_ALL, waits for replicas to catch up.
func (t *Topic) HandleProduce(ctx context.Context, logEntry *protocol.LogEntry, acks protocol.AckMode) (uint64, error) {
	t.mu.RLock()
	if !t.isLeader {
		t.mu.RUnlock()
		return 0, fmt.Errorf("topic %s has no leader on this node", t.Name)
	}
	t.mu.RUnlock()

	offset, err := t.Log.Append(logEntry.Value)
	if err != nil {
		return 0, err
	}
	switch acks {
	case protocol.AckLeader:
		return offset, nil
	case protocol.AckAll:
		if err := t.waitForAllFollowersToCatchUp(ctx, offset); err != nil {
			return 0, fmt.Errorf("failed to wait for all followers to catch up: %w", err)
		}
		return offset, nil
	default:
		return 0, fmt.Errorf("invalid ack mode: %d", int32(acks))
	}
}

// HandleProduceBatch appends multiple records (leader only).
func (t *Topic) HandleProduceBatch(ctx context.Context, values [][]byte, acks protocol.AckMode) (uint64, uint64, error) {
	t.mu.RLock()
	if !t.isLeader {
		t.mu.RUnlock()
		return 0, 0, fmt.Errorf("topic %s has no leader on this node", t.Name)
	}
	t.mu.RUnlock()

	if len(values) == 0 {
		return 0, 0, fmt.Errorf("values cannot be empty")
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
			return 0, 0, fmt.Errorf("failed to wait for all followers to catch up: %w", err)
		}
		return base, last, nil
	default:
		return 0, 0, fmt.Errorf("invalid ack mode: %d", int32(acks))
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
			return fmt.Errorf("timeout before all followers caught up")
		}
	}
}

func (t *Topic) maybeAdvanceHW() {
	t.mu.RLock()
	if !t.isLeader || t.Log == nil {
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
func (t *Topic) RecordLEORemote(replicaID string, leo uint64, leoTime time.Time) error {
	t.mu.RLock()
	if !t.isLeader {
		t.mu.RUnlock()
		return fmt.Errorf("topic %s has no leader on this node", t.Name)
	}
	replica, ok := t.replicas[replicaID]
	t.mu.RUnlock()
	if !ok {
		return fmt.Errorf("replica %s not found", replicaID)
	}

	t.mu.Lock()
	replica.State.LEO = leo
	replica.State.LastFetchTime = leoTime
	if t.Log.LEO() >= 100 && leo >= t.Log.LEO()-100 {
		replica.State.IsISR = true
	} else if t.Log.LEO() < 100 {
		if leo >= t.Log.LEO() {
			replica.State.IsISR = true
		}
	}
	t.mu.Unlock()
	t.maybeAdvanceHW()
	return nil
}
