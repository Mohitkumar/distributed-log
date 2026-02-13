package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/common"
	"github.com/mohitkumar/mlog/protocol"
	"github.com/mohitkumar/mlog/topic"
)

var _ topic.TopicCoordinator = (*FakeTopicCoordinator)(nil)

// FakeTopicCoordinator implements topic.TopicCoordinator in memory without Raft.
// Use for unit testing TopicManager in isolation.
type FakeTopicCoordinator struct {
	mu sync.RWMutex

	NodeID       string
	RPCAddr      string
	IsRaftLeader bool

	Nodes    map[string]*common.Node                    // nodeID -> node
	Topics   map[string]*fakeTopicMeta                  // topic -> meta
	Replicas map[string]map[string]*common.ReplicaState // topic -> replicaNodeID -> state

	replicationTarget *topic.TopicManager
	stopReplication   chan struct{}
}

type fakeTopicMeta struct {
	LeaderNodeID string
	ReplicaIDs   []string
}

// NewFakeTopicCoordinator returns a TopicCoordinator that behaves as a single node (leader).
func NewFakeTopicCoordinator(nodeID, rpcAddr string) *FakeTopicCoordinator {
	f := &FakeTopicCoordinator{
		NodeID:       nodeID,
		RPCAddr:      rpcAddr,
		IsRaftLeader: true,
		Nodes:        make(map[string]*common.Node),
		Topics:       make(map[string]*fakeTopicMeta),
		Replicas:     make(map[string]map[string]*common.ReplicaState),
	}
	f.Nodes[nodeID] = &common.Node{NodeID: nodeID, RPCAddr: rpcAddr}
	return f
}

var _ topic.TopicCoordinator = (*FakeTopicCoordinator)(nil)

func (f *FakeTopicCoordinator) GetCurrentNode() *common.Node {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.Nodes[f.NodeID]
}

func (f *FakeTopicCoordinator) GetNode(nodeID string) (*common.Node, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	n, ok := f.Nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}
	return n, nil
}

func (f *FakeTopicCoordinator) GetOtherNodes() []*common.Node {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]*common.Node, 0, len(f.Nodes))
	for id, n := range f.Nodes {
		if id != f.NodeID {
			out = append(out, n)
		}
	}
	return out
}

func (f *FakeTopicCoordinator) GetRpcClient(nodeID string) (*client.RemoteClient, error) {
	f.mu.RLock()
	n, ok := f.Nodes[nodeID]
	f.mu.RUnlock()
	if !ok || n == nil {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}
	return client.NewRemoteClient(n.RPCAddr)
}

func (f *FakeTopicCoordinator) GetConsumerClient(nodeID string) (*client.ConsumerClient, error) {
	f.mu.RLock()
	n, ok := f.Nodes[nodeID]
	f.mu.RUnlock()
	if !ok || n == nil {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}
	return client.NewConsumerClient(n.RPCAddr)
}

func (f *FakeTopicCoordinator) IsLeader() bool {
	return f.IsRaftLeader
}

func (f *FakeTopicCoordinator) GetRaftLeaderRemoteClient() (*client.RemoteClient, error) {
	if f.IsRaftLeader {
		return f.GetRpcClient(f.NodeID)
	}
	return nil, fmt.Errorf("no raft leader")
}

func (f *FakeTopicCoordinator) TopicExists(topicName string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, ok := f.Topics[topicName]
	return ok
}

func (f *FakeTopicCoordinator) ListTopicNames() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	names := make([]string, 0, len(f.Topics))
	for name := range f.Topics {
		names = append(names, name)
	}
	return names
}

func (f *FakeTopicCoordinator) GetTopicLeaderNode(topicName string) (*common.Node, error) {
	f.mu.RLock()
	meta, ok := f.Topics[topicName]
	f.mu.RUnlock()
	if !ok || meta == nil {
		return nil, fmt.Errorf("topic %s not found", topicName)
	}
	return f.GetNode(meta.LeaderNodeID)
}

func (f *FakeTopicCoordinator) GetTopicReplicaNodes(topicName string) ([]*common.Node, error) {
	f.mu.RLock()
	meta, ok := f.Topics[topicName]
	f.mu.RUnlock()
	if !ok || meta == nil {
		return nil, fmt.Errorf("topic %s not found", topicName)
	}
	out := make([]*common.Node, 0, len(meta.ReplicaIDs))
	for _, id := range meta.ReplicaIDs {
		n, err := f.GetNode(id)
		if err != nil {
			continue
		}
		out = append(out, n)
	}
	return out, nil
}

func (f *FakeTopicCoordinator) GetTopicReplicaStates(topicName string) map[string]*common.ReplicaState {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.Replicas[topicName]
}

func (f *FakeTopicCoordinator) UpdateTopicReplicaLEO(topicName, replicaNodeID string, leo int64, isr bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Replicas[topicName] == nil {
		f.Replicas[topicName] = make(map[string]*common.ReplicaState)
	}
	rs := f.Replicas[topicName][replicaNodeID]
	if rs == nil {
		f.Replicas[topicName][replicaNodeID] = &common.ReplicaState{
			ReplicaNodeID: replicaNodeID,
			LEO:           leo,
			IsISR:         isr,
		}
		return
	}
	rs.LEO = leo
	rs.IsISR = isr
}

// UpdateTopicReplicaISR updates only the Isr bit for a replica (used when applying ISR events that no longer carry LEO).
func (f *FakeTopicCoordinator) UpdateTopicReplicaISR(topicName, replicaNodeID string, isr bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Replicas[topicName] == nil {
		f.Replicas[topicName] = make(map[string]*common.ReplicaState)
	}
	rs := f.Replicas[topicName][replicaNodeID]
	if rs == nil {
		f.Replicas[topicName][replicaNodeID] = &common.ReplicaState{
			ReplicaNodeID: replicaNodeID,
			LEO:           0,
			IsISR:         isr,
		}
		return
	}
	rs.IsISR = isr
}

func (f *FakeTopicCoordinator) GetNodeIDWithLeastTopics() (*common.Node, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if len(f.Nodes) == 0 {
		return nil, fmt.Errorf("no nodes in cluster")
	}
	// Single node: return self
	return f.Nodes[f.NodeID], nil
}

func (f *FakeTopicCoordinator) applyCreateTopicEvent(topicName string, replicaCount uint32, leaderNodeID string, replicaNodeIds []string) error {
	// For tests, when replicaCount == 0 (no replicas), treat the local node as the topic leader
	// so that single-node producer tests can always talk to the local node as leader.
	if replicaCount == 0 {
		leaderNodeID = f.NodeID
	}
	f.Topics[topicName] = &fakeTopicMeta{
		LeaderNodeID: leaderNodeID,
		ReplicaIDs:   replicaNodeIds,
	}
	f.Replicas[topicName] = make(map[string]*common.ReplicaState)
	for _, id := range replicaNodeIds {
		f.Replicas[topicName][id] = &common.ReplicaState{
			ReplicaNodeID: id,
			LEO:           0,
			IsISR:         true,
		}
	}
	// Push event to TopicManager (metadata store) so it creates the topic locally, mirroring Raft FSM Apply.
	if f.replicationTarget != nil {
		eventData, _ := json.Marshal(protocol.CreateTopicEvent{
			Topic:          topicName,
			ReplicaCount:   replicaCount,
			LeaderNodeID:   leaderNodeID,
			LeaderEpoch:    1,
			ReplicaNodeIds: replicaNodeIds,
		})
		ev := &protocol.MetadataEvent{EventType: protocol.MetadataEventTypeCreateTopic, Data: eventData}
		_ = f.replicationTarget.Apply(ev)
	}
	return nil
}

func (f *FakeTopicCoordinator) applyDeleteTopicEvent(topicName string) error {
	delete(f.Topics, topicName)
	delete(f.Replicas, topicName)
	// Push event to TopicManager so it deletes the topic locally, mirroring Raft FSM Apply.
	if f.replicationTarget != nil {
		eventData, _ := json.Marshal(protocol.DeleteTopicEvent{Topic: topicName})
		ev := &protocol.MetadataEvent{EventType: protocol.MetadataEventTypeDeleteTopic, Data: eventData}
		_ = f.replicationTarget.Apply(ev)
	}
	return nil
}

func (f *FakeTopicCoordinator) applyIsrUpdateEvent(topicName, replicaNodeID string, isr bool) error {
	f.UpdateTopicReplicaISR(topicName, replicaNodeID, isr)
	if f.replicationTarget != nil {
		eventData, _ := json.Marshal(protocol.IsrUpdateEvent{Topic: topicName, ReplicaNodeID: replicaNodeID, Isr: isr})
		ev := &protocol.MetadataEvent{EventType: protocol.MetadataEventTypeIsrUpdate, Data: eventData}
		_ = f.replicationTarget.Apply(ev)
	}
	return nil
}

// TopicCoordinator interface: apply events (update in-memory state for tests).

func (f *FakeTopicCoordinator) ApplyCreateTopicEvent(topicName string, replicaCount uint32, leaderNodeID string, replicaNodeIds []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applyCreateTopicEvent(topicName, replicaCount, leaderNodeID, replicaNodeIds)
}

func (f *FakeTopicCoordinator) ApplyDeleteTopicEventInternal(topicName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applyDeleteTopicEvent(topicName)
}

func (f *FakeTopicCoordinator) ApplyIsrUpdateEventInternal(topicName, replicaNodeID string, isr bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applyIsrUpdateEvent(topicName, replicaNodeID, isr)
}

func (f *FakeTopicCoordinator) ApplyLeaderChangeEvent(topicName, leaderNodeID string, leaderEpoch int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if meta := f.Topics[topicName]; meta != nil {
		meta.LeaderNodeID = leaderNodeID
	}
	return nil
}

func (f *FakeTopicCoordinator) GetRaftLeaderNodeID() (string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.IsRaftLeader {
		return f.NodeID, nil
	}
	return "", fmt.Errorf("not raft leader")
}

// AddNode adds a node to the fake cluster (for multi-node tests without Raft).
func (f *FakeTopicCoordinator) AddNode(nodeID, rpcAddr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Nodes[nodeID] = &common.Node{NodeID: nodeID, RPCAddr: rpcAddr}
}

func (f *FakeTopicCoordinator) SetReplicationTarget(t *topic.TopicManager) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replicationTarget = t
}

func (f *FakeTopicCoordinator) StartReplicationThread() {
	f.mu.Lock()
	if f.stopReplication != nil {
		f.mu.Unlock()
		return
	}
	f.stopReplication = make(chan struct{})
	f.mu.Unlock()
	go f.runReplicationThread()
}

func (f *FakeTopicCoordinator) StopReplicationThread() {
	f.mu.Lock()
	ch := f.stopReplication
	f.stopReplication = nil
	f.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

func (f *FakeTopicCoordinator) runReplicationThread() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-f.stopReplication:
			return
		case <-ticker.C:
			f.replicateAllTopics()
		}
	}
}

func (f *FakeTopicCoordinator) replicateAllTopics() {
	f.mu.RLock()
	target := f.replicationTarget
	f.mu.RUnlock()
	if target == nil {
		return
	}
	leaderToTopics := make(map[string][]string)
	for _, info := range target.ListReplicaTopics() {
		leaderToTopics[info.LeaderNodeID] = append(leaderToTopics[info.LeaderNodeID], info.TopicName)
	}
	ctx := context.Background()
	for leaderID, topicNames := range leaderToTopics {
		_ = topic.DoReplicateTopicsForLeader(ctx, target, f.NodeID, leaderID, topicNames, 5000)
	}
}

func (f *FakeTopicCoordinator) ApplyEvent(ev topic.ApplyEvent) {
	switch ev.Type {
	case topic.ApplyEventCreateTopic:
		if ev.CreateTopic != nil {
			f.mu.Lock()
			defer f.mu.Unlock()
			_ = f.applyCreateTopicEvent(ev.CreateTopic.Topic, ev.CreateTopic.ReplicaCount, ev.CreateTopic.LeaderNodeID, ev.CreateTopic.ReplicaNodeIds)
		}
	case topic.ApplyEventDeleteTopic:
		if ev.DeleteTopic != nil {
			f.mu.Lock()
			defer f.mu.Unlock()
			_ = f.applyDeleteTopicEvent(ev.DeleteTopic.Topic)
		}
	case topic.ApplyEventIsrUpdate:
		if ev.IsrUpdate != nil {
			_ = f.applyIsrUpdateEvent(ev.IsrUpdate.Topic, ev.IsrUpdate.ReplicaNodeID, ev.IsrUpdate.Isr)
		}
	}
}
