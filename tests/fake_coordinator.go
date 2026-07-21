package tests

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mohitkumar/mlog/broker/cluster"
	raft "github.com/mohitkumar/mlog/broker/cluster/raft"
	"github.com/mohitkumar/mlog/broker/topic"
)

var _ topic.TopicCoordinator = (*FakeTopicCoordinator)(nil)

// FakeNodeInfo is a minimal node record for FakeTopicCoordinator (no Raft): just enough
// to answer AliveNodeIDs/NodeRPCAddr/IsNodeAlive, which in production come from Raft's
// voter configuration reconciled with Serf gossip (see cluster.Cluster).
type FakeNodeInfo struct {
	NodeID  string
	RpcAddr string
}

// FakeTopicCoordinator implements topic.TopicCoordinator in memory (no Raft).
// Mimics Cluster: Apply* methods update in-memory state and, when
// SetReplicationTarget is set, push the same events to the TopicManager (like FSM Apply).
type FakeTopicCoordinator struct {
	mu sync.RWMutex

	NodeID       string
	RPCAddr      string
	IsRaftLeader bool

	Nodes    map[string]*FakeNodeInfo                    // nodeID -> node (fake's own membership view)
	Topics   map[string]*fakeTopicMeta                   // topic -> meta
	Replicas map[string]map[string]*cluster.ReplicaState // topic -> replicaNodeID -> state

	replicationTarget *topic.TopicManager
	stopReplication   chan struct{}
}

type fakeTopicMeta struct {
	LeaderNodeID string
	ReplicaIDs   []string
}

func NewFakeTopicCoordinator(nodeID, rpcAddr string) *FakeTopicCoordinator {
	f := &FakeTopicCoordinator{
		NodeID:       nodeID,
		RPCAddr:      rpcAddr,
		IsRaftLeader: true,
		Nodes:        make(map[string]*FakeNodeInfo),
		Topics:       make(map[string]*fakeTopicMeta),
		Replicas:     make(map[string]map[string]*cluster.ReplicaState),
	}
	f.Nodes[nodeID] = &FakeNodeInfo{NodeID: nodeID, RpcAddr: rpcAddr}
	return f
}

// — TopicCoordinator interface —

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
	if f.replicationTarget != nil {
		eventData, _ := raft.EncodeLeaderChangeEvent(raft.LeaderChangeEvent{
			Topic: topicName, LeaderNodeID: leaderNodeID, LeaderEpoch: leaderEpoch,
		})
		_ = f.replicationTarget.Apply(&raft.MetadataEvent{EventType: raft.MetadataEventTypeLeaderChange, Data: eventData})
	}
	return nil
}

func (f *FakeTopicCoordinator) IsLeader() bool {
	return f.IsRaftLeader
}

func (f *FakeTopicCoordinator) GetRaftLeaderNodeID() (string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.IsRaftLeader {
		return f.NodeID, nil
	}
	return "", fmt.Errorf("not raft leader")
}

// AliveNodeIDs returns all known node IDs (fakes don't model liveness beyond "known").
func (f *FakeTopicCoordinator) AliveNodeIDs() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	ids := make([]string, 0, len(f.Nodes))
	for id := range f.Nodes {
		ids = append(ids, id)
	}
	return ids
}

// NodeRPCAddr returns the RPC address for nodeID, if known.
func (f *FakeTopicCoordinator) NodeRPCAddr(nodeID string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	n := f.Nodes[nodeID]
	if n == nil {
		return "", false
	}
	return n.RpcAddr, true
}

// IsNodeAlive reports whether nodeID is known to this fake.
func (f *FakeTopicCoordinator) IsNodeAlive(nodeID string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.Nodes[nodeID] != nil
}

// — Helpers for tests (testutil and node/topic/replica tests) —

func (f *FakeTopicCoordinator) AddNode(nodeID, rpcAddr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Nodes[nodeID] = &FakeNodeInfo{NodeID: nodeID, RpcAddr: rpcAddr}
}

func (f *FakeTopicCoordinator) SetReplicationTarget(tm *topic.TopicManager) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replicationTarget = tm
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

func (f *FakeTopicCoordinator) GetOtherNodes() []*FakeNodeInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]*FakeNodeInfo, 0, len(f.Nodes))
	for id, n := range f.Nodes {
		if id != f.NodeID && n != nil {
			out = append(out, n)
		}
	}
	return out
}

// ApplyEvent applies a single metadata event (for tests that drive create/delete/isr without going through TopicManager).
func (f *FakeTopicCoordinator) ApplyEvent(ev topic.ApplyEvent) {
	switch ev.Type {
	case topic.ApplyEventCreateTopic:
		if ev.CreateTopic != nil {
			f.mu.Lock()
			_ = f.applyCreateTopicEvent(ev.CreateTopic.Topic, ev.CreateTopic.ReplicaCount, ev.CreateTopic.LeaderNodeID, ev.CreateTopic.ReplicaNodeIds)
			f.mu.Unlock()
		}
	case topic.ApplyEventDeleteTopic:
		if ev.DeleteTopic != nil {
			f.mu.Lock()
			_ = f.applyDeleteTopicEvent(ev.DeleteTopic.Topic)
			f.mu.Unlock()
		}
	case topic.ApplyEventIsrUpdate:
		if ev.IsrUpdate != nil {
			f.mu.Lock()
			_ = f.applyIsrUpdateEvent(ev.IsrUpdate.Topic, ev.IsrUpdate.ReplicaNodeID, ev.IsrUpdate.Isr)
			f.mu.Unlock()
		}
	}
}

// — internal apply helpers (mirror Raft FSM: update in-memory + push to replicationTarget) —

func (f *FakeTopicCoordinator) applyCreateTopicEvent(topicName string, replicaCount uint32, leaderNodeID string, replicaNodeIds []string) error {
	if replicaCount == 0 {
		leaderNodeID = f.NodeID
	}
	f.Topics[topicName] = &fakeTopicMeta{LeaderNodeID: leaderNodeID, ReplicaIDs: replicaNodeIds}
	f.Replicas[topicName] = make(map[string]*cluster.ReplicaState)
	for _, id := range replicaNodeIds {
		f.Replicas[topicName][id] = &cluster.ReplicaState{ReplicaNodeID: id, LEO: 0, IsISR: true}
	}
	if f.replicationTarget != nil {
		eventData, _ := raft.EncodeCreateTopicEvent(raft.CreateTopicEvent{
			Topic: topicName, ReplicaCount: replicaCount, LeaderNodeID: leaderNodeID, LeaderEpoch: 1, ReplicaNodeIds: replicaNodeIds,
		})
		_ = f.replicationTarget.Apply(&raft.MetadataEvent{EventType: raft.MetadataEventTypeCreateTopic, Data: eventData})
	}
	return nil
}

func (f *FakeTopicCoordinator) applyDeleteTopicEvent(topicName string) error {
	delete(f.Topics, topicName)
	delete(f.Replicas, topicName)
	if f.replicationTarget != nil {
		eventData, _ := raft.EncodeDeleteTopicEvent(raft.DeleteTopicEvent{Topic: topicName})
		_ = f.replicationTarget.Apply(&raft.MetadataEvent{EventType: raft.MetadataEventTypeDeleteTopic, Data: eventData})
	}
	return nil
}

func (f *FakeTopicCoordinator) applyIsrUpdateEvent(topicName, replicaNodeID string, isr bool) error {
	f.updateReplicaISR(topicName, replicaNodeID, isr)
	if f.replicationTarget != nil {
		eventData, _ := raft.EncodeIsrUpdateEvent(raft.IsrUpdateEvent{Topic: topicName, ReplicaNodeID: replicaNodeID, Isr: isr})
		_ = f.replicationTarget.Apply(&raft.MetadataEvent{EventType: raft.MetadataEventTypeIsrUpdate, Data: eventData})
	}
	return nil
}

func (f *FakeTopicCoordinator) updateReplicaISR(topicName, replicaNodeID string, isr bool) {
	if f.Replicas[topicName] == nil {
		f.Replicas[topicName] = make(map[string]*cluster.ReplicaState)
	}
	rs := f.Replicas[topicName][replicaNodeID]
	if rs == nil {
		f.Replicas[topicName][replicaNodeID] = &cluster.ReplicaState{ReplicaNodeID: replicaNodeID, LEO: 0, IsISR: isr}
		return
	}
	rs.IsISR = isr
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
		_ = target.ReplicateFromLeader(ctx, leaderID, topicNames, 5000)
	}
}
