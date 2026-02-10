package tests

import (
	"fmt"
	"sync"

	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/common"
	"github.com/mohitkumar/mlog/topic"
)

// FakeTopicCoordinator implements topic.TopicCoordinator in memory without Raft.
// Use for unit testing TopicManager in isolation.
type FakeTopicCoordinator struct {
	mu sync.RWMutex

	NodeID  string
	RPCAddr string
	IsRaftLeader bool

	Nodes   map[string]*common.Node          // nodeID -> node
	Topics  map[string]*fakeTopicMeta        // topic -> meta
	Replicas map[string]map[string]*common.ReplicaState // topic -> replicaNodeID -> state
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

func (f *FakeTopicCoordinator) GetRpcStreamClient(nodeID string) (*client.RemoteStreamClient, error) {
	f.mu.RLock()
	n, ok := f.Nodes[nodeID]
	f.mu.RUnlock()
	if !ok || n == nil {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}
	return client.NewRemoteStreamClient(n.RPCAddr)
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

func (f *FakeTopicCoordinator) GetNodeIDWithLeastTopics() (*common.Node, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if len(f.Nodes) == 0 {
		return nil, fmt.Errorf("no nodes in cluster")
	}
	// Single node: return self
	return f.Nodes[f.NodeID], nil
}

func (f *FakeTopicCoordinator) ApplyCreateTopicEvent(topicName string, replicaCount uint32, leaderNodeID string, replicaNodeIds []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
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
	return nil
}

func (f *FakeTopicCoordinator) ApplyDeleteTopicEvent(topicName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.Topics, topicName)
	delete(f.Replicas, topicName)
	return nil
}

func (f *FakeTopicCoordinator) ApplyIsrUpdateEvent(topicName, replicaNodeID string, isr bool, leo int64) error {
	f.UpdateTopicReplicaLEO(topicName, replicaNodeID, leo, isr)
	return nil
}

// AddNode adds a node to the fake cluster (for multi-node tests without Raft).
func (f *FakeTopicCoordinator) AddNode(nodeID, rpcAddr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Nodes[nodeID] = &common.Node{NodeID: nodeID, RPCAddr: rpcAddr}
}
