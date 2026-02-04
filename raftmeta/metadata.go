package raftmeta

import (
	"sync"

	"github.com/mohitkumar/mlog/protocol"
)

type NodeMetadata struct {
	NodeID  string `json:"node_id"`
	Addr    string `json:"addr"`
	RpcAddr string `json:"rpc_addr"`
}
type TopicMetadata struct {
	LeaderNodeID string                   `json:"leader_id"`
	LeaderEpoch  int64                    `json:"leader_epoch"`
	Replicas     map[string]*ReplicaState `json:"replicas"`
}

type ReplicaState struct {
	ReplicaNodeID string `json:"replica_id"`
	LEO           int64  `json:"leo"`
	IsISR         bool   `json:"is_isr"`
}

type MetadataStore struct {
	mu     sync.RWMutex
	Nodes  map[string]*NodeMetadata  `json:"nodes"`
	Topics map[string]*TopicMetadata `json:"topics"`
}

func NewMetadataStore() *MetadataStore {
	return &MetadataStore{
		Nodes:  make(map[string]*NodeMetadata),
		Topics: make(map[string]*TopicMetadata),
	}
}

func (ms *MetadataStore) GetTopic(topic string) *TopicMetadata {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.Topics[topic]
}

func (ms *MetadataStore) Apply(ev *protocol.MetadataEvent) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	switch ev.WhichEvent() {
	case "CreateTopicEvent":
		e := ev.CreateTopicEvent
		ms.Topics[e.Topic] = &TopicMetadata{
			LeaderNodeID: e.LeaderNodeID,
			LeaderEpoch:  e.LeaderEpoch,
			Replicas:     make(map[string]*ReplicaState),
		}
		for _, replica := range e.ReplicaNodeIds {
			ms.Topics[e.Topic].Replicas[replica] = &ReplicaState{
				ReplicaNodeID: replica,
				LEO:           0,
				IsISR:         true,
			}
		}
	case "LeaderChangeEvent":
		e := ev.LeaderChangeEvent
		if tm := ms.Topics[e.Topic]; tm != nil {
			tm.LeaderNodeID = e.LeaderNodeID
			tm.LeaderEpoch = e.LeaderEpoch
		}
	case "IsrUpdateEvent":
		e := ev.IsrUpdateEvent
		if tm := ms.Topics[e.Topic]; tm != nil {
			if rs := tm.Replicas[e.ReplicaNodeID]; rs != nil {
				rs.IsISR = e.Isr
			}
		}
	case "DeleteTopicEvent":
		e := ev.DeleteTopicEvent
		delete(ms.Topics, e.Topic)
	case "AddNodeEvent":
		e := ev.AddNodeEvent
		ms.Nodes[e.NodeID] = &NodeMetadata{
			NodeID:  e.NodeID,
			Addr:    e.Addr,
			RpcAddr: e.RpcAddr,
		}
	case "RemoveNodeEvent":
		e := ev.RemoveNodeEvent
		delete(ms.Nodes, e.NodeID)
	case "UpdateNodeEvent":
		//TODO: update node status
	}
}

func (ms *MetadataStore) GetNodeMetadata(nodeID string) *NodeMetadata {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.Nodes[nodeID]
}
