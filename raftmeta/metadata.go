package raftmeta

import (
	"sync"

	"github.com/mohitkumar/mlog/protocol"
)

type NodeMetadata struct {
	NodeID    string `json:"node_id"`
	Addr      string `json:"addr"`
	IsHealthy bool   `json:"is_healthy"`
}
type TopicMetadata struct {
	LeaderID    string                   `json:"leader_id"`
	LeaderEpoch int64                    `json:"leader_epoch"`
	Replicas    map[string]*ReplicaState `json:"replicas"`
}

type ReplicaState struct {
	ReplicaID string `json:"replica_id"`
	LEO       int64  `json:"leo"`
	IsISR     bool   `json:"is_isr"`
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

func (ms *MetadataStore) Apply(ev *protocol.MetadataEvent) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	switch ev.WhichEvent() {
	case "CreateTopicEvent":
		e := ev.CreateTopicEvent
		ms.topics[e.Topic] = &TopicMetadata{
			LeaderID:    e.LeaderID,
			LeaderEpoch: e.LeaderEpoch,
			Replicas:    make(map[string]*ReplicaState),
		}
		for _, replica := range e.Replicas {
			ms.topics[e.Topic].Replicas[replica] = &ReplicaState{
				ReplicaID: replica,
				LEO:       0,
				IsISR:     true,
			}
		}
	case "LeaderChangeEvent":
		e := ev.LeaderChangeEvent
		ms.topics[e.Topic].LeaderID = e.LeaderID
		ms.topics[e.Topic].LeaderEpoch = e.LeaderEpoch
	case "IsrUpdateEvent":
		e := ev.IsrUpdateEvent
		ms.topics[e.Topic].Replicas[e.ReplicaID].IsISR = true
		for _, replica := range e.Isr {
			ms.topics[e.Topic].Replicas[replica].IsISR = true
		}
	case "DeleteTopicEvent":
		e := ev.DeleteTopicEvent
		delete(ms.topics, e.Topic)
	case "AddNodeEvent":
		e := ev.AddNodeEvent
		ms.nodes[e.NodeID] = &NodeMetadata{
			NodeID: e.NodeID,
			Addr:   e.Addr,
		}
	case "RemoveNodeEvent":
		e := ev.RemoveNodeEvent
		delete(ms.nodes, e.NodeID)
	case "UpdateNodeEvent":
		e := ev.UpdateNodeEvent
		ms.nodes[e.NodeID].IsHealthy = e.IsHealthy
	}
}
