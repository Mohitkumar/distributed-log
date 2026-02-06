package raftmeta

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

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

const defaultLogInterval = 30 * time.Second

type MetadataStore struct {
	mu           sync.RWMutex
	Nodes        map[string]*NodeMetadata  `json:"nodes"`
	Topics       map[string]*TopicMetadata `json:"topics"`
	stopPeriodic chan struct{}
}

func NewMetadataStore() *MetadataStore {
	ms := &MetadataStore{
		Nodes:        make(map[string]*NodeMetadata),
		Topics:       make(map[string]*TopicMetadata),
		stopPeriodic: make(chan struct{}),
	}
	go ms.periodicLog(defaultLogInterval)
	return ms
}

// periodicLog prints the metadata store every interval until StopPeriodicLog is called.
func (ms *MetadataStore) periodicLog(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ms.stopPeriodic:
			return
		case <-ticker.C:
			ms.mu.RLock()
			b, err := json.Marshal(ms)
			ms.mu.RUnlock()
			if err != nil {
				fmt.Printf("[raftmeta] periodic log marshal error: %v\n", err)
				continue
			}
			fmt.Printf("[raftmeta] metadata store (%s):\n%s\n", time.Now().Format(time.RFC3339), string(b))
		}
	}
}

// StopPeriodicLog stops the periodic logging goroutine. Safe to call multiple times.
func (ms *MetadataStore) StopPeriodicLog() {
	select {
	case <-ms.stopPeriodic:
		return
	default:
		close(ms.stopPeriodic)
	}
}

func (ms *MetadataStore) GetTopic(topic string) *TopicMetadata {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.Topics[topic]
}

// ListTopicNames returns all topic names in the metadata (for restore after restart).
func (ms *MetadataStore) ListTopicNames() []string {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	names := make([]string, 0, len(ms.Topics))
	for name := range ms.Topics {
		names = append(names, name)
	}
	return names
}

func (ms *MetadataStore) Apply(ev *protocol.MetadataEvent) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	switch ev.EventType {
	case protocol.MetadataEventTypeCreateTopic:
		e := protocol.CreateTopicEvent{}
		if err := json.Unmarshal(ev.Data, &e); err != nil {
			return err
		}
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
	case protocol.MetadataEventTypeLeaderChange:
		e := protocol.LeaderChangeEvent{}
		if err := json.Unmarshal(ev.Data, &e); err != nil {
			return err
		}
		if tm := ms.Topics[e.Topic]; tm != nil {
			tm.LeaderNodeID = e.LeaderNodeID
			tm.LeaderEpoch = e.LeaderEpoch
		}
	case protocol.MetadataEventTypeIsrUpdate:
		e := protocol.IsrUpdateEvent{}
		if err := json.Unmarshal(ev.Data, &e); err != nil {
			return err
		}
		if tm := ms.Topics[e.Topic]; tm != nil {
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
		delete(ms.Topics, e.Topic)
	case protocol.MetadataEventTypeAddNode:
		e := protocol.AddNodeEvent{}
		if err := json.Unmarshal(ev.Data, &e); err != nil {
			return err
		}
		ms.Nodes[e.NodeID] = &NodeMetadata{
			NodeID:  e.NodeID,
			Addr:    e.Addr,
			RpcAddr: e.RpcAddr,
		}
	case protocol.MetadataEventTypeRemoveNode:
		e := protocol.RemoveNodeEvent{}
		if err := json.Unmarshal(ev.Data, &e); err != nil {
			return err
		}
		delete(ms.Nodes, e.NodeID)
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

func (ms *MetadataStore) GetNodeMetadata(nodeID string) *NodeMetadata {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.Nodes[nodeID]
}

// TopicCountByLeader returns the number of topics each node is leader of.
func (ms *MetadataStore) TopicCountByLeader() map[string]int {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	counts := make(map[string]int)
	for _, tm := range ms.Topics {
		if tm != nil && tm.LeaderNodeID != "" {
			counts[tm.LeaderNodeID]++
		}
	}
	return counts
}
