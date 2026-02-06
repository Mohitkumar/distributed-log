package protocol

import "encoding/json"

type MetadataEventType uint16

const (
	MetadataEventTypeCreateTopic MetadataEventType = iota
	MetadataEventTypeDeleteTopic
	MetadataEventTypeLeaderChange
	MetadataEventTypeIsrUpdate
	MetadataEventTypeAddNode
	MetadataEventTypeRemoveNode
	MetadataEventTypeUpdateNode
)

type MetadataEvent struct {
	EventType MetadataEventType `json:"event_type"`
	Data      json.RawMessage   `json:"data"`
}

type CreateTopicEvent struct {
	Topic          string   `json:"topic"`
	ReplicaCount   uint32   `json:"replica_count"`
	LeaderNodeID   string   `json:"leader_id"`
	LeaderEpoch    int64    `json:"leader_epoch"`
	ReplicaNodeIds []string `json:"replicas"`
}

type DeleteTopicEvent struct {
	Topic string `json:"topic"`
}

type LeaderChangeEvent struct {
	Topic        string `json:"topic"`
	LeaderNodeID string `json:"leader_id"`
	LeaderEpoch  int64  `json:"leader_epoch"`
}

type IsrUpdateEvent struct {
	Topic         string `json:"topic"`
	ReplicaNodeID string `json:"replica_id"`
	Isr           bool   `json:"isr"`
	Leo           int64  `json:"leo"`
}

type AddNodeEvent struct {
	NodeID  string `json:"node_id"`
	Addr    string `json:"addr"`
	RpcAddr string `json:"rpc_addr"`
}

type RemoveNodeEvent struct {
	NodeID string `json:"node_id"`
}

type UpdateNodeEvent struct {
	NodeID    string `json:"node_id"`
	IsHealthy bool   `json:"is_healthy"`
}
