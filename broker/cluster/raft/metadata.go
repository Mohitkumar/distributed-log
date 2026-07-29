package raft

import (
	"github.com/mohitkumar/mlog/api/protocol/pb"
	"google.golang.org/protobuf/proto"
)

type MetadataStore interface {
	Apply(ev *MetadataEvent) error
	Restore(data []byte) error
	Snapshot() ([]byte, error)
}

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
	EventType MetadataEventType
	Data      []byte
}

// EncodeMetadataEvent serializes the event envelope (event type + already-encoded
// payload bytes) as protobuf, for persisting via raft.Apply.
func EncodeMetadataEvent(ev *MetadataEvent) ([]byte, error) {
	return proto.Marshal(&pb.MetadataEvent{EventType: uint32(ev.EventType), Data: ev.Data})
}

// DecodeMetadataEvent decodes a MetadataEvent envelope previously written by EncodeMetadataEvent.
func DecodeMetadataEvent(data []byte) (*MetadataEvent, error) {
	var m pb.MetadataEvent
	if err := proto.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &MetadataEvent{EventType: MetadataEventType(m.EventType), Data: m.Data}, nil
}

type CreateTopicEvent struct {
	Topic          string
	ReplicaCount   uint32
	LeaderNodeID   string
	LeaderEpoch    int64
	ReplicaNodeIds []string
}

func EncodeCreateTopicEvent(e CreateTopicEvent) ([]byte, error) {
	return proto.Marshal(&pb.CreateTopicEvent{
		Topic:        e.Topic,
		ReplicaCount: e.ReplicaCount,
		LeaderId:     e.LeaderNodeID,
		LeaderEpoch:  e.LeaderEpoch,
		Replicas:     e.ReplicaNodeIds,
	})
}

func DecodeCreateTopicEvent(data []byte) (CreateTopicEvent, error) {
	var m pb.CreateTopicEvent
	if err := proto.Unmarshal(data, &m); err != nil {
		return CreateTopicEvent{}, err
	}
	return CreateTopicEvent{
		Topic:          m.Topic,
		ReplicaCount:   m.ReplicaCount,
		LeaderNodeID:   m.LeaderId,
		LeaderEpoch:    m.LeaderEpoch,
		ReplicaNodeIds: m.Replicas,
	}, nil
}

type DeleteTopicEvent struct {
	Topic string
}

func EncodeDeleteTopicEvent(e DeleteTopicEvent) ([]byte, error) {
	return proto.Marshal(&pb.DeleteTopicEvent{Topic: e.Topic})
}

func DecodeDeleteTopicEvent(data []byte) (DeleteTopicEvent, error) {
	var m pb.DeleteTopicEvent
	if err := proto.Unmarshal(data, &m); err != nil {
		return DeleteTopicEvent{}, err
	}
	return DeleteTopicEvent{Topic: m.Topic}, nil
}

type LeaderChangeEvent struct {
	Topic        string
	LeaderNodeID string
	LeaderEpoch  int64
}

func EncodeLeaderChangeEvent(e LeaderChangeEvent) ([]byte, error) {
	return proto.Marshal(&pb.LeaderChangeEvent{Topic: e.Topic, LeaderId: e.LeaderNodeID, LeaderEpoch: e.LeaderEpoch})
}

func DecodeLeaderChangeEvent(data []byte) (LeaderChangeEvent, error) {
	var m pb.LeaderChangeEvent
	if err := proto.Unmarshal(data, &m); err != nil {
		return LeaderChangeEvent{}, err
	}
	return LeaderChangeEvent{Topic: m.Topic, LeaderNodeID: m.LeaderId, LeaderEpoch: m.LeaderEpoch}, nil
}

type IsrUpdateEvent struct {
	Topic         string
	ReplicaNodeID string
	Isr           bool
}

func EncodeIsrUpdateEvent(e IsrUpdateEvent) ([]byte, error) {
	return proto.Marshal(&pb.IsrUpdateEvent{Topic: e.Topic, ReplicaId: e.ReplicaNodeID, Isr: e.Isr})
}

func DecodeIsrUpdateEvent(data []byte) (IsrUpdateEvent, error) {
	var m pb.IsrUpdateEvent
	if err := proto.Unmarshal(data, &m); err != nil {
		return IsrUpdateEvent{}, err
	}
	return IsrUpdateEvent{Topic: m.Topic, ReplicaNodeID: m.ReplicaId, Isr: m.Isr}, nil
}

type AddNodeEvent struct {
	NodeID  string
	Addr    string
	RpcAddr string
}

func EncodeAddNodeEvent(e AddNodeEvent) ([]byte, error) {
	return proto.Marshal(&pb.AddNodeEvent{NodeId: e.NodeID, Addr: e.Addr, RpcAddr: e.RpcAddr})
}

func DecodeAddNodeEvent(data []byte) (AddNodeEvent, error) {
	var m pb.AddNodeEvent
	if err := proto.Unmarshal(data, &m); err != nil {
		return AddNodeEvent{}, err
	}
	return AddNodeEvent{NodeID: m.NodeId, Addr: m.Addr, RpcAddr: m.RpcAddr}, nil
}

type RemoveNodeEvent struct {
	NodeID string
}

func EncodeRemoveNodeEvent(e RemoveNodeEvent) ([]byte, error) {
	return proto.Marshal(&pb.RemoveNodeEvent{NodeId: e.NodeID})
}

func DecodeRemoveNodeEvent(data []byte) (RemoveNodeEvent, error) {
	var m pb.RemoveNodeEvent
	if err := proto.Unmarshal(data, &m); err != nil {
		return RemoveNodeEvent{}, err
	}
	return RemoveNodeEvent{NodeID: m.NodeId}, nil
}

type UpdateNodeEvent struct {
	NodeID    string
	IsHealthy bool
}

func EncodeUpdateNodeEvent(e UpdateNodeEvent) ([]byte, error) {
	return proto.Marshal(&pb.UpdateNodeEvent{NodeId: e.NodeID, IsHealthy: e.IsHealthy})
}

func DecodeUpdateNodeEvent(data []byte) (UpdateNodeEvent, error) {
	var m pb.UpdateNodeEvent
	if err := proto.Unmarshal(data, &m); err != nil {
		return UpdateNodeEvent{}, err
	}
	return UpdateNodeEvent{NodeID: m.NodeId, IsHealthy: m.IsHealthy}, nil
}
