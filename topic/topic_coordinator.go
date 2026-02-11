package topic

import (
	"context"

	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/common"
)

// ApplyEventType identifies the kind of topic metadata change to apply via the coordinator.
type ApplyEventType int

const (
	ApplyEventCreateTopic ApplyEventType = iota
	ApplyEventDeleteTopic
	ApplyEventIsrUpdate
)

type CreateTopicApplyEvent struct {
	Topic          string
	ReplicaCount   uint32
	LeaderNodeID   string
	ReplicaNodeIds []string
}

type DeleteTopicApplyEvent struct {
	Topic string
}

type IsrUpdateApplyEvent struct {
	Topic         string
	ReplicaNodeID string
	Isr           bool
	Leo           int64
}

// ApplyEvent is a tagged union of possible apply events. Exactly one payload should be set
// corresponding to Type.
type ApplyEvent struct {
	Type ApplyEventType

	CreateTopic *CreateTopicApplyEvent
	DeleteTopic *DeleteTopicApplyEvent
	IsrUpdate   *IsrUpdateApplyEvent
}

func NewCreateTopicApplyEvent(topic string, replicaCount uint32, leaderNodeID string, replicaNodeIds []string) ApplyEvent {
	return ApplyEvent{
		Type: ApplyEventCreateTopic,
		CreateTopic: &CreateTopicApplyEvent{
			Topic:          topic,
			ReplicaCount:   replicaCount,
			LeaderNodeID:   leaderNodeID,
			ReplicaNodeIds: replicaNodeIds,
		},
	}
}

func NewDeleteTopicApplyEvent(topic string) ApplyEvent {
	return ApplyEvent{
		Type:        ApplyEventDeleteTopic,
		DeleteTopic: &DeleteTopicApplyEvent{Topic: topic},
	}
}

func NewIsrUpdateApplyEvent(topic, replicaNodeID string, isr bool, leo int64) ApplyEvent {
	return ApplyEvent{
		Type: ApplyEventIsrUpdate,
		IsrUpdate: &IsrUpdateApplyEvent{
			Topic:         topic,
			ReplicaNodeID: replicaNodeID,
			Isr:           isr,
			Leo:           leo,
		},
	}
}

// TopicCoordinator is the interface used by TopicManager for cluster metadata, node lookup, and Raft/apply events.
// Coordinator implements this interface; tests can use a fake implementation without Raft.
type TopicCoordinator interface {
	// Node identity and lookup
	GetCurrentNode() *common.Node
	GetNode(nodeID string) (*common.Node, error)
	GetOtherNodes() []*common.Node
	GetRpcClient(nodeID string) (*client.RemoteClient, error)
	GetReplicationClient(nodeID string) (*client.RemoteClient, error)

	// Raft leader
	IsLeader() bool
	GetRaftLeaderRemoteClient() (*client.RemoteClient, error)

	// Topic metadata
	TopicExists(topic string) bool
	ListTopicNames() []string
	GetTopicLeaderNode(topic string) (*common.Node, error)
	GetTopicReplicaNodes(topic string) ([]*common.Node, error)
	GetTopicReplicaStates(topic string) map[string]*common.ReplicaState
	GetNodeIDWithLeastTopics() (*common.Node, error)

	// ApplyEvent enqueues an apply request to the coordinator implementation.
	ApplyEvent(ev ApplyEvent)
}

// ReplicaTopicInfo identifies a topic this node replicates from a given leader.
type ReplicaTopicInfo struct {
	TopicName    string
	LeaderNodeID string
}

// ReplicationTarget is used by the coordinator to drive replication.
// TopicManager implements this; the coordinator holds a reference and calls it from its replication thread.
type ReplicationTarget interface {
	// ListReplicaTopics returns all topics this node replicates (topic name + leader node ID).
	ListReplicaTopics() []ReplicaTopicInfo
	GetLEO(topicName string) (leo uint64, ok bool)
	ApplyChunk(topicName string, rawChunk []byte) error
	ReportLEO(ctx context.Context, topicName string, leaderNodeID string) error
}
