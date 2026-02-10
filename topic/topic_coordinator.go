package topic

import (
	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/common"
)

// TopicCoordinator is the interface used by TopicManager for cluster metadata, node lookup, and Raft/apply events.
// Coordinator implements this interface; tests can use a fake implementation without Raft.
type TopicCoordinator interface {
	// Node identity and lookup
	GetCurrentNode() *common.Node
	GetNode(nodeID string) (*common.Node, error)
	GetOtherNodes() []*common.Node
	GetRpcClient(nodeID string) (*client.RemoteClient, error)
	GetRpcStreamClient(nodeID string) (*client.RemoteStreamClient, error)

	// Raft leader
	IsLeader() bool
	GetRaftLeaderRemoteClient() (*client.RemoteClient, error)

	// Topic metadata
	TopicExists(topic string) bool
	ListTopicNames() []string
	GetTopicLeaderNode(topic string) (*common.Node, error)
	GetTopicReplicaNodes(topic string) ([]*common.Node, error)
	GetTopicReplicaStates(topic string) map[string]*common.ReplicaState
	UpdateTopicReplicaLEO(topic, replicaNodeID string, leo int64, isr bool)
	GetNodeIDWithLeastTopics() (*common.Node, error)

	// Apply events (Raft leader applies to log)
	ApplyCreateTopicEvent(topic string, replicaCount uint32, leaderNodeID string, replicaNodeIds []string) error
	ApplyDeleteTopicEvent(topic string) error
	ApplyIsrUpdateEvent(topic, replicaNodeID string, isr bool, leo int64) error
}
