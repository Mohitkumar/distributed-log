package topic

import (
	"time"

	"github.com/mohitkumar/mlog/api/protocol"
)

// TopicCoordinator is the interface TopicManager uses for every cluster-metadata
// query, cluster-metadata mutation, and cluster-membership query it needs. TopicManager
// never holds or touches cluster-internal storage directly — only through this
// interface — which keeps topic a lean consumer of cluster state rather than a second
// owner of it. Implemented by *cluster.Cluster and *tests.FakeTopicCoordinator.
type TopicCoordinator interface {
	// — metadata queries (read-only) —

	TopicExists(topic string) bool
	TopicNames() []string
	// TopicInfo returns a point-in-time snapshot of topic's leader/epoch/replica
	// state, or ok=false if topic doesn't exist.
	TopicInfo(topic string) (info protocol.TopicInfo, ok bool)
	// TopicLeaderNodeID returns the current leader node ID for topic, or ok=false
	// if topic doesn't exist.
	TopicLeaderNodeID(topic string) (leaderNodeID string, ok bool)
	// TopicHasReplica reports whether nodeID is a tracked replica of topic.
	TopicHasReplica(topic, nodeID string) bool
	// TopicMinISRLeo returns min(localLEO, all of topic's in-sync replicas' LEO),
	// used to compute the consumer-visible high watermark.
	TopicMinISRLeo(topic string, localLEO uint64) uint64
	NodeIDWithLeastTopics(candidateNodeIDs []string) (string, error)

	// — metadata mutation (Raft-applied; only take effect when called on the Raft leader) —

	ApplyCreateTopicEvent(topic string, replicaCount uint32, leaderNodeID string, replicaNodeIds []string) error
	ApplyDeleteTopicEventInternal(topic string) error
	ApplyIsrUpdateEventInternal(topic, replicaNodeID string, isr bool) error
	ApplyLeaderChangeEvent(topic, leaderNodeID string, leaderEpoch int64) error
	// RecordReplicaFetch updates replicaNodeID's LEO for topic from a Fetch call and
	// recomputes its ISR status against lagThreshold.
	RecordReplicaFetch(topic, replicaNodeID string, leo int64, lagThreshold uint64, localLEO uint64) (isr bool, ok bool)
	// ExpireStaleISR demotes any of topic's ISR replicas that haven't fetched within
	// maxLag (Kafka's replica.lag.time.max.ms), returning the node IDs demoted.
	ExpireStaleISR(topic string, maxLag time.Duration) []string

	// — cluster/raft state —

	IsLeader() bool
	GetRaftLeaderNodeID() (string, error)
	AliveNodeIDs() []string
	NodeRPCAddr(nodeID string) (string, bool)
	IsNodeAlive(nodeID string) bool
}

// ApplyEventType is the type of a metadata apply event (used by tests with fake coordinator).
type ApplyEventType int

const (
	ApplyEventCreateTopic ApplyEventType = iota
	ApplyEventDeleteTopic
	ApplyEventIsrUpdate
)

// ApplyEvent is a single metadata event (used by tests; fake coordinator dispatches on Type).
type ApplyEvent struct {
	Type        ApplyEventType
	CreateTopic *struct {
		Topic          string
		ReplicaCount   uint32
		LeaderNodeID   string
		ReplicaNodeIds []string
	}
	DeleteTopic *struct{ Topic string }
	IsrUpdate   *struct {
		Topic         string
		ReplicaNodeID string
		Isr           bool
	}
}

// NewCreateTopicApplyEvent builds an ApplyEvent for CreateTopic (for tests).
func NewCreateTopicApplyEvent(topic string, replicaCount uint32, leaderNodeID string, replicaNodeIds []string) ApplyEvent {
	return ApplyEvent{
		Type: ApplyEventCreateTopic,
		CreateTopic: &struct {
			Topic          string
			ReplicaCount   uint32
			LeaderNodeID   string
			ReplicaNodeIds []string
		}{Topic: topic, ReplicaCount: replicaCount, LeaderNodeID: leaderNodeID, ReplicaNodeIds: replicaNodeIds},
	}
}

// NewDeleteTopicApplyEvent builds an ApplyEvent for DeleteTopic (for tests).
func NewDeleteTopicApplyEvent(topic string) ApplyEvent {
	return ApplyEvent{Type: ApplyEventDeleteTopic, DeleteTopic: &struct{ Topic string }{Topic: topic}}
}
