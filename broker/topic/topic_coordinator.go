package topic

// TopicCoordinator is the interface TopicManager uses to apply metadata events (Raft or fake in tests)
// and to query cluster membership. Implemented by *cluster.Cluster and *tests.FakeTopicCoordinator.
type TopicCoordinator interface {
	ApplyCreateTopicEvent(topic string, replicaCount uint32, leaderNodeID string, replicaNodeIds []string) error
	ApplyDeleteTopicEventInternal(topic string) error
	ApplyIsrUpdateEventInternal(topic, replicaNodeID string, isr bool) error
	ApplyLeaderChangeEvent(topic, leaderNodeID string, leaderEpoch int64) error
	IsLeader() bool
	GetRaftLeaderNodeID() (string, error)
	// AliveNodeIDs returns the current cluster member node IDs, used as the
	// candidate list for topic/replica placement.
	AliveNodeIDs() []string
	// NodeRPCAddr returns the RPC address for nodeID, if known.
	NodeRPCAddr(nodeID string) (string, bool)
	// IsNodeAlive reports whether nodeID is currently a recognized cluster member.
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
