package common

// Node holds identity and address for a cluster node (used by tests and discovery).
type Node struct {
	NodeID  string
	RPCAddr string
}

// ReplicaState holds LEO and ISR state for a topic replica (used by tests and metadata).
type ReplicaState struct {
	ReplicaNodeID string
	LEO           int64
	IsISR         bool
}
