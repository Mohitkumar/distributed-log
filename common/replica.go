package common

// ReplicaState holds the LEO and ISR status of a topic replica (used by topic layer for HW and reporting).
type ReplicaState struct {
	ReplicaNodeID string
	LEO           int64
	IsISR         bool
}
