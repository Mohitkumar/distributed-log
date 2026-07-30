package topic

import "time"

// DefaultISRLagThreshold is the max number of records a replica can lag behind
// the leader and still be considered in-sync.
const DefaultISRLagThreshold = uint64(100)

// DefaultISRLagTime mirrors Kafka's replica.lag.time.max.ms: an ISR replica that
// hasn't sent a Fetch within this long is dropped from ISR regardless of offset lag —
// the liveness-independent half of ISR membership (RecordReplicaFetch/
// DefaultISRLagThreshold cover the reactive, offset-lag half but never fire for a
// replica that's gone silent rather than merely behind).
const DefaultISRLagTime = 30 * time.Second

// PickReplicaNodeIds returns up to replicaCount node IDs from candidateNodeIDs,
// excluding leaderNodeID. Pure placement policy — no cluster metadata state
// involved, so it lives here rather than behind TopicCoordinator.
func PickReplicaNodeIds(leaderNodeID string, replicaCount int, candidateNodeIDs []string) ([]string, error) {
	var others []string
	for _, id := range candidateNodeIDs {
		if id != leaderNodeID {
			others = append(others, id)
		}
	}
	if len(others) < replicaCount {
		return nil, ErrNotEnoughNodesf(replicaCount, len(others))
	}
	return others[:replicaCount], nil
}
