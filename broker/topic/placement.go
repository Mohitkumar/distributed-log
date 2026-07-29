package topic

// DefaultISRLagThreshold is the max number of records a replica can lag behind
// the leader and still be considered in-sync.
const DefaultISRLagThreshold = uint64(100)

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
