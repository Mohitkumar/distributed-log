package topic

import "github.com/mohitkumar/mlog/api/protocol/pb"

// snapshotToPB/pbToSnapshot convert between the in-memory TopicManager state and
// the protobuf MetadataSnapshot message used for Raft snapshot persistence.
// Runtime-only fields (mutexes, open log handles, loggers) are never part of
// the snapshot; they're reconstructed by Restore.

func snapshotToPB(topics map[string]*Topic, nodes map[string]*NodeMetadata) *pb.MetadataSnapshot {
	pbTopics := make(map[string]*pb.TopicState, len(topics))
	for name, t := range topics {
		if t == nil {
			continue
		}
		replicas := make(map[string]*pb.ReplicaState, len(t.Replicas))
		for id, r := range t.Replicas {
			if r == nil {
				continue
			}
			replicas[id] = &pb.ReplicaState{ReplicaId: r.ReplicaNodeID, Leo: r.LEO, IsIsr: r.IsISR}
		}
		pbTopics[name] = &pb.TopicState{
			Name:                t.Name,
			LeaderId:            t.LeaderNodeID,
			LeaderEpoch:         t.LeaderEpoch,
			DesiredReplicaCount: int32(t.DesiredReplicaCount),
			Replicas:            replicas,
		}
	}
	pbNodes := make(map[string]*pb.NodeState, len(nodes))
	for id, n := range nodes {
		if n == nil {
			continue
		}
		pbNodes[id] = &pb.NodeState{NodeId: n.NodeID, Addr: n.Addr, RpcAddr: n.RpcAddr}
	}
	return &pb.MetadataSnapshot{Topics: pbTopics, Nodes: pbNodes}
}

func pbToSnapshot(m *pb.MetadataSnapshot) (map[string]*Topic, map[string]*NodeMetadata) {
	topics := make(map[string]*Topic, len(m.Topics))
	for name, t := range m.Topics {
		if t == nil {
			continue
		}
		replicas := make(map[string]*ReplicaState, len(t.Replicas))
		for id, r := range t.Replicas {
			if r == nil {
				continue
			}
			replicas[id] = &ReplicaState{ReplicaNodeID: r.ReplicaId, LEO: r.Leo, IsISR: r.IsIsr}
		}
		topics[name] = &Topic{
			Name:                t.Name,
			LeaderNodeID:        t.LeaderId,
			LeaderEpoch:         t.LeaderEpoch,
			DesiredReplicaCount: int(t.DesiredReplicaCount),
			Replicas:            replicas,
		}
	}
	nodes := make(map[string]*NodeMetadata, len(m.Nodes))
	for id, n := range m.Nodes {
		if n == nil {
			continue
		}
		nodes[id] = &NodeMetadata{NodeID: n.NodeId, Addr: n.Addr, RpcAddr: n.RpcAddr}
	}
	return topics, nodes
}
