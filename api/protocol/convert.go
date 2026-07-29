package protocol

import "github.com/mohitkumar/mlog/api/protocol/pb"

// This file converts between the hand-written domain types used throughout the
// codebase (topic manager, RPC handlers, clients) and the generated protobuf
// types used only for wire encoding (see codec.go). Keeping the two separate
// means callers never see generated-struct internals (XXX_unrecognized, etc.)
// or have to worry about proto enum naming.

func logEntryToPB(e *LogEntry) *pb.LogEntry {
	if e == nil {
		return nil
	}
	return &pb.LogEntry{Offset: e.Offset, Value: e.Value}
}

func logEntryFromPB(m *pb.LogEntry) *LogEntry {
	if m == nil {
		return nil
	}
	return &LogEntry{Offset: m.Offset, Value: m.Value}
}

func logEntriesToPB(es []*LogEntry) []*pb.LogEntry {
	if es == nil {
		return nil
	}
	out := make([]*pb.LogEntry, len(es))
	for i, e := range es {
		out[i] = logEntryToPB(e)
	}
	return out
}

func logEntriesFromPB(ms []*pb.LogEntry) []*LogEntry {
	if ms == nil {
		return nil
	}
	out := make([]*LogEntry, len(ms))
	for i, m := range ms {
		out[i] = logEntryFromPB(m)
	}
	return out
}

func replicaInfosToPB(rs []ReplicaInfo) []*pb.ReplicaInfo {
	if rs == nil {
		return nil
	}
	out := make([]*pb.ReplicaInfo, len(rs))
	for i, r := range rs {
		out[i] = &pb.ReplicaInfo{NodeId: r.NodeID, IsIsr: r.IsISR, Leo: r.LEO}
	}
	return out
}

func replicaInfosFromPB(ms []*pb.ReplicaInfo) []ReplicaInfo {
	if ms == nil {
		return nil
	}
	out := make([]ReplicaInfo, len(ms))
	for i, m := range ms {
		out[i] = ReplicaInfo{NodeID: m.NodeId, IsISR: m.IsIsr, LEO: m.Leo}
	}
	return out
}

func topicInfosToPB(ts []TopicInfo) []*pb.TopicInfo {
	if ts == nil {
		return nil
	}
	out := make([]*pb.TopicInfo, len(ts))
	for i, t := range ts {
		out[i] = &pb.TopicInfo{
			Name:         t.Name,
			LeaderNodeId: t.LeaderNodeID,
			LeaderEpoch:  t.LeaderEpoch,
			Replicas:     replicaInfosToPB(t.Replicas),
		}
	}
	return out
}

func topicInfosFromPB(ms []*pb.TopicInfo) []TopicInfo {
	if ms == nil {
		return nil
	}
	out := make([]TopicInfo, len(ms))
	for i, m := range ms {
		out[i] = TopicInfo{
			Name:         m.Name,
			LeaderNodeID: m.LeaderNodeId,
			LeaderEpoch:  m.LeaderEpoch,
			Replicas:     replicaInfosFromPB(m.Replicas),
		}
	}
	return out
}
