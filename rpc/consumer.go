package rpc

import (
	"context"
	"fmt"

	"github.com/mohitkumar/mlog/protocol"
)

func (s *RpcServer) Fetch(ctx context.Context, req *protocol.FetchRequest) (*protocol.FetchResponse, error) {
	if req.Topic == "" {
		return nil, Err(protocol.CodeTopicRequired, "topic is required")
	}
	id := req.Id
	if id == "" {
		id = "default"
	}

	off := req.Offset
	if off == 0 && req.ReplicaNodeID == "" {
		if err := s.consumerManager.Recover(); err == nil {
			if cached, err := s.consumerManager.GetOffset(id, req.Topic); err == nil {
				off = cached
			}
		}
	}

	leaderLog, err := s.topicManager.GetLeader(req.Topic)
	if err != nil {
		return nil, &protocol.RPCError{Code: protocol.CodeTopicNotFound, Message: fmt.Sprintf("topic %s not found: %v", req.Topic, err)}
	}

	var raw []byte
	if req.ReplicaNodeID != "" {
		raw, err = leaderLog.ReadUncommitted(off)
	} else {
		raw, err = leaderLog.Read(off)
	}
	if err != nil {
		if req.ReplicaNodeID != "" {
			_ = s.topicManager.RecordReplicaLEOFromFetch(ctx, req.Topic, req.ReplicaNodeID, int64(req.Offset))
		}
		return nil, &protocol.RPCError{Code: protocol.CodeReadOffset, Message: fmt.Sprintf("failed to read offset %d: %v", off, err)}
	}
	// Segment returns [offset 8 bytes][value]; strip header for response
	const offWidth = 8
	if len(raw) >= offWidth {
		raw = raw[offWidth:]
	}

	if req.ReplicaNodeID != "" {
		_ = s.topicManager.RecordReplicaLEOFromFetch(ctx, req.Topic, req.ReplicaNodeID, int64(off+1))
	}
	return &protocol.FetchResponse{
		Entry: &protocol.LogEntry{Offset: off, Value: raw},
	}, nil
}

func (s *RpcServer) FetchBatch(ctx context.Context, req *protocol.FetchBatchRequest) (*protocol.FetchBatchResponse, error) {
	if req.Topic == "" {
		return nil, Err(protocol.CodeTopicRequired, "topic is required")
	}
	id := req.Id
	if id == "" {
		id = "default"
	}

	off := req.Offset
	if off == 0 && req.ReplicaNodeID == "" {
		if err := s.consumerManager.Recover(); err == nil {
			if cached, err := s.consumerManager.GetOffset(id, req.Topic); err == nil {
				off = cached
			}
		}
	}

	leaderLog, err := s.topicManager.GetLeader(req.Topic)
	if err != nil {
		return nil, &protocol.RPCError{Code: protocol.CodeTopicNotFound, Message: fmt.Sprintf("topic %s not found: %v", req.Topic, err)}
	}

	maxCount := req.MaxCount
	if maxCount == 0 {
		maxCount = 1
	}

	const offWidth = 8
	var entries []*protocol.LogEntry
	useUncommitted := req.ReplicaNodeID != ""

	for n := uint32(0); n < maxCount; n++ {
		var raw []byte
		if useUncommitted {
			raw, err = leaderLog.ReadUncommitted(off)
		} else {
			raw, err = leaderLog.Read(off)
		}
		if err != nil {
			break
		}
		value := raw
		if len(raw) >= offWidth {
			value = raw[offWidth:]
		}
		entries = append(entries, &protocol.LogEntry{Offset: off, Value: value})
		off++
	}

	if req.ReplicaNodeID != "" {
		replicaLEO := int64(off)
		if len(entries) == 0 {
			replicaLEO = int64(req.Offset)
			_ = s.topicManager.RecordReplicaLEOFromFetch(ctx, req.Topic, req.ReplicaNodeID, replicaLEO)
			return nil, &protocol.RPCError{Code: protocol.CodeReadOffset, Message: fmt.Sprintf("no data at offset %d (replica caught up)", req.Offset)}
		}
		_ = s.topicManager.RecordReplicaLEOFromFetch(ctx, req.Topic, req.ReplicaNodeID, replicaLEO)
	}

	return &protocol.FetchBatchResponse{Entries: entries}, nil
}

func (s *RpcServer) CommitOffset(ctx context.Context, req *protocol.CommitOffsetRequest) (*protocol.CommitOffsetResponse, error) {
	if req.Topic == "" {
		return nil, Err(protocol.CodeTopicRequired, "topic is required")
	}

	id := req.Id
	if id == "" {
		id = "default"
	}

	if err := s.consumerManager.CommitOffset(id, req.Topic, req.Offset); err != nil {
		return nil, &protocol.RPCError{Code: protocol.CodeCommitOffset, Message: fmt.Sprintf("commit offset failed: %v", err)}
	}
	return &protocol.CommitOffsetResponse{Success: true}, nil
}

func (s *RpcServer) FetchOffset(ctx context.Context, req *protocol.FetchOffsetRequest) (*protocol.FetchOffsetResponse, error) {
	if req.Topic == "" {
		return nil, Err(protocol.CodeTopicRequired, "topic is required")
	}

	id := req.Id
	if id == "" {
		id = "default"
	}

	if err := s.consumerManager.Recover(); err != nil {
		return nil, &protocol.RPCError{Code: protocol.CodeRecoverOffsets, Message: fmt.Sprintf("recover offsets failed: %v", err)}
	}

	off, err := s.consumerManager.GetOffset(id, req.Topic)
	if err != nil {
		return &protocol.FetchOffsetResponse{Offset: 0}, nil
	}
	return &protocol.FetchOffsetResponse{Offset: off}, nil
}
