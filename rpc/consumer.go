package rpc

import (
	"context"

	"github.com/mohitkumar/mlog/protocol"
)

func (s *RpcServer) Fetch(ctx context.Context, req *protocol.FetchRequest) (*protocol.FetchResponse, error) {
	if req.Topic == "" {
		return nil, ErrTopicRequired
	}
	id := req.Id
	if id == "" {
		id = "default"
	}

	off := req.Offset
	if off == 0 {
		if err := s.consumerManager.Recover(); err == nil {
			if cached, err := s.consumerManager.GetOffset(id, req.Topic); err == nil {
				off = cached
			}
		}
	}

	leaderNode, err := s.topicManager.GetLeader(req.Topic)
	if err != nil {
		return nil, ErrTopicNotFound(req.Topic, err)
	}

	raw, err := leaderNode.Log.Read(off)
	if err != nil {
		return nil, ErrReadOffset(off, err)
	}
	// Segment returns [offset 8 bytes][value]; strip header for response
	const offWidth = 8
	if len(raw) >= offWidth {
		raw = raw[offWidth:]
	}

	return &protocol.FetchResponse{
		Entry: &protocol.LogEntry{Offset: off, Value: raw},
	}, nil
}

func (s *RpcServer) CommitOffset(ctx context.Context, req *protocol.CommitOffsetRequest) (*protocol.CommitOffsetResponse, error) {
	if req.Topic == "" {
		return nil, ErrTopicRequired
	}

	id := req.Id
	if id == "" {
		id = "default"
	}

	if err := s.consumerManager.CommitOffset(id, req.Topic, req.Offset); err != nil {
		return nil, ErrCommitOffset(err)
	}
	return &protocol.CommitOffsetResponse{Success: true}, nil
}

func (s *RpcServer) FetchOffset(ctx context.Context, req *protocol.FetchOffsetRequest) (*protocol.FetchOffsetResponse, error) {
	if req.Topic == "" {
		return nil, ErrTopicRequired
	}

	id := req.Id
	if id == "" {
		id = "default"
	}

	if err := s.consumerManager.Recover(); err != nil {
		return nil, ErrRecoverOffsets(err)
	}

	off, err := s.consumerManager.GetOffset(id, req.Topic)
	if err != nil {
		return &protocol.FetchOffsetResponse{Offset: 0}, nil
	}
	return &protocol.FetchOffsetResponse{Offset: off}, nil
}
