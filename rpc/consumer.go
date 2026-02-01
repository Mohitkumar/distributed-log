package rpc

import (
	"context"
	"fmt"
	"time"

	"github.com/mohitkumar/mlog/protocol"
)

func (s *RpcServer) Fetch(ctx context.Context, req *protocol.FetchRequest) (*protocol.FetchResponse, error) {
	if req.Topic == "" {
		return nil, fmt.Errorf("topic is required")
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
		return nil, fmt.Errorf("topic %s not found: %w", req.Topic, err)
	}

	raw, err := leaderNode.Log.Read(off)
	if err != nil {
		return nil, fmt.Errorf("failed to read offset %d: %w", off, err)
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

// FetchStream streams log entries to the client (used over transport with polling).
func (s *RpcServer) FetchStream(req *protocol.FetchRequest, send func(*protocol.FetchResponse) error) error {
	if req.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	id := req.Id
	if id == "" {
		id = "default"
	}

	leaderNode, err := s.topicManager.GetLeader(req.Topic)
	if err != nil {
		return fmt.Errorf("topic %s not found: %w", req.Topic, err)
	}

	off := req.Offset
	if off == 0 {
		if err := s.consumerManager.Recover(); err == nil {
			if cached, err := s.consumerManager.GetOffset(id, req.Topic); err == nil {
				off = cached
			}
		}
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		raw, err := leaderNode.Log.Read(off)
		if err != nil {
			<-ticker.C
			continue
		}
		const offWidth = 8
		if len(raw) >= offWidth {
			raw = raw[offWidth:]
		}
		if err := send(&protocol.FetchResponse{
			Entry: &protocol.LogEntry{Offset: off, Value: raw},
		}); err != nil {
			return err
		}
		off++
	}
}

func (s *RpcServer) CommitOffset(ctx context.Context, req *protocol.CommitOffsetRequest) (*protocol.CommitOffsetResponse, error) {
	if req.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}

	id := req.Id
	if id == "" {
		id = "default"
	}

	if err := s.consumerManager.CommitOffset(id, req.Topic, req.Offset); err != nil {
		return nil, fmt.Errorf("commit offset failed: %w", err)
	}
	return &protocol.CommitOffsetResponse{Success: true}, nil
}

func (s *RpcServer) FetchOffset(ctx context.Context, req *protocol.FetchOffsetRequest) (*protocol.FetchOffsetResponse, error) {
	if req.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}

	id := req.Id
	if id == "" {
		id = "default"
	}

	if err := s.consumerManager.Recover(); err != nil {
		return nil, fmt.Errorf("recover offsets failed: %w", err)
	}

	off, err := s.consumerManager.GetOffset(id, req.Topic)
	if err != nil {
		return &protocol.FetchOffsetResponse{Offset: 0}, nil
	}
	return &protocol.FetchOffsetResponse{Offset: off}, nil
}
