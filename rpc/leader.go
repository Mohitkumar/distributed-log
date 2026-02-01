package rpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/mohitkumar/mlog/protocol"
)

// CreateTopic creates a new topic with the specified replica count
func (s *RpcServer) CreateTopic(ctx context.Context, req *protocol.CreateTopicRequest) (*protocol.CreateTopicResponse, error) {
	if req.Topic == "" {
		return nil, fmt.Errorf("topic name is required")
	}
	if req.ReplicaCount < 1 {
		return nil, fmt.Errorf("replica count must be at least 1")
	}

	err := s.topicManager.CreateTopic(req.Topic, int(req.ReplicaCount))
	if err != nil {
		return nil, fmt.Errorf("failed to create topic: %w", err)
	}

	return &protocol.CreateTopicResponse{
		Topic: req.Topic,
	}, nil
}

// DeleteTopic deletes a topic
func (s *RpcServer) DeleteTopic(ctx context.Context, req *protocol.DeleteTopicRequest) (*protocol.DeleteTopicResponse, error) {
	if req.Topic == "" {
		return nil, fmt.Errorf("topic name is required")
	}

	err := s.topicManager.DeleteTopic(req.Topic)
	if err != nil {
		return nil, fmt.Errorf("failed to delete topic: %w", err)
	}

	return &protocol.DeleteTopicResponse{
		Topic: req.Topic,
	}, nil
}

// RecordLEO records the Log End Offset (LEO) of a replica
func (s *RpcServer) RecordLEO(ctx context.Context, req *protocol.RecordLEORequest) (*protocol.RecordLEOResponse, error) {
	if req.ReplicaId == "" {
		return nil, fmt.Errorf("replica_id is required")
	}

	topicObj, err := s.topicManager.GetTopic(req.Topic)
	if err != nil {
		return nil, fmt.Errorf("topic %s not found: %w", req.Topic, err)
	}

	err = topicObj.RecordLEORemote(req.ReplicaId, uint64(req.Leo), time.Now())
	if err != nil {
		return nil, fmt.Errorf("failed to record LEO for replica %s in topic %s: %w", req.ReplicaId, req.Topic, err)
	}

	return &protocol.RecordLEOResponse{}, nil
}

// handleReplicateStream runs on a persistent connection: reads from the leader log using ReaderFrom(offset)
// and streams raw segment-format bytes to the replica until caught up, then sends EndOfStream.
func (s *RpcServer) handleReplicateStream(ctx context.Context, msg any, conn net.Conn, codec *protocol.Codec) error {
	req := msg.(protocol.ReplicateRequest)
	leaderView, err := s.topicManager.GetLeader(req.Topic)
	if err != nil {
		return err
	}
	reader, err := leaderView.Log.ReaderFrom(req.Offset)
	if err != nil {
		return err
	}
	const chunkSize = 64 * 1024 // 64KB
	buf := make([]byte, chunkSize)
	for {
		n, err := reader.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			if err := codec.Encode(conn, &protocol.ReplicateResponse{RawChunk: chunk, EndOfStream: false}); err != nil {
				return err
			}
		}
		if err == io.EOF || n == 0 {
			if err := codec.Encode(conn, &protocol.ReplicateResponse{RawChunk: nil, EndOfStream: true}); err != nil {
				return err
			}
			return nil
		}
	}
}
