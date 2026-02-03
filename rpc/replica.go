package rpc

import (
	"context"
	"fmt"

	"github.com/mohitkumar/mlog/protocol"
)

// CreateReplica creates a new replica for a topic on this broker
func (s *RpcServer) CreateReplica(ctx context.Context, req *protocol.CreateReplicaRequest) (*protocol.CreateReplicaResponse, error) {
	if req.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}

	if req.LeaderAddr == "" {
		return nil, fmt.Errorf("leader_addr is required")
	}

	err := s.topicManager.CreateReplicaRemote(req.Topic, req.LeaderAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create replica: %w", err)
	}

	return &protocol.CreateReplicaResponse{
		Topic: req.Topic,
	}, nil
}

// DeleteReplica deletes a replica for a topic
func (s *RpcServer) DeleteReplica(ctx context.Context, req *protocol.DeleteReplicaRequest) (*protocol.DeleteReplicaResponse, error) {
	if req.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}
	err := s.topicManager.DeleteReplicaRemote(req.Topic)
	if err != nil {
		return nil, fmt.Errorf("failed to delete replica: %w", err)
	}

	return &protocol.DeleteReplicaResponse{}, nil
}
