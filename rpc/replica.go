package rpc

import (
	"context"
	"fmt"

	"github.com/mohitkumar/mlog/protocol"
)

// CreateReplica creates a new replica for a topic on this broker
func (s *rpcServer) CreateReplica(ctx context.Context, req *protocol.CreateReplicaRequest) (*protocol.CreateReplicaResponse, error) {
	if req.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}
	if req.ReplicaId == "" {
		return nil, fmt.Errorf("replica_id is required")
	}
	if req.LeaderAddr == "" {
		return nil, fmt.Errorf("leader_addr is required")
	}

	err := s.topicManager.CreateReplicaRemote(req.Topic, req.ReplicaId, req.LeaderAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create replica: %w", err)
	}

	return &protocol.CreateReplicaResponse{
		ReplicaId: req.ReplicaId,
	}, nil
}

// DeleteReplica deletes a replica for a topic
func (s *rpcServer) DeleteReplica(ctx context.Context, req *protocol.DeleteReplicaRequest) (*protocol.DeleteReplicaResponse, error) {
	if req.ReplicaId == "" {
		return nil, fmt.Errorf("replica_id is required")
	}
	err := s.topicManager.DeleteReplicaRemote(req.Topic, req.ReplicaId)
	if err != nil {
		return nil, fmt.Errorf("failed to delete replica: %w", err)
	}

	return &protocol.DeleteReplicaResponse{}, nil
}
