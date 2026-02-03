package rpc

import (
	"context"

	"github.com/mohitkumar/mlog/protocol"
)

// CreateReplica creates a new replica for a topic on this broker
func (s *RpcServer) CreateReplica(ctx context.Context, req *protocol.CreateReplicaRequest) (*protocol.CreateReplicaResponse, error) {
	if req.Topic == "" {
		return nil, ErrTopicRequired
	}

	if req.LeaderAddr == "" {
		return nil, ErrLeaderAddrRequired
	}

	err := s.topicManager.CreateReplicaRemote(req.Topic, req.LeaderAddr)
	if err != nil {
		return nil, ErrCreateReplica(err)
	}

	return &protocol.CreateReplicaResponse{
		Topic: req.Topic,
	}, nil
}

// DeleteReplica deletes a replica for a topic
func (s *RpcServer) DeleteReplica(ctx context.Context, req *protocol.DeleteReplicaRequest) (*protocol.DeleteReplicaResponse, error) {
	if req.Topic == "" {
		return nil, ErrTopicRequired
	}
	err := s.topicManager.DeleteReplicaRemote(req.Topic)
	if err != nil {
		return nil, ErrDeleteReplica(err)
	}

	return &protocol.DeleteReplicaResponse{}, nil
}
