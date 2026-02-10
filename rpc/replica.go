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

	if req.LeaderId == "" {
		return nil, ErrLeaderAddrRequired
	}

	err := s.topicManager.CreateReplicaRemote(req.Topic, req.LeaderId)
	if err != nil {
		return nil, ErrCreateReplica(err)
	}

	return &protocol.CreateReplicaResponse{
		Topic: req.Topic,
	}, nil
}

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
