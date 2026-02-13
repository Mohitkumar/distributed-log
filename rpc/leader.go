package rpc

import (
	"context"
	"fmt"

	"github.com/mohitkumar/mlog/protocol"
)

func (s *RpcServer) CreateTopic(ctx context.Context, req *protocol.CreateTopicRequest) (*protocol.CreateTopicResponse, error) {
	if req.Topic == "" {
		return nil, Err(protocol.CodeTopicNameRequired, "topic name is required")
	}
	resp, err := s.topicManager.CreateTopic(ctx, req)
	if err != nil {
		return nil, FromError(err)
	}
	return resp, nil
}

func (s *RpcServer) DeleteTopic(ctx context.Context, req *protocol.DeleteTopicRequest) (*protocol.DeleteTopicResponse, error) {
	if req.Topic == "" {
		return nil, Err(protocol.CodeTopicNameRequired, "topic name is required")
	}
	resp, err := s.topicManager.DeleteTopic(ctx, req)
	if err != nil {
		return nil, FromError(err)
	}
	return resp, nil
}

func (srv *RpcServer) FindTopicLeader(ctx context.Context, req *protocol.FindTopicLeaderRequest) (*protocol.FindTopicLeaderResponse, error) {
	if req.Topic == "" {
		return nil, Err(protocol.CodeTopicRequired, "topic is required")
	}

	leaderAddr, err := srv.topicManager.GetTopicLeaderRPCAddr(req.Topic)
	if err != nil {
		return nil, &protocol.RPCError{Code: protocol.CodeTopicNotFound, Message: fmt.Sprintf("topic %s not found: %v", req.Topic, err)}
	}

	return &protocol.FindTopicLeaderResponse{
		LeaderAddr: leaderAddr,
	}, nil
}

// FindRaftLeader returns the RPC address of the current Raft (metadata) leader.
// Any node can answer; clients should send create-topic and other metadata ops to this address.
func (srv *RpcServer) FindRaftLeader(ctx context.Context, req *protocol.FindRaftLeaderRequest) (*protocol.FindRaftLeaderResponse, error) {
	addr, err := srv.topicManager.GetRaftLeaderRPCAddr()
	if err != nil {
		return nil, &protocol.RPCError{Code: protocol.CodeRaftLeaderUnavailable, Message: err.Error()}
	}
	return &protocol.FindRaftLeaderResponse{RaftLeaderAddr: addr}, nil
}
