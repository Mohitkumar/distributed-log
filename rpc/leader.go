package rpc

import (
	"context"

	"github.com/mohitkumar/mlog/protocol"
)

func (s *RpcServer) CreateTopic(ctx context.Context, req *protocol.CreateTopicRequest) (*protocol.CreateTopicResponse, error) {
	if req.Topic == "" {
		return nil, ErrTopicNameRequired
	}
	return s.topicManager.CreateTopic(ctx, req)
}

func (s *RpcServer) DeleteTopic(ctx context.Context, req *protocol.DeleteTopicRequest) (*protocol.DeleteTopicResponse, error) {
	if req.Topic == "" {
		return nil, ErrTopicNameRequired
	}
	return s.topicManager.DeleteTopic(ctx, req)
}

func (srv *RpcServer) FindTopicLeader(ctx context.Context, req *protocol.FindTopicLeaderRequest) (*protocol.FindTopicLeaderResponse, error) {
	if req.Topic == "" {
		return nil, ErrTopicRequired
	}

	leaderAddr, err := srv.topicManager.GetTopicLeaderRPCAddr(req.Topic)
	if err != nil {
		return nil, ErrTopicNotFound(req.Topic, err)
	}

	return &protocol.FindTopicLeaderResponse{
		LeaderAddr: leaderAddr,
	}, nil
}

// GetRaftLeader returns the RPC address of the current Raft (metadata) leader.
// Any node can answer; clients should send create-topic and other metadata ops to this address.
func (srv *RpcServer) FindRaftLeader(ctx context.Context, req *protocol.FindRaftLeaderRequest) (*protocol.FindRaftLeaderResponse, error) {
	addr, err := srv.topicManager.GetRaftLeaderRPCAddr()
	if err != nil {
		return nil, err
	}
	return &protocol.FindRaftLeaderResponse{RaftLeaderAddr: addr}, nil
}
