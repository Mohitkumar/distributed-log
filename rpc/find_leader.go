package rpc

import (
	"context"

	"github.com/mohitkumar/mlog/protocol"
)

// FindLeader returns the RPC address of the current leader for the requested topic.
// Any node can answer this using its coordinator metadata.
func (srv *RpcServer) FindLeader(ctx context.Context, req *protocol.FindLeaderRequest) (*protocol.FindLeaderResponse, error) {
	if req.Topic == "" {
		return nil, ErrTopicRequired
	}

	leaderAddr, err := srv.topicManager.GetTopicLeaderRPCAddr(req.Topic)
	if err != nil {
		return nil, ErrTopicNotFound(req.Topic, err)
	}

	return &protocol.FindLeaderResponse{
		LeaderAddr: leaderAddr,
	}, nil
}

// GetRaftLeader returns the RPC address of the current Raft (metadata) leader.
// Any node can answer; clients should send create-topic and other metadata ops to this address.
func (srv *RpcServer) GetRaftLeader(ctx context.Context, req *protocol.GetRaftLeaderRequest) (*protocol.GetRaftLeaderResponse, error) {
	addr, err := srv.topicManager.GetRaftLeaderRPCAddr()
	if err != nil {
		return nil, err
	}
	return &protocol.GetRaftLeaderResponse{RaftLeaderAddr: addr}, nil
}
