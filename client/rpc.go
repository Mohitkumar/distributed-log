package client

import (
	"context"

	"github.com/mohitkumar/mlog/api/protocol"
	"github.com/mohitkumar/mlog/api/transport"
)

type RemoteClient struct {
	tc *transport.TransportClient
}

func NewRemoteClient(addr string) (*RemoteClient, error) {
	tc, err := transport.DialWithFallback(addr)
	if err != nil {
		return nil, err
	}
	return &RemoteClient{tc: tc}, nil
}

func (c *RemoteClient) Close() error {
	return c.tc.Close()
}

func (c *RemoteClient) CreateTopic(ctx context.Context, req *protocol.CreateTopicRequest) (*protocol.CreateTopicResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.CreateTopicResponse)
	return &r, nil
}

func (c *RemoteClient) DeleteTopic(ctx context.Context, req *protocol.DeleteTopicRequest) (*protocol.DeleteTopicResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.DeleteTopicResponse)
	return &r, nil
}

// FindLeader asks a node which RPC address is currently the leader for the given topic.
func (c *RemoteClient) FindTopicLeader(ctx context.Context, req *protocol.FindTopicLeaderRequest) (*protocol.FindTopicLeaderResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.FindTopicLeaderResponse)
	return &r, nil
}

// GetRaftLeader asks a node for the Raft (metadata) leader RPC address. Use this before create-topic.
func (c *RemoteClient) FindRaftLeader(ctx context.Context, req *protocol.FindRaftLeaderRequest) (*protocol.FindRaftLeaderResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.FindRaftLeaderResponse)
	return &r, nil
}

// ListTopics returns all topics with leader and replica info. Any node can answer.
func (c *RemoteClient) ListTopics(ctx context.Context, req *protocol.ListTopicsRequest) (*protocol.ListTopicsResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.ListTopicsResponse)
	return &r, nil
}
