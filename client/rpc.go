package client

import (
	"context"

	"github.com/mohitkumar/mlog/protocol"
	"github.com/mohitkumar/mlog/transport"
)

type RemoteClient struct {
	tc *transport.TransportClient
}

func NewRemoteClient(addr string) (*RemoteClient, error) {
	tc, err := transport.Dial(addr)
	if err != nil {
		return nil, err
	}
	return &RemoteClient{tc: tc}, nil
}

func (c *RemoteClient) Close() error {
	return c.tc.Close()
}

func (c *RemoteClient) CreateReplica(ctx context.Context, req *protocol.CreateReplicaRequest) (*protocol.CreateReplicaResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.CreateReplicaResponse)
	return &r, nil
}

func (c *RemoteClient) DeleteReplica(ctx context.Context, req *protocol.DeleteReplicaRequest) (*protocol.DeleteReplicaResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.DeleteReplicaResponse)
	return &r, nil
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

func (c *RemoteClient) RecordLEO(ctx context.Context, req *protocol.RecordLEORequest) (*protocol.RecordLEOResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.RecordLEOResponse)
	return &r, nil
}

func (c *RemoteClient) ApplyDeleteTopicEvent(ctx context.Context, req *protocol.ApplyDeleteTopicEventRequest) (*protocol.ApplyDeleteTopicEventResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.ApplyDeleteTopicEventResponse)
	return &r, nil
}

func (c *RemoteClient) ApplyIsrUpdateEvent(ctx context.Context, req *protocol.ApplyIsrUpdateEventRequest) (*protocol.ApplyIsrUpdateEventResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.ApplyIsrUpdateEventResponse)
	return &r, nil
}

// FindLeader asks a node which RPC address is currently the leader for the given topic.
func (c *RemoteClient) FindLeader(ctx context.Context, req *protocol.FindLeaderRequest) (*protocol.FindLeaderResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.FindLeaderResponse)
	return &r, nil
}

func (c *RemoteClient) Replicate(ctx context.Context, req *protocol.ReplicateRequest) (*protocol.ReplicateResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.ReplicateResponse)
	return &r, nil
}

func (c *RemoteClient) ReplicatePipeline(requests []protocol.ReplicateRequest) ([]protocol.ReplicateResponse, error) {
	for i := range requests {
		if err := c.tc.Write(requests[i]); err != nil {
			return nil, err
		}
	}
	responses := make([]protocol.ReplicateResponse, len(requests))
	for i := range responses {
		resp, err := c.tc.ReadResponse()
		if err != nil {
			return nil, err
		}
		responses[i] = resp.(protocol.ReplicateResponse)
	}
	return responses, nil
}
