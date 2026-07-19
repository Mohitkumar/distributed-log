package client

import (
	"context"

	"github.com/mohitkumar/mlog/api/protocol"
	"github.com/mohitkumar/mlog/api/transport"
)

type ConsumerClient struct {
	tc            *transport.TransportClient
	ReplicaNodeID string // when set, Fetch uses read-uncommitted (replication); otherwise read up to HW
}

func NewConsumerClient(addr string) (*ConsumerClient, error) {
	tc, err := transport.DialWithFallback(addr)
	if err != nil {
		return nil, err
	}
	return &ConsumerClient{tc: tc}, nil
}

func (c *ConsumerClient) Close() error {
	return c.tc.Close()
}

// SetReplicaNodeID sets the client to replication mode: Fetch requests will include
// ReplicaNodeID so the leader uses ReadUncommitted and records replica LEO.
func (c *ConsumerClient) SetReplicaNodeID(id string) {
	c.ReplicaNodeID = id
}

func (c *ConsumerClient) Fetch(ctx context.Context, req *protocol.FetchRequest) (*protocol.FetchResponse, error) {
	reqCopy := *req
	if c.ReplicaNodeID != "" {
		reqCopy.ReplicaNodeID = c.ReplicaNodeID
	}
	resp, err := c.tc.Call(reqCopy)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.FetchResponse)
	return &r, nil
}

func (c *ConsumerClient) FetchBatch(ctx context.Context, req *protocol.FetchBatchRequest) (*protocol.FetchBatchResponse, error) {
	reqCopy := *req
	if c.ReplicaNodeID != "" {
		reqCopy.ReplicaNodeID = c.ReplicaNodeID
	}
	resp, err := c.tc.Call(reqCopy)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.FetchBatchResponse)
	return &r, nil
}

func (c *ConsumerClient) CommitOffset(ctx context.Context, req *protocol.CommitOffsetRequest) (*protocol.CommitOffsetResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.CommitOffsetResponse)
	return &r, nil
}

func (c *ConsumerClient) FetchOffset(ctx context.Context, req *protocol.FetchOffsetRequest) (*protocol.FetchOffsetResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.FetchOffsetResponse)
	return &r, nil
}
