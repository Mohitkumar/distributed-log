package client

import (
	"context"

	"github.com/mohitkumar/mlog/api/protocol"
	"github.com/mohitkumar/mlog/api/transport"
	toplevelclient "github.com/mohitkumar/mlog/client"
)

type ConsumerClient struct {
	tc            *transport.TransportClient
	ReplicaNodeID string // when set, Fetch uses read-uncommitted (replication); otherwise read up to HW
	noRetry       bool
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

// DisableTopicNotReadyRetry turns off Fetch/FetchBatch's automatic retry on
// CodeTopicNotFound/CodeNotTopicLeader. Internal replication (topic.ReplicateFromLeader)
// already retries at a coarser granularity — abort this leader, let the next replication
// tick try again — so the two would otherwise stack: a stalled leader could add the full
// client-side retry budget as extra blocking latency inside a single replication tick on
// top of the tick-level retry that already handles it.
func (c *ConsumerClient) DisableTopicNotReadyRetry() {
	c.noRetry = true
}

func (c *ConsumerClient) Fetch(ctx context.Context, req *protocol.FetchRequest) (*protocol.FetchResponse, error) {
	reqCopy := *req
	if c.ReplicaNodeID != "" {
		reqCopy.ReplicaNodeID = c.ReplicaNodeID
	}
	var resp protocol.FetchResponse
	call := func() error {
		r, err := c.tc.Call(reqCopy)
		if err != nil {
			return err
		}
		resp = r.(protocol.FetchResponse)
		return nil
	}
	var err error
	if c.noRetry {
		err = call()
	} else {
		err = toplevelclient.RetryTopicNotReady(ctx, call)
	}
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *ConsumerClient) FetchBatch(ctx context.Context, req *protocol.FetchBatchRequest) (*protocol.FetchBatchResponse, error) {
	reqCopy := *req
	if c.ReplicaNodeID != "" {
		reqCopy.ReplicaNodeID = c.ReplicaNodeID
	}
	var resp protocol.FetchBatchResponse
	call := func() error {
		r, err := c.tc.Call(reqCopy)
		if err != nil {
			return err
		}
		resp = r.(protocol.FetchBatchResponse)
		return nil
	}
	var err error
	if c.noRetry {
		err = call()
	} else {
		err = toplevelclient.RetryTopicNotReady(ctx, call)
	}
	if err != nil {
		return nil, err
	}
	return &resp, nil
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
