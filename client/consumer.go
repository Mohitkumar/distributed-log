package client

import (
	"context"
	"time"

	"github.com/mohitkumar/mlog/protocol"
	"github.com/mohitkumar/mlog/transport"
)

type ConsumerClient struct {
	tc *transport.TransportClient
}

func NewConsumerClient(addr string) (*ConsumerClient, error) {
	tc, err := transport.Dial(addr)
	if err != nil {
		return nil, err
	}
	return &ConsumerClient{tc: tc}, nil
}

func (c *ConsumerClient) Close() error {
	return c.tc.Close()
}

func (c *ConsumerClient) Fetch(ctx context.Context, req *protocol.FetchRequest) (*protocol.FetchResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.FetchResponse)
	return &r, nil
}

func (c *ConsumerClient) FetchStream(ctx context.Context, req *protocol.FetchRequest) (FetchStreamReader, error) {
	return &fetchStreamReader{tc: c.tc, req: *req}, nil
}

type FetchStreamReader interface {
	Recv() (*protocol.FetchResponse, error)
}

type fetchStreamReader struct {
	tc  *transport.TransportClient
	req protocol.FetchRequest
}

func (r *fetchStreamReader) Recv() (*protocol.FetchResponse, error) {
	for {
		resp, err := r.tc.Call(r.req)
		if err != nil {
			return nil, err
		}
		rep := resp.(protocol.FetchResponse)
		if rep.Entry != nil {
			r.req.Offset = rep.Entry.Offset + 1
			return &rep, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
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
