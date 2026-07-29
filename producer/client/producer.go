package client

import (
	"context"

	"github.com/mohitkumar/mlog/api/protocol"
	"github.com/mohitkumar/mlog/api/transport"
	toplevelclient "github.com/mohitkumar/mlog/client"
)

type ProducerClient struct {
	tc *transport.TransportClient
}

func NewProducerClient(addr string) (*ProducerClient, error) {
	tc, err := transport.DialWithFallback(addr)
	if err != nil {
		return nil, err
	}
	return &ProducerClient{tc: tc}, nil
}

func (c *ProducerClient) Close() error {
	return c.tc.Close()
}

func (c *ProducerClient) Produce(ctx context.Context, req *protocol.ProduceRequest) (*protocol.ProduceResponse, error) {
	var resp protocol.ProduceResponse
	err := toplevelclient.RetryTopicNotReady(ctx, func() error {
		r, err := c.tc.Call(*req)
		if err != nil {
			return err
		}
		resp = r.(protocol.ProduceResponse)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *ProducerClient) ProduceBatch(ctx context.Context, req *protocol.ProduceBatchRequest) (*protocol.ProduceBatchResponse, error) {
	var resp protocol.ProduceBatchResponse
	err := toplevelclient.RetryTopicNotReady(ctx, func() error {
		r, err := c.tc.Call(*req)
		if err != nil {
			return err
		}
		resp = r.(protocol.ProduceBatchResponse)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}
