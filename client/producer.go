package client

import (
	"context"

	"github.com/mohitkumar/mlog/protocol"
	"github.com/mohitkumar/mlog/transport"
)

// ProducerClient performs produce RPCs to a broker address (used from separate producer machines).
type ProducerClient struct {
	tc *transport.TransportClient
}

// NewProducerClient dials addr and returns a producer client. Call Close when done.
func NewProducerClient(addr string) (*ProducerClient, error) {
	tc, err := transport.Dial(addr)
	if err != nil {
		return nil, err
	}
	return &ProducerClient{tc: tc}, nil
}

// Close closes the transport connection.
func (c *ProducerClient) Close() error {
	return c.tc.Close()
}

func (c *ProducerClient) Produce(ctx context.Context, req *protocol.ProduceRequest) (*protocol.ProduceResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.ProduceResponse)
	return &r, nil
}

func (c *ProducerClient) ProduceBatch(ctx context.Context, req *protocol.ProduceBatchRequest) (*protocol.ProduceBatchResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.ProduceBatchResponse)
	return &r, nil
}
