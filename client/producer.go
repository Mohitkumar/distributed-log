package client

import (
	"context"
	"net"
	"strings"

	"github.com/mohitkumar/mlog/protocol"
	"github.com/mohitkumar/mlog/transport"
)

type ProducerClient struct {
	tc *transport.TransportClient
}

func NewProducerClient(addr string) (*ProducerClient, error) {
	tc, err := transport.Dial(addr)
	if err != nil {
		// When running the producer outside Docker but the cluster is inside Docker,
		// FindLeader may return hostnames like "node1:9094" that are only resolvable
		// inside the Docker network. If we see a DNS error for such a hostname,
		// fall back to dialing 127.0.0.1:<port>, which works with typical port mappings.
		if strings.Contains(err.Error(), "no such host") {
			if host, port, splitErr := net.SplitHostPort(addr); splitErr == nil && strings.HasPrefix(host, "node") && port != "" {
				fallback := net.JoinHostPort("127.0.0.1", port)
				if tc2, err2 := transport.Dial(fallback); err2 == nil {
					return &ProducerClient{tc: tc2}, nil
				}
			}
		}
		return nil, err
	}
	return &ProducerClient{tc: tc}, nil
}

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
