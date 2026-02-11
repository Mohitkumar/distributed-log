package client

import (
	"context"
	"net"
	"strings"
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
		// When running the consumer outside Docker but the cluster is inside Docker,
		// FindLeader may return hostnames like "node1:9092" that are only resolvable
		// inside the Docker network. If we see a DNS error for such a hostname,
		// fall back to dialing 127.0.0.1:<port>, which works with typical port mappings.
		if strings.Contains(err.Error(), "no such host") {
			if host, port, splitErr := net.SplitHostPort(addr); splitErr == nil && strings.HasPrefix(host, "node") && port != "" {
				fallback := net.JoinHostPort("127.0.0.1", port)
				if tc2, err2 := transport.Dial(fallback); err2 == nil {
					return &ConsumerClient{tc: tc2}, nil
				}
			}
		}
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
			// When the requested offset is beyond the current end/high watermark of the log,
			// the server returns an error like "offset X out of range" or "beyond high watermark".
			// Treat this as "no new data yet": wait briefly and retry instead of failing the stream.
			msg := err.Error()
			if strings.Contains(msg, "out of range") || strings.Contains(msg, "beyond high watermark") {
				time.Sleep(100 * time.Millisecond)
				continue
			}
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
