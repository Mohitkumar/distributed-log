package client

import (
	"context"
	"net"
	"strings"

	"github.com/mohitkumar/mlog/protocol"
	"github.com/mohitkumar/mlog/transport"
)

type RemoteClient struct {
	tc *transport.TransportClient
}

func NewRemoteClient(addr string) (*RemoteClient, error) {
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
					return &RemoteClient{tc: tc2}, nil
				}
			}
		}
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
func (c *RemoteClient) FindLeader(ctx context.Context, req *protocol.FindLeaderRequest) (*protocol.FindLeaderResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.FindLeaderResponse)
	return &r, nil
}

// GetRaftLeader asks a node for the Raft (metadata) leader RPC address. Use this before create-topic.
func (c *RemoteClient) GetRaftLeader(ctx context.Context, req *protocol.GetRaftLeaderRequest) (*protocol.GetRaftLeaderResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.GetRaftLeaderResponse)
	return &r, nil
}

