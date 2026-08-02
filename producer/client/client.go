package client

import (
	"context"
	"fmt"
	"sync"

	"github.com/mohitkumar/mlog/api/protocol"
	toplevelclient "github.com/mohitkumar/mlog/client"
)

// Client is a topic-aware producer: it discovers the current topic leader and
// reconnects automatically on failover or leader change.
type Client struct {
	mu             sync.Mutex
	bootstrapAddrs []string
	topic          string
	pc             *ProducerClient
	addr           string

	// OnReconnect, if set, is called with the new leader address after Client
	// transparently reconnects following a failover/leader change. Optional — purely
	// for callers (e.g. a CLI) that want to surface status; Client never requires it.
	OnReconnect func(addr string)
}

// NewClient resolves topic's current leader among bootstrapAddrs and connects to it.
func NewClient(ctx context.Context, bootstrapAddrs []string, topic string) (*Client, error) {
	c := &Client{bootstrapAddrs: bootstrapAddrs, topic: topic}
	if err := c.connect(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// LeaderAddr returns the RPC address of the leader this client is currently connected to.
func (c *Client) LeaderAddr() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.addr
}

func (c *Client) connect(ctx context.Context) error {
	addr, err := toplevelclient.ResolveTopicLeader(c.bootstrapAddrs, c.topic)(ctx)
	if err != nil {
		return err
	}
	pc, err := NewProducerClient(addr)
	if err != nil {
		return err
	}
	c.mu.Lock()
	old := c.pc
	c.pc, c.addr = pc, addr
	c.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	return nil
}

func (c *Client) reconnect(ctx context.Context) error {
	if err := toplevelclient.ReconnectBackoff(ctx, 10, func() error { return c.connect(ctx) }); err != nil {
		return err
	}
	if c.OnReconnect != nil {
		c.OnReconnect(c.LeaderAddr())
	}
	return nil
}

// Send produces one record, transparently reconnecting to the new leader and retrying
// on failover or connection failure, until it succeeds or ctx is done.
func (c *Client) Send(ctx context.Context, value []byte, acks protocol.AckMode) (uint64, error) {
	for {
		c.mu.Lock()
		pc := c.pc
		c.mu.Unlock()
		resp, err := pc.Produce(ctx, &protocol.ProduceRequest{Topic: c.topic, Value: value, Acks: acks})
		if err == nil {
			return resp.Offset, nil
		}
		if !toplevelclient.ShouldReconnect(err) {
			return 0, err
		}
		if rerr := c.reconnect(ctx); rerr != nil {
			return 0, fmt.Errorf("produce failed (%v), reconnect failed: %w", err, rerr)
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
	}
}

// SendBatch produces multiple records, with the same reconnect-and-retry behavior as Send.
func (c *Client) SendBatch(ctx context.Context, values [][]byte, acks protocol.AckMode) (base, last uint64, err error) {
	for {
		c.mu.Lock()
		pc := c.pc
		c.mu.Unlock()
		resp, err := pc.ProduceBatch(ctx, &protocol.ProduceBatchRequest{Topic: c.topic, Values: values, Acks: acks})
		if err == nil {
			return resp.BaseOffset, resp.LastOffset, nil
		}
		if !toplevelclient.ShouldReconnect(err) {
			return 0, 0, err
		}
		if rerr := c.reconnect(ctx); rerr != nil {
			return 0, 0, fmt.Errorf("produce batch failed (%v), reconnect failed: %w", err, rerr)
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, 0, ctxErr
		}
	}
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pc == nil {
		return nil
	}
	return c.pc.Close()
}
