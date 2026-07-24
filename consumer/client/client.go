package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mohitkumar/mlog/api/protocol"
	toplevelclient "github.com/mohitkumar/mlog/client"
)

// Client is a topic-aware consumer: it discovers the current topic leader and
// reconnects automatically on failover or leader change, and hides the "no new data
// yet, keep waiting" loop behind Poll — mirrors how KafkaConsumer.poll(Duration) hides
// both connection/leader-tracking and the wait-for-data loop from the caller.
// ConsumerClient (which this wraps) already retries same-connection for the brief
// post-create/leader-change window (see ConsumerClient.Fetch); Client adds the second
// tier Kafka's client also has: reconnecting to a different broker when the leader
// actually moved or the connection died.
type Client struct {
	mu             sync.Mutex
	bootstrapAddrs []string
	topic, id      string
	cc             *ConsumerClient
	addr           string

	// OnReconnect, if set, is called with the new leader address after Client
	// transparently reconnects following a failover/leader change. Optional — purely
	// for callers (e.g. a CLI) that want to surface status; Client never requires it.
	OnReconnect func(addr string)
}

// NewClient resolves topic's current leader among bootstrapAddrs and connects to it.
// id identifies this consumer for offset commit/fetch.
func NewClient(ctx context.Context, bootstrapAddrs []string, topic, id string) (*Client, error) {
	c := &Client{bootstrapAddrs: bootstrapAddrs, topic: topic, id: id}
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
	cc, err := NewConsumerClient(addr)
	if err != nil {
		return err
	}
	c.mu.Lock()
	old := c.cc
	c.cc, c.addr = cc, addr
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

// Poll fetches the next record at offset. While the leader reports "caught up, nothing
// new yet" (CodeReadOffset), it sleeps pollInterval and retries. On failover or
// connection failure it reconnects and retries. Returns when a record is available,
// ctx is done, or a non-retriable error occurs.
func (c *Client) Poll(ctx context.Context, offset uint64, pollInterval time.Duration) (*protocol.LogEntry, error) {
	for {
		c.mu.Lock()
		cc := c.cc
		c.mu.Unlock()
		resp, err := cc.Fetch(ctx, &protocol.FetchRequest{Id: c.id, Topic: c.topic, Offset: offset})
		if err == nil {
			if resp.Entry != nil {
				return resp.Entry, nil
			}
			if werr := waitOrDone(ctx, pollInterval); werr != nil {
				return nil, werr
			}
			continue
		}

		var rpcErr *protocol.RPCError
		if errors.As(err, &rpcErr) && rpcErr.Code == protocol.CodeReadOffset {
			if werr := waitOrDone(ctx, pollInterval); werr != nil {
				return nil, werr
			}
			continue
		}

		if !toplevelclient.ShouldReconnect(err) {
			return nil, err
		}
		if rerr := c.reconnect(ctx); rerr != nil {
			return nil, fmt.Errorf("fetch failed (%v), reconnect failed: %w", err, rerr)
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
	}
}

// Commit commits offset for this consumer's id/topic, reconnecting and retrying once
// on failover or connection failure.
func (c *Client) Commit(ctx context.Context, offset uint64) error {
	for {
		c.mu.Lock()
		cc := c.cc
		c.mu.Unlock()
		_, err := cc.CommitOffset(ctx, &protocol.CommitOffsetRequest{Id: c.id, Topic: c.topic, Offset: offset})
		if err == nil {
			return nil
		}
		if !toplevelclient.ShouldReconnect(err) {
			return err
		}
		if rerr := c.reconnect(ctx); rerr != nil {
			return fmt.Errorf("commit offset failed (%v), reconnect failed: %w", err, rerr)
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	}
}

// FetchCommittedOffset returns the last committed offset for this consumer's id/topic.
func (c *Client) FetchCommittedOffset(ctx context.Context) (uint64, error) {
	for {
		c.mu.Lock()
		cc := c.cc
		c.mu.Unlock()
		resp, err := cc.FetchOffset(ctx, &protocol.FetchOffsetRequest{Id: c.id, Topic: c.topic})
		if err == nil {
			return resp.Offset, nil
		}
		if !toplevelclient.ShouldReconnect(err) {
			return 0, err
		}
		if rerr := c.reconnect(ctx); rerr != nil {
			return 0, fmt.Errorf("fetch offset failed (%v), reconnect failed: %w", err, rerr)
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
	}
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cc == nil {
		return nil
	}
	return c.cc.Close()
}

func waitOrDone(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
