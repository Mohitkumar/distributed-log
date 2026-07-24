package client

import (
	"context"
	"fmt"
	"time"

	"github.com/mohitkumar/mlog/api/protocol"
)

// ResolveTopicLeader returns a resolver that discovers topic's current leader RPC
// address by trying each of bootstrapAddrs in turn (see TryAddrs) — the leader lookup
// producer/client.Client and consumer/client.Client use to (re)connect.
func ResolveTopicLeader(bootstrapAddrs []string, topic string) func(ctx context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		return TryAddrs(ctx, bootstrapAddrs, func(c *RemoteClient) (string, error) {
			resp, err := c.FindTopicLeader(ctx, &protocol.FindTopicLeaderRequest{Topic: topic})
			if err != nil {
				return "", err
			}
			if resp.LeaderAddr == "" {
				return "", fmt.Errorf("empty leader address for topic %s", topic)
			}
			return resp.LeaderAddr, nil
		})
	}
}

// ReconnectBackoff calls connect repeatedly, backing off linearly (attempt * 500ms)
// between tries, until it succeeds, ctx is done, or maxAttempts is reached. This is the
// shared "leader moved, redial" retry policy for producer/client.Client and
// consumer/client.Client — the same shape both CLIs used to hand-roll independently.
func ReconnectBackoff(ctx context.Context, maxAttempts int, connect func() error) error {
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := connect(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 500 * time.Millisecond):
		}
	}
	return fmt.Errorf("reconnect failed after %d attempts: %w", maxAttempts, lastErr)
}
