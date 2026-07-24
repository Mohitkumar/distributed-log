package client

import (
	"context"
	"time"

	"github.com/mohitkumar/mlog/api/protocol"
)

const (
	topicNotReadyBackoff = 50 * time.Millisecond
	topicNotReadyMaxWait = 2 * time.Second
)

// RetryTopicNotReady retries fn while it fails with protocol.IsTopicNotReady — the
// brief window after CreateTopic (or a leader change) before the target node's own
// periodic metadata reconciliation has opened the local log — backing off between
// attempts, until fn succeeds, ctx is done, or the retry budget is spent. Any other
// error (including a genuinely nonexistent topic, which looks identical from the
// client's perspective) is returned once that budget runs out, same tradeoff Kafka's
// own producer/consumer clients make with max.block.ms.
func RetryTopicNotReady(ctx context.Context, fn func() error) error {
	deadline := time.Now().Add(topicNotReadyMaxWait)
	for {
		err := fn()
		if err == nil || !protocol.IsTopicNotReady(err) || !time.Now().Before(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(topicNotReadyBackoff):
		}
	}
}
