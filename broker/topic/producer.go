package topic

import (
	"context"
	"time"

	"github.com/mohitkumar/mlog/api/protocol"
	"go.uber.org/zap"
)

// This file holds the produce-side of TopicManager: appending records to a topic's
// leader log and, for ACK_ALL, waiting for replicas to catch up. It stays in package
// topic (rather than a separate package) because it needs Topic's internal locking
// (topic_entry.go) to read Log/Replicas safely — pulling it out further would mean
// exposing that synchronization outside the package. Consumer-side concerns (offset
// tracking) have no such coupling and live in their own broker/consumer package.

// HandleProduce appends to the topic log (leader only). For ACK_ALL, waits for replicas to catch up.
func (tm *TopicManager) HandleProduce(ctx context.Context, t *Topic, logEntry *protocol.LogEntry, acks protocol.AckMode) (uint64, error) {
	l := t.GetLog()
	offset, err := l.Append(logEntry.Value)
	if err != nil {
		return 0, err
	}
	switch acks {
	case protocol.AckLeader:
		return offset, nil
	case protocol.AckAll:
		if err := tm.waitForAllFollowersToCatchUp(ctx, t, offset); err != nil {
			return 0, ErrWaitFollowersCatchUp(err)
		}
		return offset, nil
	default:
		return 0, ErrInvalidAckModef(int32(acks))
	}
}

// HandleProduceBatch appends multiple records (leader only).
func (tm *TopicManager) HandleProduceBatch(ctx context.Context, t *Topic, values [][]byte, acks protocol.AckMode) (uint64, uint64, error) {
	if len(values) == 0 {
		return 0, 0, ErrValuesEmpty
	}

	l := t.GetLog()
	base, err := l.AppendBatch(values)
	if err != nil {
		return 0, 0, err
	}
	last := base + uint64(len(values)) - 1

	switch acks {
	case protocol.AckLeader:
		return base, last, nil
	case protocol.AckAll:
		if err := tm.waitForAllFollowersToCatchUp(ctx, t, last); err != nil {
			return 0, 0, ErrWaitFollowersCatchUp(err)
		}
		return base, last, nil
	default:
		return 0, 0, ErrInvalidAckModef(int32(acks))
	}
}

func (tm *TopicManager) waitForAllFollowersToCatchUp(ctx context.Context, t *Topic, offset uint64) error {
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	requiredLEO := offset + 1

	for {
		var replicas []protocol.ReplicaInfo
		if info, ok := tm.coordinator.TopicInfo(t.Name); ok {
			replicas = info.Replicas
		}

		useISR := false
		for _, r := range replicas {
			if r.IsISR {
				useISR = true
				break
			}
		}

		allCaughtUp := true
		candidates := 0
		for _, replica := range replicas {
			if useISR && !replica.IsISR {
				continue
			}
			candidates++
			if uint64(replica.LEO) < requiredLEO {
				allCaughtUp = false
				break
			}
		}

		if candidates == 0 {
			return nil
		}
		if allCaughtUp {
			return nil
		}

		select {
		case <-ticker.C:
			continue
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			if t.Logger != nil {
				t.Logger.Warn("followers catch-up timeout", zap.String("topic", t.Name), zap.Uint64("required_offset", offset))
			}
			return ErrTimeoutCatchUp
		}
	}
}
