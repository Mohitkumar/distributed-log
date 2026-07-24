package topic

import (
	"context"
	"time"

	"github.com/mohitkumar/mlog/api/protocol"
	"github.com/mohitkumar/mlog/broker/log"
	"go.uber.org/zap"
)

// This file holds the produce-side of TopicManager: appending records to a topic's
// leader log and, for ACK_ALL, waiting for replicas to catch up. Consumer-side
// concerns (offset tracking) have no such coupling and live in their own
// broker/consumer package.

// HandleProduce appends to the topic log (leader only). For ACK_ALL, waits for replicas to catch up.
func (tm *TopicManager) HandleProduce(ctx context.Context, topicName string, l *log.LogManager, logEntry *protocol.LogEntry, acks protocol.AckMode) (uint64, error) {
	offset, err := l.Append(logEntry.Value)
	if err != nil {
		return 0, err
	}
	switch acks {
	case protocol.AckLeader:
		return offset, nil
	case protocol.AckAll:
		if err := tm.waitForAllFollowersToCatchUp(ctx, topicName, offset); err != nil {
			return 0, ErrWaitFollowersCatchUp(err)
		}
		return offset, nil
	default:
		return 0, ErrInvalidAckModef(int32(acks))
	}
}

// HandleProduceBatch appends multiple records (leader only).
func (tm *TopicManager) HandleProduceBatch(ctx context.Context, topicName string, l *log.LogManager, values [][]byte, acks protocol.AckMode) (uint64, uint64, error) {
	if len(values) == 0 {
		return 0, 0, ErrValuesEmpty
	}

	base, err := l.AppendBatch(values)
	if err != nil {
		return 0, 0, err
	}
	last := base + uint64(len(values)) - 1

	switch acks {
	case protocol.AckLeader:
		return base, last, nil
	case protocol.AckAll:
		if err := tm.waitForAllFollowersToCatchUp(ctx, topicName, last); err != nil {
			return 0, 0, ErrWaitFollowersCatchUp(err)
		}
		return base, last, nil
	default:
		return 0, 0, ErrInvalidAckModef(int32(acks))
	}
}

func (tm *TopicManager) waitForAllFollowersToCatchUp(ctx context.Context, topicName string, offset uint64) error {
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	requiredLEO := offset + 1

	for {
		var replicas []protocol.ReplicaInfo
		if info, ok := tm.coordinator.TopicInfo(topicName); ok {
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
			tm.Logger.Warn("followers catch-up timeout", zap.String("topic", topicName), zap.Uint64("required_offset", offset))
			return ErrTimeoutCatchUp
		}
	}
}
