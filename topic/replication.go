package topic

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mohitkumar/mlog/protocol"
)

const (
	ReplicateTopicsMaxRounds    = 100
	DefaultReplicationBatchSize = 5000
)

// ReplicaTopicInfo describes a topic a node replicates from a leader.
type ReplicaTopicInfo struct {
	TopicName    string
	LeaderNodeID string
}

// StartReplicationThread starts the replication loop (TopicManager owns the replication thread).
func (tm *TopicManager) StartReplicationThread() {
	tm.mu.Lock()
	if tm.stopReplication != nil {
		tm.mu.Unlock()
		return
	}
	tm.stopReplication = make(chan struct{})
	tm.mu.Unlock()
	go tm.runReplicationThread()
}

// StopReplicationThread stops the replication thread (e.g. on shutdown).
func (tm *TopicManager) StopReplicationThread() {
	tm.mu.Lock()
	ch := tm.stopReplication
	tm.stopReplication = nil
	tm.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

func (tm *TopicManager) runReplicationThread() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		tm.mu.RLock()
		stop := tm.stopReplication
		tm.mu.RUnlock()
		if stop == nil {
			return
		}
		select {
		case <-stop:
			return
		case <-ticker.C:
			tm.replicateAllTopics()
		}
	}
}

func (tm *TopicManager) replicateAllTopics() {
	leaderToTopics := make(map[string][]string)
	for _, info := range tm.ListReplicaTopics() {
		leaderToTopics[info.LeaderNodeID] = append(leaderToTopics[info.LeaderNodeID], info.TopicName)
	}
	ctx := context.Background()
	batchSize := tm.replicationBatchSize
	if batchSize == 0 {
		batchSize = DefaultReplicationBatchSize
	}
	for leaderID, topicNames := range leaderToTopics {
		_ = DoReplicateTopicsForLeader(ctx, tm, tm.CurrentNodeID, leaderID, topicNames, batchSize)
	}
}

// DoReplicateTopicsForLeader replicates topicNames from leaderNodeID using the consumer FetchBatch API.
// The consumer client is used with ReplicaNodeID set so the leader uses ReadUncommitted and records replica LEO.
func DoReplicateTopicsForLeader(
	ctx context.Context,
	topicMgr *TopicManager,
	currentNodeID string,
	leaderNodeID string,
	topicNames []string,
	batchSize uint32,
) error {
	if len(topicNames) == 0 {
		return nil
	}
	if batchSize == 0 {
		batchSize = DefaultReplicationBatchSize
	}
	consumerClient, err := topicMgr.GetConsumerClient(leaderNodeID)
	if err != nil {
		return err
	}
	consumerClient.SetReplicaNodeID(currentNodeID)
	defer consumerClient.SetReplicaNodeID("")

	names := append([]string(nil), topicNames...)
	for round := 0; round < ReplicateTopicsMaxRounds && len(names) > 0; round++ {
		stillReplicating := names[:0]
		for _, topicName := range names {
			leo, ok := topicMgr.GetLEO(topicName)
			if !ok {
				continue
			}
			req := &protocol.FetchBatchRequest{
				Topic:    topicName,
				Id:       fmt.Sprintf("replicate-%s-%s", currentNodeID, leaderNodeID),
				Offset:   leo,
				MaxCount: batchSize,
			}
			resp, err := consumerClient.FetchBatch(ctx, req)
			if err != nil {
				var rpcErr *protocol.RPCError
				if errors.As(err, &rpcErr) && rpcErr.Code == protocol.CodeReadOffset {
					continue
				}
				if protocol.ShouldReconnect(err) {
					topicMgr.InvalidateConsumerClient(leaderNodeID)
				}
				stillReplicating = append(stillReplicating, topicName)
				continue
			}
			applyErr := false
			for _, entry := range resp.Entries {
				if entry == nil {
					continue
				}
				if err := topicMgr.ApplyRecord(topicName, entry.Value); err != nil {
					stillReplicating = append(stillReplicating, topicName)
					applyErr = true
					break
				}
			}
			if applyErr {
				continue
			}
			if uint32(len(resp.Entries)) >= batchSize {
				stillReplicating = append(stillReplicating, topicName)
			}
		}
		names = stillReplicating
	}
	return nil
}
