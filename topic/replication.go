package topic

import (
	"context"
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

// DoReplicateTopicsForLeader runs pipelined replication for topicNames from leaderNodeID.
func DoReplicateTopicsForLeader(
	ctx context.Context,
	target *TopicManager,
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
	replClient, err := target.GetReplicationClient(leaderNodeID)
	if err != nil {
		return err
	}
	names := append([]string(nil), topicNames...)
	for round := 0; round < ReplicateTopicsMaxRounds && len(names) > 0; round++ {
		var requests []protocol.ReplicateRequest
		for _, name := range names {
			leo, ok := target.GetLEO(name)
			if !ok {
				continue
			}
			requests = append(requests, protocol.ReplicateRequest{
				Topic:         name,
				Offset:        leo,
				BatchSize:     batchSize,
				ReplicaNodeID: currentNodeID,
			})
		}
		if len(requests) == 0 {
			break
		}
		responses, err := replClient.ReplicatePipeline(requests)
		if err != nil {
			return err
		}
		names = names[:0]
		for _, resp := range responses {
			topicName := resp.Topic
			if topicName == "" {
				continue
			}
			if len(resp.RawChunk) > 0 {
				_ = target.ApplyChunk(topicName, resp.RawChunk)
			}
			if resp.EndOfStream {
				if leo, ok := target.GetLEO(topicName); ok {
					_ = target.ReportLEOViaRaft(ctx, topicName, leo, uint64(resp.LeaderLEO))
				}
			} else {
				names = append(names, topicName)
			}
		}
	}
	return nil
}
