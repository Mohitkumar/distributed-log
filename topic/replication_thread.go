package topic

import (
	"context"
	"time"

	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/protocol"
)

const (
	replicateTopicsMaxRounds    = 100
	defaultReplicationBatchSize = 5000
)

func (c *TopicManager) startReplicationThread() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopReplication:
			return
		case <-ticker.C:
			c.replicateAllTopics()
		}
	}
}

func (c *TopicManager) replicateAllTopics() {
	c.mu.RLock()
	target := c.replicationTarget
	c.mu.RUnlock()
	if target == nil {
		return
	}

	leaderToTopics := make(map[string][]string)
	for _, info := range target.ListReplicaTopics() {
		leaderToTopics[info.LeaderNodeID] = append(leaderToTopics[info.LeaderNodeID], info.TopicName)
	}

	ctx := context.Background()
	for leaderID, topicNames := range leaderToTopics {
		_ = c.ReplicateTopicsForLeader(ctx, leaderID, topicNames)
	}
}

// ReplicateTopicsForLeader performs pipelined replication for the given topics from one leader.
func (c *Coordinator) ReplicateTopicsForLeader(ctx context.Context, leaderNodeID string, topicNames []string) error {
	c.mu.RLock()
	target := c.replicationTarget
	c.mu.RUnlock()
	if target == nil {
		return nil
	}
	currentNodeID := ""
	if n := c.GetCurrentNode(); n != nil {
		currentNodeID = n.NodeID
	}
	batchSize := c.cfg.Replication.BatchSize
	if batchSize == 0 {
		batchSize = defaultReplicationBatchSize
	}
	return DoReplicateTopicsForLeader(ctx, target, c.GetReplicationClient, currentNodeID, leaderNodeID, topicNames, batchSize)
}

func DoReplicateTopicsForLeader(
	ctx context.Context,
	target topic.ReplicationTarget,
	getClient func(string) (*client.RemoteClient, error),
	currentNodeID string,
	leaderNodeID string,
	topicNames []string,
	batchSize uint32,
) error {
	if len(topicNames) == 0 {
		return nil
	}
	if batchSize == 0 {
		batchSize = defaultReplicationBatchSize
	}
	replClient, err := getClient(leaderNodeID)
	if err != nil {
		return err
	}

	names := append([]string(nil), topicNames...)
	for round := 0; round < replicateTopicsMaxRounds && len(names) > 0; round++ {
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
				_ = target.ReportLEO(ctx, topicName, leaderNodeID)
			} else {
				names = append(names, topicName)
			}
		}
	}
	return nil
}
