package coordinator

import (
	"context"
	"time"

	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/protocol"
	"github.com/mohitkumar/mlog/topic"
)

const replicateTopicsMaxRounds = 100

func (c *Coordinator) startReplicationThread() {
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

func (c *Coordinator) replicateAllTopics() {
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
	return DoReplicateTopicsForLeader(ctx, target, c.GetReplicationClient, currentNodeID, leaderNodeID, topicNames)
}

// DoReplicateTopicsForLeader runs one leader's pipelined replication (used by Coordinator and by test fakes).
// It sends one ReplicateRequest per topic on the same connection, reads responses in order, applies chunks via target.
// Repeats until all topics report EndOfStream or max rounds.
func DoReplicateTopicsForLeader(
	ctx context.Context,
	target topic.ReplicationTarget,
	getClient func(string) (*client.RemoteClient, error),
	currentNodeID string,
	leaderNodeID string,
	topicNames []string,
) error {
	if len(topicNames) == 0 {
		return nil
	}
	replClient, err := getClient(leaderNodeID)
	if err != nil {
		return err
	}

	names := append([]string(nil), topicNames...)
	for round := 0; round < replicateTopicsMaxRounds && len(names) > 0; round++ {
		var requests []protocol.ReplicateRequest
		var topicOrder []string
		for _, name := range names {
			leo, ok := target.GetLEO(name)
			if !ok {
				continue
			}
			requests = append(requests, protocol.ReplicateRequest{
				Topic:         name,
				Offset:        leo,
				BatchSize:     5000,
				ReplicaNodeID: currentNodeID,
			})
			topicOrder = append(topicOrder, name)
		}
		if len(requests) == 0 {
			break
		}

		responses, err := replClient.ReplicatePipeline(requests)
		if err != nil {
			return err
		}

		names = names[:0]
		for i, resp := range responses {
			if i >= len(topicOrder) {
				break
			}
			topicName := topicOrder[i]
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
