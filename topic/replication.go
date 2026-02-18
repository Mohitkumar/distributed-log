package topic

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/protocol"
	"go.uber.org/zap"
)

const (
	DefaultReplicationBatchSize = 5000
	replicationTickInterval     = 1 * time.Second
)

// ReplicaTopicInfo describes a topic this node replicates from a leader.
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
	ticker := time.NewTicker(replicationTickInterval)
	defer ticker.Stop()

	tm.mu.RLock()
	stop := tm.stopReplication
	tm.mu.RUnlock()

	// Derive a cancellable context from the stop channel.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-stop
		cancel()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tm.replicateAllTopics(ctx)
		}
	}
}

// replicateAllTopics launches one goroutine per leader and waits for all to finish.
func (tm *TopicManager) replicateAllTopics(ctx context.Context) {
	leaderToTopics := make(map[string][]string)
	for _, info := range tm.ListReplicaTopics() {
		leaderToTopics[info.LeaderNodeID] = append(leaderToTopics[info.LeaderNodeID], info.TopicName)
	}
	if len(leaderToTopics) == 0 {
		return
	}

	batchSize := tm.replicationBatchSize
	if batchSize == 0 {
		batchSize = DefaultReplicationBatchSize
	}

	var wg sync.WaitGroup
	for leaderID, topicNames := range leaderToTopics {
		wg.Add(1)
		go func(leaderID string, topicNames []string) {
			defer wg.Done()
			if err := tm.ReplicateFromLeader(ctx, leaderID, topicNames, batchSize); err != nil {
				tm.Logger.Warn("replication from leader failed",
					zap.String("leader_id", leaderID),
					zap.Error(err),
				)
			}
		}(leaderID, topicNames)
	}
	wg.Wait()
}

// ReplicateFromLeader creates a dedicated consumer client for this leader,
// fetches batches for each topic, and applies them locally.
func (tm *TopicManager) ReplicateFromLeader(ctx context.Context, leaderID string, topicNames []string, batchSize uint32) error {
	// Look up leader RPC address.
	tm.mu.RLock()
	node := tm.Nodes[leaderID]
	tm.mu.RUnlock()
	if node == nil {
		return fmt.Errorf("leader node %s not found", leaderID)
	}

	// Create a dedicated client for this replication goroutine (not shared).
	cc, err := client.NewConsumerClient(node.RpcAddr)
	if err != nil {
		return fmt.Errorf("connect to leader %s at %s: %w", leaderID, node.RpcAddr, err)
	}
	defer cc.Close()
	cc.SetReplicaNodeID(tm.CurrentNodeID)

	consumerID := fmt.Sprintf("replicate-%s-%s", tm.CurrentNodeID, leaderID)

	// Fetch each topic in a loop. A topic is done when FetchBatch returns fewer than batchSize entries.
	// No multi-round outer loop needed: we keep fetching until each topic is caught up or ctx is cancelled.
	pending := make([]string, len(topicNames))
	copy(pending, topicNames)

	for len(pending) > 0 {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		next := make([]string, 0, len(pending))
		for _, topicName := range pending {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			leo, ok := tm.GetLEO(topicName)
			if !ok {
				continue
			}

			resp, err := cc.FetchBatch(ctx, &protocol.FetchBatchRequest{
				Topic:    topicName,
				Id:       consumerID,
				Offset:   leo,
				MaxCount: batchSize,
			})
			if err != nil {
				var rpcErr *protocol.RPCError
				if errors.As(err, &rpcErr) && rpcErr.Code == protocol.CodeReadOffset {
					// Caught up — skip this topic.
					continue
				}
				if protocol.ShouldReconnect(err) {
					// Connection gone — abort this leader entirely; next tick will retry.
					return fmt.Errorf("connection lost to leader %s: %w", leaderID, err)
				}
				// Transient error — keep topic for next round.
				next = append(next, topicName)
				continue
			}

			if len(resp.Entries) > 0 {
				values := make([][]byte, 0, len(resp.Entries))
				for _, entry := range resp.Entries {
					if entry != nil {
						values = append(values, entry.Value)
					}
				}
				if err := tm.ApplyRecordBatch(topicName, values); err != nil {
					next = append(next, topicName)
					continue
				}
			}

			// If we got a full batch, there's likely more data — keep fetching.
			if uint32(len(resp.Entries)) >= batchSize {
				next = append(next, topicName)
			}
		}
		pending = next
	}
	return nil
}
