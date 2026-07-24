package topic

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mohitkumar/mlog/api/protocol"
	"github.com/mohitkumar/mlog/consumer/client"
	"go.uber.org/zap"
)

const (
	DefaultReplicationBatchSize = 5000
	replicationTickInterval     = 1 * time.Second
	// reconcileTickInterval drives reconcileLocalTopics (topic.go): opening/closing
	// local logs in reaction to cluster metadata changes (create/delete/leader-change).
	// Deliberately much faster than replicationTickInterval — it's a cheap in-memory
	// diff against cluster metadata, only touching disk when something actually
	// changed, and callers producing/consuming right after a create rely on this
	// window being small (see client.RetryTopicNotReady, which bounds its retry
	// budget assuming this tick rate). Reconciliation runs on its own goroutine (see
	// runReconcileLoop/runReplicateLoop below), independent of replication, precisely
	// so a slow or unreachable replication peer can never delay it.
	reconcileTickInterval = 50 * time.Millisecond
)

// ReplicaTopicInfo describes a topic this node replicates from a leader.
type ReplicaTopicInfo struct {
	TopicName    string
	LeaderNodeID string
}

// StartReplicationThread starts the background loops that both replicate from leaders
// (ListReplicaTopics/replicateAllTopics) and reconcile local topic state against
// cluster metadata (reconcileLocalTopics) — every node needs this running, not just
// replicas, since leader-side log opening is also driven by it now.
func (tm *TopicManager) StartReplicationThread() {
	tm.mu.Lock()
	if tm.replicationCancel != nil {
		tm.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	tm.replicationCancel = cancel
	tm.mu.Unlock()
	go tm.runReplicationThread(ctx)
}

// StopReplicationThread stops the replication/reconcile loops (e.g. on shutdown).
func (tm *TopicManager) StopReplicationThread() {
	tm.mu.Lock()
	cancel := tm.replicationCancel
	tm.replicationCancel = nil
	tm.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// runReplicationThread runs reconciliation and replication on independent goroutines
// (rather than sharing one select loop) so a slow or unreachable replication peer —
// replicateAllTopics can legitimately block on network I/O — never delays reconcile,
// which callers rely on staying fast (see reconcileTickInterval).
func (tm *TopicManager) runReplicationThread(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		tm.runReconcileLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		tm.runReplicateLoop(ctx)
	}()
	wg.Wait()
	tm.replConns.closeAll()
}

func (tm *TopicManager) runReconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(reconcileTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tm.reconcileLocalTopics()
		}
	}
}

func (tm *TopicManager) runReplicateLoop(ctx context.Context) {
	ticker := time.NewTicker(replicationTickInterval)
	defer ticker.Stop()
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

// replicationConnCache keeps one ConsumerClient per leader alive across replication
// ticks instead of dialing and tearing down a fresh TCP connection every tick (every
// leader, every second) — real, avoidable handshake overhead for what's usually a
// steady-state relationship. Entries are only replaced when a call actually reports a
// reconnect-worthy error (see ReplicateFromLeader), not proactively.
type replicationConnCache struct {
	mu      sync.Mutex
	clients map[string]*client.ConsumerClient
}

func newReplicationConnCache() *replicationConnCache {
	return &replicationConnCache{clients: make(map[string]*client.ConsumerClient)}
}

// get returns the cached connection for leaderID, dialing and caching one if absent.
func (c *replicationConnCache) get(leaderID, rpcAddr, currentNodeID string) (*client.ConsumerClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cc, ok := c.clients[leaderID]; ok {
		return cc, nil
	}
	cc, err := client.NewConsumerClient(rpcAddr)
	if err != nil {
		return nil, err
	}
	cc.SetReplicaNodeID(currentNodeID)
	// This loop already retries at tick granularity (abort this leader, try again next
	// tick — see runReplicateLoop/replicateAllTopics); the client's own
	// retry-on-topic-not-ready would just stack another retry budget on top.
	cc.DisableTopicNotReadyRetry()
	c.clients[leaderID] = cc
	return cc, nil
}

// invalidate drops and closes the cached connection for leaderID, so the next get
// redials — used when a call reports the connection/leader is actually gone.
func (c *replicationConnCache) invalidate(leaderID string) {
	c.mu.Lock()
	cc := c.clients[leaderID]
	delete(c.clients, leaderID)
	c.mu.Unlock()
	if cc != nil {
		_ = cc.Close()
	}
}

// closeAll closes every cached connection — called when replication stops.
func (c *replicationConnCache) closeAll() {
	c.mu.Lock()
	clients := c.clients
	c.clients = make(map[string]*client.ConsumerClient)
	c.mu.Unlock()
	for _, cc := range clients {
		_ = cc.Close()
	}
}

// ReplicateFromLeader reuses (or opens) this node's cached connection to leaderID,
// fetches batches for each topic, and applies them locally.
func (tm *TopicManager) ReplicateFromLeader(ctx context.Context, leaderID string, topicNames []string, batchSize uint32) error {
	// Look up leader RPC address.
	rpcAddr, ok := tm.coordinator.NodeRPCAddr(leaderID)
	if !ok {
		return fmt.Errorf("leader node %s not found", leaderID)
	}

	cc, err := tm.replConns.get(leaderID, rpcAddr, tm.currentNodeID())
	if err != nil {
		return fmt.Errorf("connect to leader %s at %s: %w", leaderID, rpcAddr, err)
	}

	consumerID := fmt.Sprintf("replicate-%s-%s", tm.currentNodeID(), leaderID)

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
					// Connection gone — invalidate it so the next tick redials, and
					// abort this leader entirely for now; next tick will retry.
					tm.replConns.invalidate(leaderID)
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
