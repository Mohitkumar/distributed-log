//go:build chaos

package chaos

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/api/protocol"
	"github.com/mohitkumar/mlog/client"
	consumerclient "github.com/mohitkumar/mlog/consumer/client"
	producerclient "github.com/mohitkumar/mlog/producer/client"
	"github.com/mohitkumar/mlog/tests"
)

const offsetPrefixWidth = 8

func createTopic(t testing.TB, node *tests.RealTestServer, topicName string, replicaCount uint32) protocol.CreateTopicResponse {
	t.Helper()
	rc, err := client.NewRemoteClient(node.Addr)
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	defer rc.Close()
	resp, err := rc.CreateTopic(context.Background(), &protocol.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: replicaCount,
	})
	if err != nil {
		t.Fatalf("CreateTopic %q: %v", topicName, err)
	}
	return *resp
}

func produceOrdered(t testing.TB, leaderAddr, topicName, prefix string, n int, acks protocol.AckMode) []string {
	t.Helper()
	pc, err := producerclient.NewProducerClient(leaderAddr)
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer pc.Close()
	values := make([]string, n)
	for i := 0; i < n; i++ {
		val := fmt.Sprintf("%s-%d", prefix, i)
		if _, err := pc.Produce(context.Background(), &protocol.ProduceRequest{
			Topic: topicName,
			Value: []byte(val),
			Acks:  acks,
		}); err != nil {
			t.Fatalf("Produce %s %d: %v", prefix, i, err)
		}
		values[i] = val
	}
	return values
}

// waitForFollowerLEO polls until node's local log for topicName reaches at least
// target, or fails after timeout. AckAll's wait (waitForAllFollowersToCatchUp) returns
// as soon as the leader has served the follower's Fetch response for the target
// offset, which can be a moment before that follower finishes appending it locally
// (ApplyRecordBatch) — so a direct log read immediately after an AckAll Produce
// returns can still race a follower's own on-disk write. Poll rather than assume.
func waitForFollowerLEO(t testing.TB, node *tests.RealTestServer, topicName string, target uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		l, err := node.TopicManager.GetLog(topicName)
		if err == nil && l != nil && l.LEO() >= target {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("node %s: log for %q did not reach LEO=%d within %s", node.NodeID, topicName, target, timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// readLogValues reads offsets [0,n) directly off node's on-disk log for topicName,
// bypassing HW (ReadUncommitted) — the ground truth for "what did replication actually
// write, in what order", independent of what a normal consumer would be allowed to see.
func readLogValues(t testing.TB, node *tests.RealTestServer, topicName string, n uint64) []string {
	t.Helper()
	l, err := node.TopicManager.GetLog(topicName)
	if err != nil {
		t.Fatalf("node %s: GetLog(%s): %v", node.NodeID, topicName, err)
	}
	out := make([]string, n)
	for i := uint64(0); i < n; i++ {
		entry, err := l.ReadUncommitted(i)
		if err != nil {
			t.Fatalf("node %s: ReadUncommitted(%s, %d): %v", node.NodeID, topicName, i, err)
		}
		if len(entry) < offsetPrefixWidth {
			t.Fatalf("node %s: offset %d: short read", node.NodeID, i)
		}
		out[i] = string(entry[offsetPrefixWidth:])
	}
	return out
}

func assertEqualValues(t testing.TB, context string, want, got []string) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("%s: count mismatch: want %d messages, got %d", context, len(want), len(got))
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("%s: offset %d: want %q, got %q — order/content diverged", context, i, want[i], got[i])
		}
	}
}

// topicInfo fetches topicName's cluster-metadata view from node, failing the test if the
// topic isn't known there.
func topicInfo(t testing.TB, node *tests.RealTestServer, topicName string) protocol.TopicInfo {
	t.Helper()
	info, ok := node.Coordinator.TopicInfo(topicName)
	if !ok {
		t.Fatalf("node %s: TopicInfo(%s): topic not found", node.NodeID, topicName)
	}
	return info
}

// waitForISR polls node's view of topicName until every nodeID in want reports IsISR,
// or fails after timeout. Used after healing a fault, since ISR membership only
// recovers once the replica actually resumes fetching and catches up (see
// topic.TopicManager.RecordReplicaLEOFromFetch) — not immediately on restart.
func waitForISR(t testing.TB, node *tests.RealTestServer, topicName string, want []string, timeout time.Duration) protocol.TopicInfo {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last protocol.TopicInfo
	for time.Now().Before(deadline) {
		last = topicInfo(t, node, topicName)
		isr := map[string]bool{}
		for _, r := range last.Replicas {
			if r.IsISR {
				isr[r.NodeID] = true
			}
		}
		allPresent := true
		for _, id := range want {
			if !isr[id] {
				allPresent = false
				break
			}
		}
		if allPresent {
			return last
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("node %s: ISR for %q never included all of %v within %s (last=%+v)", node.NodeID, topicName, want, timeout, last.Replicas)
	return last
}

// TestChaos_MultiTopicLeaderElection creates several topics spread across the cluster
// (replicaCount=2 each, so every topic's leader+replicas cover all 3 nodes), kills
// whichever node currently leads the most topics, and verifies: every topic the dead
// node led re-elects a live leader, every topic it didn't lead keeps its original
// leader untouched, and after the node is restarted and rejoins, all topics remain
// healthy (still have a valid, alive leader).
func TestChaos_MultiTopicLeaderElection(t *testing.T) {
	node1, node2, node3, cleanup := tests.StartRealThreeNodeCluster(t, "chaos-multitopic")
	defer cleanup()
	nodes := []*tests.RealTestServer{node1, node2, node3}

	topics := []string{"chaos-mt-a", "chaos-mt-b", "chaos-mt-c"}
	for _, topicName := range topics {
		createTopic(t, node1, topicName, 2)
	}

	leaderOf := make(map[string]string, len(topics))
	topicsLedBy := make(map[string][]string) // nodeID -> topics it leads
	for _, topicName := range topics {
		id := waitForTopicLeaderID(t, nodes, topicName, "", 15*time.Second)
		leaderOf[topicName] = id
		topicsLedBy[id] = append(topicsLedBy[id], topicName)
	}

	// Kill whichever node leads the most topics, so this exercises at least one
	// re-election (and, with 3 topics on 3 nodes, likely more than one).
	var deadNodeID string
	for id, ts := range topicsLedBy {
		if len(ts) > len(topicsLedBy[deadNodeID]) {
			deadNodeID = id
		}
	}
	deadNode := nodeByID(nodes, deadNodeID)
	ledTopics := topicsLedBy[deadNodeID]
	t.Logf("killing node %s, which leads topics %v", deadNodeID, ledTopics)
	if err := deadNode.Kill(); err != nil {
		t.Fatalf("Kill %s: %v", deadNodeID, err)
	}

	ledSet := map[string]bool{}
	for _, tn := range ledTopics {
		ledSet[tn] = true
	}
	for _, topicName := range topics {
		if ledSet[topicName] {
			newID := waitForTopicLeaderID(t, nodes, topicName, deadNodeID, 30*time.Second)
			if newID == deadNodeID {
				t.Fatalf("topic %q: still shows dead node %q as leader", topicName, deadNodeID)
			}
			t.Logf("topic %q: re-elected leader %s (was %s)", topicName, newID, deadNodeID)
		} else {
			// Topic wasn't led by the dead node — its leader must be untouched.
			survivor := nodeByID(nodes, leaderOf[topicName])
			id, ok := survivor.Coordinator.TopicLeaderNodeID(topicName)
			if !ok || id != leaderOf[topicName] {
				t.Fatalf("topic %q: leader changed from %q to %q without its leader dying", topicName, leaderOf[topicName], id)
			}
		}
	}

	waitForNodeRemoved(t, nodes, deadNodeID, 30*time.Second)
	deadNode.Restart(t)

	// After healing, every topic should still resolve to a live leader.
	deadline := time.Now().Add(30 * time.Second)
	for _, topicName := range topics {
		for {
			id, ok := node1.Coordinator.TopicLeaderNodeID(topicName)
			if ok && id != "" {
				break
			}
			if !time.Now().Before(deadline) {
				t.Fatalf("topic %q: no leader after node %s healed", topicName, deadNodeID)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	t.Logf("✓ all %d topics have a live leader after node %s healed", len(topics), deadNodeID)
}

// TestChaos_FollowerFaultIntegrity produces to a topic, checks the full set of
// invariants (order/count in every follower's log, leader-reported LEO, ISR membership,
// and HW) both before and after a follower is hard-killed and later restarted.
func TestChaos_FollowerFaultIntegrity(t *testing.T) {
	node1, node2, node3, cleanup := tests.StartRealThreeNodeCluster(t, "chaos-follower-integrity")
	defer cleanup()
	nodes := []*tests.RealTestServer{node1, node2, node3}

	topicName := "chaos-integrity-topic"
	createTopic(t, node1, topicName, 2)

	leaderID := waitForTopicLeaderID(t, nodes, topicName, "", 15*time.Second)
	leader := nodeByID(nodes, leaderID)
	var followers []*tests.RealTestServer
	for _, n := range nodes {
		if n.NodeID != leaderID {
			followers = append(followers, n)
		}
	}

	// --- Baseline: produce a batch, verify order/count/LEO/ISR/HW while healthy. ---
	const batchA = 30
	valuesA := produceOrdered(t, leader.Addr, topicName, "a", batchA, protocol.AckAll)

	leaderLog, err := leader.TopicManager.GetLog(topicName)
	if err != nil {
		t.Fatalf("GetLog on leader: %v", err)
	}
	if got := leaderLog.LEO(); got != uint64(batchA) {
		t.Fatalf("leader LEO = %d, want %d", got, batchA)
	}
	for _, f := range followers {
		waitForFollowerLEO(t, f, topicName, uint64(batchA), 10*time.Second)
		got := readLogValues(t, f, topicName, uint64(batchA))
		assertEqualValues(t, fmt.Sprintf("follower %s log content (baseline)", f.NodeID), valuesA, got)
		fLog, err := f.TopicManager.GetLog(topicName)
		if err != nil {
			t.Fatalf("GetLog on follower %s: %v", f.NodeID, err)
		}
		if fLog.LEO() != uint64(batchA) {
			t.Fatalf("follower %s LEO = %d, want %d", f.NodeID, fLog.LEO(), batchA)
		}
	}

	followerIDs := []string{followers[0].NodeID, followers[1].NodeID}
	info := waitForISR(t, leader, topicName, followerIDs, 15*time.Second)
	for _, r := range info.Replicas {
		if r.IsISR && uint64(r.LEO) != uint64(batchA) {
			t.Fatalf("metadata LEO for ISR replica %s = %d, want %d", r.NodeID, r.LEO, batchA)
		}
	}
	wantHW := leader.Coordinator.TopicMinISRLeo(topicName, leaderLog.LEO())
	if wantHW != uint64(batchA) {
		t.Fatalf("computed HW = %d, want %d (all replicas caught up)", wantHW, batchA)
	}
	if got := leaderLog.HighWatermark(); got != wantHW {
		t.Fatalf("leader stored HW = %d, want %d (matches TopicMinISRLeo)", got, wantHW)
	}
	t.Logf("baseline OK: LEO=%d HW=%d ISR=%v", batchA, wantHW, followerIDs)

	// --- Kill one follower, keep producing (AckLeader — the dead follower is still
	// flagged ISR at this point, and AckAll would wait on it; see below). ---
	// Shorten ISRLagTime so the leader's periodic ExpireStaleISR (broker/topic/topic.go,
	// ticks every replicationTickInterval=1s) demotes the dead follower quickly instead
	// of waiting out the production DefaultISRLagTime (30s, matching Kafka's
	// replica.lag.time.max.ms default) — same mechanism, just tuned for a fast test.
	leader.TopicManager.ISRLagTime = 2 * time.Second
	deadFollower := followers[0]
	survivor := followers[1]
	t.Logf("killing follower %s", deadFollower.NodeID)
	if err := deadFollower.Kill(); err != nil {
		t.Fatalf("Kill %s: %v", deadFollower.NodeID, err)
	}

	const batchB = 20
	valuesB := produceOrdered(t, leader.Addr, topicName, "b", batchB, protocol.AckLeader)
	allValues := append(append([]string{}, valuesA...), valuesB...)
	total := uint64(batchA + batchB)

	if got := leaderLog.LEO(); got != total {
		t.Fatalf("leader LEO after batch B = %d, want %d", got, total)
	}

	// The survivor is still alive and fetching, so it catches up and its ISR LEO
	// should track the leader; the dead follower's ISR entry is now stale — nothing
	// re-evaluates it until it resumes fetching (RecordReplicaFetch only fires on an
	// actual replication Fetch), so ISR membership does not shrink just because a node
	// died. Documented here so a future change to that behavior shows up as a test
	// diff, not a surprise in production.
	deadline := time.Now().Add(15 * time.Second)
	for {
		l, err := survivor.TopicManager.GetLog(topicName)
		if err == nil && l != nil && l.LEO() >= total {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("survivor %s did not catch up to LEO=%d", survivor.NodeID, total)
		}
		time.Sleep(100 * time.Millisecond)
	}
	got := readLogValues(t, survivor, topicName, total)
	assertEqualValues(t, fmt.Sprintf("survivor %s log content (post-fault)", survivor.NodeID), allValues, got)

	infoAfter := topicInfo(t, leader, topicName)
	var deadReplicaLEO int64 = -1
	for _, r := range infoAfter.Replicas {
		if r.NodeID == deadFollower.NodeID {
			deadReplicaLEO = r.LEO
		}
	}
	t.Logf("post-kill metadata: %+v (dead follower %s last-known LEO=%d, leader LEO=%d)", infoAfter.Replicas, deadFollower.NodeID, deadReplicaLEO, total)

	// HW must reflect what a live majority (leader + survivor) actually has, not stay
	// pinned to the dead follower's frozen ISR entry — a topic shouldn't go read-only
	// for every consumer just because one of two followers crashed. Before
	// ExpireStaleISR this was permanently stuck; now it self-heals within
	// ~ISRLagTime + one replication tick, without needing the dead follower to restart.
	deadline = time.Now().Add(10 * time.Second)
	var midHW uint64
	for {
		midHW = leader.Coordinator.TopicMinISRLeo(topicName, leaderLog.LEO())
		if midHW >= total {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("HW stuck at %d (want %d): still pinned to dead follower %s's stale ISR entry (last-known LEO=%d) even though survivor %s and the leader both have all %d messages",
				midHW, total, deadFollower.NodeID, deadReplicaLEO, survivor.NodeID, total)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if got := leaderLog.HighWatermark(); got != midHW {
		t.Fatalf("leader stored HW = %d, want %d (matches TopicMinISRLeo) after ISR expiry", got, midHW)
	}
	postExpiry := topicInfo(t, leader, topicName)
	for _, r := range postExpiry.Replicas {
		if r.NodeID == deadFollower.NodeID && r.IsISR {
			t.Fatalf("dead follower %s still flagged ISR after ExpireStaleISR should have demoted it", deadFollower.NodeID)
		}
	}
	t.Logf("✓ HW self-healed to %d after dead follower %s expired from ISR", midHW, deadFollower.NodeID)

	// --- Restart the dead follower; it should rejoin, catch up, and ISR/HW should
	// fully recover to reflect reality again. ---
	waitForNodeRemoved(t, nodes, deadFollower.NodeID, 30*time.Second)
	deadFollower.Restart(t)

	deadline = time.Now().Add(30 * time.Second)
	for {
		l, err := deadFollower.TopicManager.GetLog(topicName)
		if err == nil && l != nil && l.LEO() >= total {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("restarted follower %s did not catch up to LEO=%d", deadFollower.NodeID, total)
		}
		time.Sleep(100 * time.Millisecond)
	}
	got = readLogValues(t, deadFollower, topicName, total)
	assertEqualValues(t, fmt.Sprintf("restarted follower %s log content", deadFollower.NodeID), allValues, got)

	finalInfo := waitForISR(t, leader, topicName, followerIDs, 30*time.Second)
	for _, r := range finalInfo.Replicas {
		if r.IsISR && uint64(r.LEO) != total {
			t.Fatalf("final metadata LEO for ISR replica %s = %d, want %d", r.NodeID, r.LEO, total)
		}
	}
	finalHW := leader.Coordinator.TopicMinISRLeo(topicName, leaderLog.LEO())
	if finalHW != total {
		t.Fatalf("final computed HW = %d, want %d after full recovery", finalHW, total)
	}
	if got := leaderLog.HighWatermark(); got != finalHW {
		t.Fatalf("leader stored HW = %d, want %d after full recovery", got, finalHW)
	}
	t.Logf("✓ full recovery OK: LEO=%d HW=%d ISR=%v", total, finalHW, followerIDs)
}

// TestChaos_ProducerConsumerFullLoopUnderLeaderKill runs a real producer and a real
// streaming consumer (both auto-reconnecting topic-aware clients) against a topic while
// the leader is hard-killed mid-stream, and verifies the consumer sees every message,
// in order, with no gaps or duplicates — end to end through the actual client/server
// wire protocol, not just direct log reads. Every successful Poll is already bounded by
// the server's HW check (LogManager.Read rejects offset>HW — see broker/rpc/consumer.go),
// so a consumer that reaches offset i is proof i was already inside the high watermark;
// this test's job is to confirm that guarantee survives a mid-stream leader failover.
func TestChaos_ProducerConsumerFullLoopUnderLeaderKill(t *testing.T) {
	node1, node2, node3, cleanup := tests.StartRealThreeNodeCluster(t, "chaos-full-loop")
	defer cleanup()
	nodes := []*tests.RealTestServer{node1, node2, node3}

	ctx := context.Background()
	topicName := "chaos-full-loop-topic"
	createTopic(t, node1, topicName, 2)

	pc, err := producerclient.NewClient(ctx, bootstrapAddrs(nodes), topicName)
	if err != nil {
		t.Fatalf("producerclient.NewClient: %v", err)
	}
	defer pc.Close()

	const total = 40
	const killAfter = 15

	produced := make([]string, 0, total)
	for i := 0; i < total; i++ {
		if i == killAfter {
			leaderID := pc.LeaderAddr()
			var dead *tests.RealTestServer
			for _, n := range nodes {
				if n.Addr == leaderID {
					dead = n
				}
			}
			if dead == nil {
				t.Fatalf("could not resolve producer's leader addr %q to a node", leaderID)
			}
			t.Logf("killing leader %s mid-stream at message %d", dead.NodeID, i)
			if err := dead.Kill(); err != nil {
				t.Fatalf("Kill: %v", err)
			}
		}
		val := fmt.Sprintf("full-loop-%d", i)
		sendCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		if _, err := pc.Send(sendCtx, []byte(val), protocol.AckAll); err != nil {
			cancel()
			t.Fatalf("Send %d: %v", i, err)
		}
		cancel()
		produced = append(produced, val)
	}

	cc, err := consumerclient.NewClient(ctx, bootstrapAddrs(nodes), topicName, "chaos-full-loop-consumer")
	if err != nil {
		t.Fatalf("consumerclient.NewClient: %v", err)
	}
	defer cc.Close()

	consumed := make([]string, 0, total)
	for offset := uint64(0); offset < total; offset++ {
		pollCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		entry, err := cc.Poll(pollCtx, offset, 50*time.Millisecond)
		cancel()
		if err != nil {
			t.Fatalf("Poll offset %d: %v", offset, err)
		}
		if entry.Offset != offset {
			t.Fatalf("Poll offset %d: got entry for offset %d instead", offset, entry.Offset)
		}
		consumed = append(consumed, string(entry.Value))
	}

	assertEqualValues(t, "consumer-observed order vs producer-observed order", produced, consumed)
	t.Logf("✓ consumer observed all %d messages in order despite mid-stream leader kill", total)
}
