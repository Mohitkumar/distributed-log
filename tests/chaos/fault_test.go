//go:build chaos

// Package chaos holds fault-injection and linearizability tests: real 3-node clusters
// with nodes hard-killed/restarted mid-workload (see tests.RealTestServer.Kill/Restart)
// and Porcupine-checked histories (porcupine_test.go). Gated behind the "chaos" build
// tag — these are slower and noisier than the default suite (real Serf failure
// detection takes real wall-clock time), so `go test ./...` skips them; run explicitly
// with `go test -tags chaos ./chaos/...`.
package chaos

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/api/protocol"
	"github.com/mohitkumar/mlog/client"
	producerclient "github.com/mohitkumar/mlog/producer/client"
	"github.com/mohitkumar/mlog/tests"
)

// nodeByID returns the server with the given node ID among the three real cluster nodes.
func nodeByID(nodes []*tests.RealTestServer, id string) *tests.RealTestServer {
	for _, n := range nodes {
		if n.NodeID == id {
			return n
		}
	}
	return nil
}

// bootstrapAddrs returns the RPC addresses of nodes, for producer/consumer client bootstrap.
func bootstrapAddrs(nodes []*tests.RealTestServer) []string {
	addrs := make([]string, len(nodes))
	for i, n := range nodes {
		addrs[i] = n.Addr
	}
	return addrs
}

// waitForTopicLeaderID polls until topic has a leader assigned other than excludeNodeID
// (per any live node's own Coordinator.TopicLeaderNodeID), or fails the test after
// timeout. Pass excludeNodeID="" to just wait for any leader (e.g. right after
// CreateTopic). Used after killing the leader so the test observes the cluster having
// actually completed Raft voter removal + leader reassignment (see
// topic.TopicManager.ReassignLeadersForDeadNode), not just guessed at timing.
func waitForTopicLeaderID(t testing.TB, nodes []*tests.RealTestServer, topicName, excludeNodeID string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n == nil || n.Coordinator == nil {
				continue
			}
			leaderID, ok := n.Coordinator.TopicLeaderNodeID(topicName)
			if ok && leaderID != "" && leaderID != excludeNodeID {
				return leaderID
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("no leader for topic %q (other than %q) within %s", topicName, excludeNodeID, timeout)
	return ""
}

// waitForNodeRemoved polls until deadNodeID is no longer a Raft voter, per every other
// still-alive node's own view (i.e. Serf's failure detector has observed the kill and
// the cluster has reacted — see Cluster.IsNodeAlive), or fails the test after timeout.
// Restarting a node before its departure has actually been observed races the survivors'
// gossip/consensus state (the rejoining Serf instance can collide with a not-yet-reaped
// membership entry for the same name/address) — this mirrors the timing a real process
// supervisor gets for free by virtue of needing to notice the crash before restarting.
func waitForNodeRemoved(t testing.TB, nodes []*tests.RealTestServer, deadNodeID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		removed, checked := true, false
		for _, n := range nodes {
			if n == nil || n.Coordinator == nil || n.NodeID == deadNodeID {
				continue
			}
			checked = true
			if n.Coordinator.IsNodeAlive(deadNodeID) {
				removed = false
			}
		}
		if checked && removed {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("node %q was not removed as a raft voter within %s", deadNodeID, timeout)
}

// TestFault_KillLeaderDuringProduce hard-kills the current topic leader mid-stream and
// verifies: a new leader is elected among the survivors, the producer client (which
// transparently reconnects on failover) recovers, and every message it received an
// AckAll acknowledgment for survives on the new leader — no acked write is lost.
func TestFault_KillLeaderDuringProduce(t *testing.T) {
	node1, node2, node3, cleanup := tests.StartRealThreeNodeCluster(t, "fault-leader-kill")
	defer cleanup()
	nodes := []*tests.RealTestServer{node1, node2, node3}

	ctx := context.Background()
	topicName := "fault-leader-kill-topic"

	remoteClient, err := client.NewRemoteClient(node1.Addr)
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	createResp, err := remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 2, // leader + 2 replicas, so it survives any single node loss
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	remoteClient.Close()
	t.Logf("topic created, replicas=%v", createResp.ReplicaNodeIds)

	pc, err := producerclient.NewClient(ctx, bootstrapAddrs(nodes), topicName)
	if err != nil {
		t.Fatalf("producerclient.NewClient: %v", err)
	}
	defer pc.Close()

	var acked []string

	// Produce a first batch against the original leader.
	for i := 0; i < 10; i++ {
		val := "pre-kill-" + strconv.Itoa(i)
		if _, err := pc.Send(ctx, []byte(val), protocol.AckAll); err != nil {
			t.Fatalf("Send pre-kill %d: %v", i, err)
		}
		acked = append(acked, val)
	}

	leaderID := pc.LeaderAddr()
	var deadNode *tests.RealTestServer
	for _, n := range nodes {
		if n.Addr == leaderID {
			deadNode = n
			break
		}
	}
	if deadNode == nil {
		t.Fatalf("could not resolve producer's leader addr %q to a node", leaderID)
	}
	t.Logf("killing leader node %s (%s)", deadNode.NodeID, deadNode.Addr)
	if err := deadNode.Kill(); err != nil {
		t.Fatalf("Kill %s: %v", deadNode.NodeID, err)
	}

	// Continue producing through the kill; Client.Send transparently reconnects to
	// whichever node becomes the new leader once Serf's failure detector and Raft
	// voter removal complete (see topic.TopicManager.ReassignLeadersForDeadNode).
	sendCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for i := 0; i < 10; i++ {
		val := "post-kill-" + strconv.Itoa(i)
		if _, err := pc.Send(sendCtx, []byte(val), protocol.AckAll); err != nil {
			t.Fatalf("Send post-kill %d: %v", i, err)
		}
		acked = append(acked, val)
	}
	t.Logf("producer recovered, new leader=%s", pc.LeaderAddr())

	newLeaderID := waitForTopicLeaderID(t, nodes, topicName, deadNode.NodeID, 30*time.Second)
	if newLeaderID == deadNode.NodeID {
		t.Fatalf("new leader is still the dead node %q", deadNode.NodeID)
	}

	// Verify every acked message survived, in order, by reading the new leader's log directly.
	newLeader := nodeByID(nodes, newLeaderID)
	if newLeader == nil {
		t.Fatalf("could not find surviving node for new leader %q", newLeaderID)
	}
	l, err := newLeader.TopicManager.GetLog(topicName)
	if err != nil {
		t.Fatalf("GetLog on new leader %s: %v", newLeaderID, err)
	}
	if l.LEO() < uint64(len(acked)) {
		t.Fatalf("new leader LEO=%d, want at least %d (all acked messages)", l.LEO(), len(acked))
	}
	for i, want := range acked {
		entry, err := l.ReadUncommitted(uint64(i))
		if err != nil {
			t.Fatalf("ReadUncommitted offset %d: %v", i, err)
		}
		const offWidth = 8
		got := string(entry[offWidth:])
		if got != want {
			t.Fatalf("offset %d: expected %q, got %q — acked message lost or reordered after leader kill", i, want, got)
		}
	}
	t.Logf("✓ all %d AckAll messages survived leader kill", len(acked))
}

// TestFault_KillAndRestartFollower verifies a follower that's hard-killed and later
// restarted rejoins the cluster and catches back up to the leader's log end offset from
// its own on-disk state, without needing to be re-added by hand.
func TestFault_KillAndRestartFollower(t *testing.T) {
	node1, node2, node3, cleanup := tests.StartRealThreeNodeCluster(t, "fault-follower-restart")
	defer cleanup()
	nodes := []*tests.RealTestServer{node1, node2, node3}

	ctx := context.Background()
	topicName := "fault-follower-restart-topic"

	remoteClient, err := client.NewRemoteClient(node1.Addr)
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	if _, err := remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 2,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	remoteClient.Close()

	leaderID := waitForTopicLeaderID(t, nodes, topicName, "", 15*time.Second)
	leaderNode := nodeByID(nodes, leaderID)
	if leaderNode == nil {
		t.Fatalf("could not find leader node %q", leaderID)
	}
	var followerNode *tests.RealTestServer
	for _, n := range nodes {
		if n.NodeID != leaderID {
			followerNode = n
			break
		}
	}
	if followerNode == nil {
		t.Fatalf("could not find a follower node")
	}

	producerClient, err := producerclient.NewProducerClient(leaderNode.Addr)
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()

	for i := 0; i < 5; i++ {
		if _, err := producerClient.Produce(ctx, &protocol.ProduceRequest{
			Topic: topicName,
			Value: []byte("pre-kill-" + strconv.Itoa(i)),
			Acks:  protocol.AckLeader,
		}); err != nil {
			t.Fatalf("Produce pre-kill %d: %v", i, err)
		}
	}

	t.Logf("killing follower node %s", followerNode.NodeID)
	if err := followerNode.Kill(); err != nil {
		t.Fatalf("Kill %s: %v", followerNode.NodeID, err)
	}

	// Leader keeps taking writes (AckLeader, so it doesn't need the dead follower) while it's down.
	for i := 5; i < 10; i++ {
		if _, err := producerClient.Produce(ctx, &protocol.ProduceRequest{
			Topic: topicName,
			Value: []byte("during-kill-" + strconv.Itoa(i)),
			Acks:  protocol.AckLeader,
		}); err != nil {
			t.Fatalf("Produce during-kill %d: %v", i, err)
		}
	}

	leaderLog, err := leaderNode.TopicManager.GetLog(topicName)
	if err != nil {
		t.Fatalf("GetLog on leader: %v", err)
	}
	targetLEO := leaderLog.LEO()

	waitForNodeRemoved(t, nodes, followerNode.NodeID, 30*time.Second)
	t.Logf("restarting follower node %s", followerNode.NodeID)
	followerNode.Restart(t)

	deadline := time.Now().Add(30 * time.Second)
	for {
		l, err := followerNode.TopicManager.GetLog(topicName)
		if err == nil && l != nil && l.LEO() >= targetLEO {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("follower did not catch up to LEO=%d within timeout", targetLEO)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("✓ restarted follower %s caught up to LEO=%d", followerNode.NodeID, targetLEO)
}

// TestFault_KillFollowerLoop repeats a follower kill/restart cycle a few times with
// ongoing production, as a lightweight seeded-chaos loop: it doesn't assert anything new
// beyond TestFault_KillAndRestartFollower, but exercises Kill/Restart back-to-back on the
// same node, which is where lifecycle bugs (stuck goroutines, stale registrations) tend
// to surface that a single kill/restart wouldn't catch.
func TestFault_KillFollowerLoop(t *testing.T) {
	node1, node2, node3, cleanup := tests.StartRealThreeNodeCluster(t, "fault-follower-loop")
	defer cleanup()
	nodes := []*tests.RealTestServer{node1, node2, node3}

	ctx := context.Background()
	topicName := "fault-follower-loop-topic"

	remoteClient, err := client.NewRemoteClient(node1.Addr)
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	if _, err := remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 2,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	remoteClient.Close()

	leaderID := waitForTopicLeaderID(t, nodes, topicName, "", 15*time.Second)
	leaderNode := nodeByID(nodes, leaderID)
	var followerNode *tests.RealTestServer
	for _, n := range nodes {
		if n.NodeID != leaderID {
			followerNode = n
			break
		}
	}

	producerClient, err := producerclient.NewProducerClient(leaderNode.Addr)
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()

	msgIdx := 0
	produceN := func(n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if _, err := producerClient.Produce(ctx, &protocol.ProduceRequest{
				Topic: topicName,
				Value: []byte(fmt.Sprintf("msg-%d", msgIdx)),
				Acks:  protocol.AckLeader,
			}); err != nil {
				t.Fatalf("Produce msg-%d: %v", msgIdx, err)
			}
			msgIdx++
		}
	}

	for round := 0; round < 3; round++ {
		produceN(5)

		if err := followerNode.Kill(); err != nil {
			t.Fatalf("round %d: Kill: %v", round, err)
		}
		produceN(5)

		leaderLog, err := leaderNode.TopicManager.GetLog(topicName)
		if err != nil {
			t.Fatalf("round %d: GetLog on leader: %v", round, err)
		}
		targetLEO := leaderLog.LEO()

		waitForNodeRemoved(t, nodes, followerNode.NodeID, 30*time.Second)
		followerNode.Restart(t)

		deadline := time.Now().Add(30 * time.Second)
		for {
			l, err := followerNode.TopicManager.GetLog(topicName)
			if err == nil && l != nil && l.LEO() >= targetLEO {
				break
			}
			if !time.Now().Before(deadline) {
				t.Fatalf("round %d: follower did not catch up to LEO=%d within timeout", round, targetLEO)
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Logf("round %d: follower caught up to LEO=%d", round, targetLEO)
	}
}
