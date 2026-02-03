package tests

import (
	"testing"
	"time"

	"github.com/mohitkumar/mlog/node"
	"github.com/mohitkumar/mlog/protocol"
)

// TestNode_TwoNodeCluster starts a 2-node cluster and tests node methods related to Raft.
func TestNode_TwoNodeCluster(t *testing.T) {
	leaderSrv, followerSrv := StartTwoNodes(t, "node-leader", "node-follower")
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	leader := leaderSrv.Node()
	follower := followerSrv.Node()
	if leader == nil || follower == nil {
		t.Fatal("expected non-nil nodes")
	}

	// Wait for one node to become Raft leader (bootstrap node may take a moment)
	var ok bool
	for i := 0; i < 100; i++ {
		if leader.IsLeader() || follower.IsLeader() {
			ok = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ok {
		t.Fatal("expected one node to become Raft leader within timeout")
	}

	// Exactly one node should be leader
	leaderIsLeader := leader.IsLeader()
	followerIsLeader := follower.IsLeader()
	if leaderIsLeader == followerIsLeader {
		t.Fatal("exactly one node should be Raft leader")
	}

	// Node IDs and addresses (node-1 / node-2 are fixed in StartTwoNodes)
	if got := leader.GetNodeID(); got != "node-1" {
		t.Errorf("leader GetNodeID() = %q, want node-1", got)
	}
	if got := follower.GetNodeID(); got != "node-2" {
		t.Errorf("follower GetNodeID() = %q, want node-2", got)
	}
	if leader.GetNodeAddr() != leaderSrv.Addr {
		t.Errorf("leader GetNodeAddr() = %q, want %q", leader.GetNodeAddr(), leaderSrv.Addr)
	}
	if follower.GetNodeAddr() != followerSrv.Addr {
		t.Errorf("follower GetNodeAddr() = %q, want %q", follower.GetNodeAddr(), followerSrv.Addr)
	}

	// GetOtherNodes: if Join succeeded in StartTwoNodes, leader should see node-2
	otherNodes := leader.GetOtherNodes()
	if len(otherNodes) >= 1 {
		found := false
		for _, n := range otherNodes {
			if n.NodeID == "node-2" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("leader GetOtherNodes() should contain node-2 when joined, got %v", otherNodes)
		}
	}

	// TopicExists: no topic yet
	if leader.TopicExists("no-such-topic") {
		t.Error("TopicExists(no-such-topic) should be false")
	}
}

// TestNode_ApplyCreateTopicEvent applies a CreateTopicEvent on the Raft leader and verifies the topic appears on both nodes.
func TestNode_ApplyCreateTopicEvent(t *testing.T) {
	leaderSrv, followerSrv := StartTwoNodes(t, "create-topic-leader", "create-topic-follower")
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	leaderNode := leaderSrv.Node()
	followerNode := followerSrv.Node()
	waitForLeader(t, leaderNode, followerNode)

	raftLeader := getLeaderNode(leaderSrv, followerSrv)
	if raftLeader == nil {
		t.Fatal("no Raft leader")
	}

	topicName := "test-apply-create-topic"
	ev := &protocol.MetadataEvent{
		CreateTopicEvent: &protocol.CreateTopicEvent{
			Topic:          topicName,
			ReplicaCount:   1,
			LeaderNodeID:   raftLeader.GetNodeID(),
			LeaderEpoch:    1,
			ReplicaNodeIds: []string{raftLeader.GetNodeID()},
		},
	}
	if err := raftLeader.ApplyCreateTopicEvent(ev); err != nil {
		t.Fatalf("ApplyCreateTopicEvent: %v", err)
	}

	waitForReplication(t, 200*time.Millisecond)
	for _, n := range []*node.Node{leaderNode, followerNode} {
		if !n.TopicExists(topicName) {
			t.Errorf("TopicExists(%q) = false on node %s, want true", topicName, n.GetNodeID())
		}
		if got := n.GetTopicLeaderNodeID(topicName); got != raftLeader.GetNodeID() {
			t.Errorf("GetTopicLeaderNodeID(%q) = %q, want %q", topicName, got, raftLeader.GetNodeID())
		}
	}
}

// TestNode_ApplyDeleteTopicEvent creates a topic via ApplyCreateTopicEvent then deletes it via ApplyDeleteTopicEvent.
func TestNode_ApplyDeleteTopicEvent(t *testing.T) {
	leaderSrv, followerSrv := StartTwoNodes(t, "delete-topic-leader", "delete-topic-follower")
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	leaderNode := leaderSrv.Node()
	followerNode := followerSrv.Node()
	waitForLeader(t, leaderNode, followerNode)

	raftLeader := getLeaderNode(leaderSrv, followerSrv)
	if raftLeader == nil {
		t.Fatal("no Raft leader")
	}

	topicName := "test-apply-delete-topic"
	ev := &protocol.MetadataEvent{
		CreateTopicEvent: &protocol.CreateTopicEvent{
			Topic:          topicName,
			ReplicaCount:   1,
			LeaderNodeID:   raftLeader.GetNodeID(),
			LeaderEpoch:    1,
			ReplicaNodeIds: []string{raftLeader.GetNodeID()},
		},
	}
	if err := raftLeader.ApplyCreateTopicEvent(ev); err != nil {
		t.Fatalf("ApplyCreateTopicEvent: %v", err)
	}
	waitForReplication(t, 200*time.Millisecond)
	if !leaderNode.TopicExists(topicName) || !followerNode.TopicExists(topicName) {
		t.Fatal("topic should exist after create")
	}

	delEv := &protocol.MetadataEvent{DeleteTopicEvent: &protocol.DeleteTopicEvent{Topic: topicName}}
	if err := raftLeader.ApplyDeleteTopicEvent(delEv); err != nil {
		t.Fatalf("ApplyDeleteTopicEvent: %v", err)
	}
	waitForReplication(t, 200*time.Millisecond)
	for _, n := range []*node.Node{leaderNode, followerNode} {
		if n.TopicExists(topicName) {
			t.Errorf("TopicExists(%q) = true on node %s after delete, want false", topicName, n.GetNodeID())
		}
	}
}

// TestNode_ApplyNodeAddEvent applies an AddNodeEvent on the Raft leader and verifies the node appears in metadata on both nodes.
func TestNode_ApplyNodeAddEvent(t *testing.T) {
	leaderSrv, followerSrv := StartTwoNodes(t, "add-node-leader", "add-node-follower")
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	leaderNode := leaderSrv.Node()
	followerNode := followerSrv.Node()
	waitForLeader(t, leaderNode, followerNode)

	raftLeader := getLeaderNode(leaderSrv, followerSrv)
	if raftLeader == nil {
		t.Fatal("no Raft leader")
	}

	newNodeID := "node-test-add"
	newRaftAddr := "127.0.0.1:19999"
	newRpcAddr := "127.0.0.1:19998"
	ev := &protocol.MetadataEvent{
		AddNodeEvent: &protocol.AddNodeEvent{
			NodeID:  newNodeID,
			Addr:    newRaftAddr,
			RpcAddr: newRpcAddr,
		},
	}
	if err := raftLeader.ApplyNodeAddEvent(ev); err != nil {
		t.Fatalf("ApplyNodeAddEvent: %v", err)
	}

	waitForReplication(t, 200*time.Millisecond)
	for _, n := range []*node.Node{leaderNode, followerNode} {
		if got := n.GetRpcAddrForNodeID(newNodeID); got != newRpcAddr {
			t.Errorf("GetRpcAddrForNodeID(%q) = %q on node %s, want %q", newNodeID, got, n.GetNodeID(), newRpcAddr)
		}
		ids := n.GetClusterNodeIDs()
		found := false
		for _, id := range ids {
			if id == newNodeID {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("GetClusterNodeIDs() on node %s should contain %q, got %v", n.GetNodeID(), newNodeID, ids)
		}
	}
}

func waitForReplication(t *testing.T, d time.Duration) {
	t.Helper()
	time.Sleep(d)
}
