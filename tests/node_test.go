package tests

import (
	"testing"
	"time"

	"github.com/mohitkumar/mlog/node"
)

// TestNode_TwoNodeCluster starts a 2-node cluster and tests node methods related to Raft.
func TestNode_TwoNodeCluster(t *testing.T) {
	server1, server2 := StartTwoNodes(t, "node-server1", "node-server2")
	defer server1.Cleanup()
	defer server2.Cleanup()

	node1 := server1.Node()
	node2 := server2.Node()
	if node1 == nil || node2 == nil {
		t.Fatal("expected non-nil nodes")
	}

	// Wait for one node to become Raft leader (bootstrap node may take a moment)
	var ok bool
	for i := 0; i < 100; i++ {
		if node1.IsLeader() || node2.IsLeader() {
			ok = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ok {
		t.Fatal("expected one node to become Raft leader within timeout")
	}

	// Exactly one node should be leader
	node1IsLeader := node1.IsLeader()
	node2IsLeader := node2.IsLeader()
	if node1IsLeader == node2IsLeader {
		t.Fatal("exactly one node should be Raft leader")
	}

	// Node IDs and addresses (node-1 / node-2 are fixed in StartTwoNodes)
	if got := node1.GetNodeID(); got != "node-1" {
		t.Errorf("node1 GetNodeID() = %q, want node-1", got)
	}
	if got := node2.GetNodeID(); got != "node-2" {
		t.Errorf("node2 GetNodeID() = %q, want node-2", got)
	}
	if node1.GetNodeAddr() != server1.Addr {
		t.Errorf("node1 GetNodeAddr() = %q, want %q", node1.GetNodeAddr(), server1.Addr)
	}
	if node2.GetNodeAddr() != server2.Addr {
		t.Errorf("node2 GetNodeAddr() = %q, want %q", node2.GetNodeAddr(), server2.Addr)
	}

	// GetOtherNodes: if Join succeeded in StartTwoNodes, node1 should see node-2
	otherNodes := node1.GetOtherNodes()
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
	if node1.TopicExists("no-such-topic") {
		t.Error("TopicExists(no-such-topic) should be false")
	}
}

// TestNode_ApplyCreateTopicEvent applies a CreateTopicEvent on the Raft leader and verifies the topic appears on both nodes.
func TestNode_ApplyCreateTopicEvent(t *testing.T) {
	server1, server2 := StartTwoNodes(t, "create-topic-server1", "create-topic-server2")
	defer server1.Cleanup()
	defer server2.Cleanup()

	waitForLeader(t, server1.Node(), server2.Node())

	raftLeader := getRaftLeaderNode(server1, server2)
	if raftLeader == nil {
		t.Fatal("no Raft leader")
	}

	topicName := "test-apply-create-topic"
	replicaNodeIds := []string{raftLeader.GetNodeID()}
	if err := raftLeader.ApplyCreateTopicEvent(topicName, 1, raftLeader.GetNodeID(), replicaNodeIds); err != nil {
		t.Fatalf("ApplyCreateTopicEvent: %v", err)
	}

	waitForReplication(t, 200*time.Millisecond)
	for _, n := range []*node.Node{server1.Node(), server2.Node()} {
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
	server1, server2 := StartTwoNodes(t, "delete-topic-server1", "delete-topic-server2")
	defer server1.Cleanup()
	defer server2.Cleanup()

	waitForLeader(t, server1.Node(), server2.Node())

	raftLeader := getRaftLeaderNode(server1, server2)
	if raftLeader == nil {
		t.Fatal("no Raft leader")
	}

	topicName := "test-apply-delete-topic"
	replicaNodeIds := []string{raftLeader.GetNodeID()}
	if err := raftLeader.ApplyCreateTopicEvent(topicName, 1, raftLeader.GetNodeID(), replicaNodeIds); err != nil {
		t.Fatalf("ApplyCreateTopicEvent: %v", err)
	}
	waitForReplication(t, 200*time.Millisecond)
	if !server1.Node().TopicExists(topicName) || !server2.Node().TopicExists(topicName) {
		t.Fatal("topic should exist after create")
	}

	if err := raftLeader.ApplyDeleteTopicEvent(topicName); err != nil {
		t.Fatalf("ApplyDeleteTopicEvent: %v", err)
	}
	waitForReplication(t, 200*time.Millisecond)
	for _, n := range []*node.Node{server1.Node(), server2.Node()} {
		if n.TopicExists(topicName) {
			t.Errorf("TopicExists(%q) = true on node %s after delete, want false", topicName, n.GetNodeID())
		}
	}
}

// TestNode_ApplyNodeAddEvent applies an AddNodeEvent on the Raft leader and verifies the node appears in metadata on both nodes.
func TestNode_ApplyNodeAddEvent(t *testing.T) {
	server1, server2 := StartTwoNodes(t, "add-node-server1", "add-node-server2")
	defer server1.Cleanup()
	defer server2.Cleanup()

	waitForLeader(t, server1.Node(), server2.Node())
	raftLeader := getRaftLeaderNode(server1, server2)
	if raftLeader == nil {
		t.Fatal("no Raft leader")
	}

	newNodeID := "node-test-add"
	newRaftAddr := "127.0.0.1:19999"
	newRpcAddr := "127.0.0.1:19998"
	if err := raftLeader.ApplyNodeAddEvent(newNodeID, newRaftAddr, newRpcAddr); err != nil {
		t.Fatalf("ApplyNodeAddEvent: %v", err)
	}

	waitForReplication(t, 200*time.Millisecond)
	for _, n := range []*node.Node{server1.Node(), server2.Node()} {
		got, err := n.GetRpcAddrForNodeID(newNodeID)
		if err != nil || got != newRpcAddr {
			t.Errorf("GetRpcAddrForNodeID(%q) = %q, %v on node %s, want %q, nil", newNodeID, got, err, n.GetNodeID(), newRpcAddr)
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
