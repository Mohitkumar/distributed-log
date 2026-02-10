package tests

import (
	"testing"
	"time"

	"github.com/mohitkumar/mlog/coordinator"
)

// TestNode_TwoNodeCluster starts a 2-node cluster and tests node methods related to Raft.
func TestNode_TwoNodeCluster(t *testing.T) {
	server1, server2 := StartTwoNodes(t, "node-server1", "node-server2")
	defer server1.Cleanup()
	defer server2.Cleanup()

	coord1 := server1.Coordinator()
	coord2 := server2.Coordinator()
	if coord1 == nil || coord2 == nil {
		t.Fatal("expected non-nil coordinators")
	}

	var ok bool
	for i := 0; i < 100; i++ {
		if coord1.IsLeader() || coord2.IsLeader() {
			ok = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ok {
		t.Fatal("expected one node to become Raft leader within timeout")
	}

	coord1IsLeader := coord1.IsLeader()
	coord2IsLeader := coord2.IsLeader()
	if coord1IsLeader == coord2IsLeader {
		t.Fatal("exactly one node should be Raft leader")
	}

	if got := coord1.GetNodeID(); got != "node-1" {
		t.Errorf("coord1 GetNodeID() = %q, want node-1", got)
	}
	if got := coord2.GetNodeID(); got != "node-2" {
		t.Errorf("coord2 GetNodeID() = %q, want node-2", got)
	}
	if coord1.GetNodeAddr() != server1.Addr {
		t.Errorf("coord1 GetNodeAddr() = %q, want %q", coord1.GetNodeAddr(), server1.Addr)
	}
	if coord2.GetNodeAddr() != server2.Addr {
		t.Errorf("coord2 GetNodeAddr() = %q, want %q", coord2.GetNodeAddr(), server2.Addr)
	}

	otherNodes := coord1.GetOtherNodes()
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

	if coord1.TopicExists("no-such-topic") {
		t.Error("TopicExists(no-such-topic) should be false")
	}
}

// TestNode_ApplyCreateTopicEvent applies a CreateTopicEvent on the Raft leader and verifies the topic appears on both nodes.
func TestNode_ApplyCreateTopicEvent(t *testing.T) {
	server1, server2 := StartTwoNodes(t, "create-topic-server1", "create-topic-server2")
	defer server1.Cleanup()
	defer server2.Cleanup()

	waitForLeader(t, server1.Coordinator(), server2.Coordinator())

	raftLeader := getRaftLeaderCoordinator(server1, server2)
	if raftLeader == nil {
		t.Fatal("no Raft leader")
	}

	topicName := "test-apply-create-topic"
	replicaNodeIds := []string{raftLeader.GetNodeID()}
	if err := raftLeader.ApplyCreateTopicEvent(topicName, 1, raftLeader.GetNodeID(), replicaNodeIds); err != nil {
		t.Fatalf("ApplyCreateTopicEvent: %v", err)
	}

	waitForReplication(t, 200*time.Millisecond)
	for _, c := range []*coordinator.Coordinator{server1.Coordinator(), server2.Coordinator()} {
		if !c.TopicExists(topicName) {
			t.Errorf("TopicExists(%q) = false on node %s, want true", topicName, c.GetNodeID())
		}
		if got := c.GetTopicLeaderNodeID(topicName); got != raftLeader.GetNodeID() {
			t.Errorf("GetTopicLeaderNodeID(%q) = %q, want %q", topicName, got, raftLeader.GetNodeID())
		}
	}
}

// TestNode_ApplyDeleteTopicEvent creates a topic via ApplyCreateTopicEvent then deletes it via ApplyDeleteTopicEvent.
func TestNode_ApplyDeleteTopicEvent(t *testing.T) {
	server1, server2 := StartTwoNodes(t, "delete-topic-server1", "delete-topic-server2")
	defer server1.Cleanup()
	defer server2.Cleanup()

	waitForLeader(t, server1.Coordinator(), server2.Coordinator())

	raftLeader := getRaftLeaderCoordinator(server1, server2)
	if raftLeader == nil {
		t.Fatal("no Raft leader")
	}

	topicName := "test-apply-delete-topic"
	replicaNodeIds := []string{raftLeader.GetNodeID()}
	if err := raftLeader.ApplyCreateTopicEvent(topicName, 1, raftLeader.GetNodeID(), replicaNodeIds); err != nil {
		t.Fatalf("ApplyCreateTopicEvent: %v", err)
	}
	waitForReplication(t, 200*time.Millisecond)
	if !server1.Coordinator().TopicExists(topicName) || !server2.Coordinator().TopicExists(topicName) {
		t.Fatal("topic should exist after create")
	}

	if err := raftLeader.ApplyDeleteTopicEvent(topicName); err != nil {
		t.Fatalf("ApplyDeleteTopicEvent: %v", err)
	}
	waitForReplication(t, 200*time.Millisecond)
	for _, c := range []*coordinator.Coordinator{server1.Coordinator(), server2.Coordinator()} {
		if c.TopicExists(topicName) {
			t.Errorf("TopicExists(%q) = true on node %s after delete, want false", topicName, c.GetNodeID())
		}
	}
}

// TestNode_ApplyNodeAddEvent applies an AddNodeEvent on the Raft leader and verifies the node appears in metadata on both nodes.
func TestNode_ApplyNodeAddEvent(t *testing.T) {
	server1, server2 := StartTwoNodes(t, "add-node-server1", "add-node-server2")
	defer server1.Cleanup()
	defer server2.Cleanup()

	waitForLeader(t, server1.Coordinator(), server2.Coordinator())
	raftLeader := getRaftLeaderCoordinator(server1, server2)
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
	for _, c := range []*coordinator.Coordinator{server1.Coordinator(), server2.Coordinator()} {
		got, err := c.GetRpcAddrForNodeID(newNodeID)
		if err != nil || got != newRpcAddr {
			t.Errorf("GetRpcAddrForNodeID(%q) = %q, %v on node %s, want %q, nil", newNodeID, got, err, c.GetNodeID(), newRpcAddr)
		}
		ids := c.GetClusterNodeIDs()
		found := false
		for _, id := range ids {
			if id == newNodeID {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("GetClusterNodeIDs() on node %s should contain %q, got %v", c.GetNodeID(), newNodeID, ids)
		}
	}
}

func waitForReplication(t *testing.T, d time.Duration) {
	t.Helper()
	time.Sleep(d)
}
