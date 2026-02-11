package tests

import (
	"testing"
	"time"

	"github.com/mohitkumar/mlog/topic"
)

func TestNode_TwoNodeCluster(t *testing.T) {
	server1, server2 := StartTwoNodes(t, "node-server1", "node-server2")
	defer server1.Cleanup()
	defer server2.Cleanup()

	coord1 := server1.Coordinator()
	coord2 := server2.Coordinator()
	if coord1 == nil || coord2 == nil {
		t.Fatal("expected non-nil coordinators")
	}

	// With fake coordinators we deterministically treat server1 as the leader.
	if !coord1.IsRaftLeader || coord2.IsRaftLeader {
		t.Fatal("expected server1 to be leader and server2 to be follower")
	}

	if got := coord1.NodeID; got != "node-1" {
		t.Errorf("coord1 NodeID = %q, want node-1", got)
	}
	if got := coord2.NodeID; got != "node-2" {
		t.Errorf("coord2 NodeID = %q, want node-2", got)
	}

	// GetOtherNodes should report the peer node.
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

// TestNode_ApplyCreateTopicEvent applies a CreateTopic event via ApplyEvent on the leader
// and verifies the topic appears on both nodes' metadata.
func TestNode_ApplyCreateTopicEvent(t *testing.T) {
	server1, server2 := StartTwoNodes(t, "create-topic-server1", "create-topic-server2")
	defer server1.Cleanup()
	defer server2.Cleanup()

	leader := server1.Coordinator()

	topicName := "test-apply-create-topic"
	replicaNodeIds := []string{leader.NodeID}
	leader.ApplyEvent(topic.NewCreateTopicApplyEvent(topicName, 1, leader.NodeID, replicaNodeIds))

	waitForReplication(t, 200*time.Millisecond)
	for _, c := range []*FakeTopicCoordinator{server1.Coordinator(), server2.Coordinator()} {
		if !c.TopicExists(topicName) {
			t.Errorf("TopicExists(%q) = false on node %s, want true", topicName, c.NodeID)
		}
	}
}

// TestNode_ApplyDeleteTopicEvent creates a topic via ApplyEvent then deletes it via ApplyEvent.
func TestNode_ApplyDeleteTopicEvent(t *testing.T) {
	server1, server2 := StartTwoNodes(t, "delete-topic-server1", "delete-topic-server2")
	defer server1.Cleanup()
	defer server2.Cleanup()

	leader := server1.Coordinator()

	topicName := "test-apply-delete-topic"
	replicaNodeIds := []string{leader.NodeID}
	leader.ApplyEvent(topic.NewCreateTopicApplyEvent(topicName, 1, leader.NodeID, replicaNodeIds))
	waitForReplication(t, 200*time.Millisecond)
	if !server1.Coordinator().TopicExists(topicName) || !server2.Coordinator().TopicExists(topicName) {
		t.Fatal("topic should exist after create")
	}

	leader.ApplyEvent(topic.NewDeleteTopicApplyEvent(topicName))
	waitForReplication(t, 200*time.Millisecond)
	for _, c := range []*FakeTopicCoordinator{server1.Coordinator(), server2.Coordinator()} {
		if c.TopicExists(topicName) {
			t.Errorf("TopicExists(%q) = true on node %s after delete, want false", topicName, c.NodeID)
		}
	}
}

// TestNode_ApplyNodeAddEvent verifies AddNode on the leader updates cluster membership on both fake coordinators.
func TestNode_ApplyNodeAddEvent(t *testing.T) {
	server1, server2 := StartTwoNodes(t, "add-node-server1", "add-node-server2")
	defer server1.Cleanup()
	defer server2.Cleanup()

	leader := server1.Coordinator()

	newNodeID := "node-test-add"
	newRpcAddr := "127.0.0.1:19998"
	leader.AddNode(newNodeID, newRpcAddr)

	waitForReplication(t, 200*time.Millisecond)
	for _, c := range []*FakeTopicCoordinator{server1.Coordinator(), server2.Coordinator()} {
		nodes := c.GetOtherNodes()
		found := false
		for _, n := range nodes {
			if n.NodeID == newNodeID {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("GetOtherNodes() on node %s should contain %q, got %v", c.NodeID, newNodeID, nodes)
		}
	}
}

func waitForReplication(t *testing.T, d time.Duration) {
	t.Helper()
	time.Sleep(d)
}
