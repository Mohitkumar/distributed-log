package tests

import (
	"path"
	"testing"

	"github.com/mohitkumar/mlog/topic"
)

func TestTopicManager_WithFakeCoordinator_CreateTopic(t *testing.T) {
	baseDir := path.Join(t.TempDir(), "topic-fake")
	fake := NewFakeTopicCoordinator("node-1", "127.0.0.1:19001")
	logger := testLogger("node-1")

	tm, err := topic.NewTopicManager(baseDir, fake, logger)
	if err != nil {
		t.Fatalf("NewTopicManager: %v", err)
	}

	// Create topic with 0 replicas (single node, no RPC to others).
	replicaNodeIds, err := tm.CreateTopic("test-topic", 0)
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if len(replicaNodeIds) != 0 {
		t.Errorf("expected 0 replica node ids, got %v", replicaNodeIds)
	}

	// Topic should exist locally.
	top, err := tm.GetTopic("test-topic")
	if err != nil || top == nil {
		t.Fatalf("GetTopic: err=%v, top=%v", err, top)
	}
	if top.Name != "test-topic" {
		t.Errorf("topic name = %q, want test-topic", top.Name)
	}
	if top.Log == nil {
		t.Error("topic log should be non-nil")
	}
}

func TestTopicManager_WithFakeCoordinator_TopicExists(t *testing.T) {
	fake := NewFakeTopicCoordinator("node-1", "127.0.0.1:19002")
	if fake.TopicExists("t1") {
		t.Error("TopicExists(t1) should be false initially")
	}
	fake.ApplyEvent(topic.NewCreateTopicApplyEvent("t1", 1, "node-1", []string{"node-1"}))
	if !fake.TopicExists("t1") {
		t.Error("TopicExists(t1) should be true after apply")
	}
	names := fake.ListTopicNames()
	if len(names) != 1 || names[0] != "t1" {
		t.Errorf("ListTopicNames = %v, want [t1]", names)
	}
}
