package tests

import (
	"context"
	"encoding/json"
	"path"
	"testing"

	"github.com/mohitkumar/mlog/protocol"
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
	fake.SetReplicationTarget(tm)
	// Populate node in TopicManager so GetNodeIDWithLeastTopics and pickReplicaNodeIds work.
	tm.SetCurrentNodeID("node-1")
	addNodeData, _ := json.Marshal(protocol.AddNodeEvent{NodeID: "node-1", Addr: "127.0.0.1:19001", RpcAddr: "127.0.0.1:19001"})
	_ = tm.Apply(&protocol.MetadataEvent{EventType: protocol.MetadataEventTypeAddNode, Data: addNodeData})

	// Create topic via Raft event path (CreateTopic applies event; fake pushes to TopicManager).
	resp, err := tm.CreateTopic(context.Background(), &protocol.CreateTopicRequest{Topic: "test-topic", ReplicaCount: 0})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if resp.Topic != "test-topic" {
		t.Errorf("topic = %q, want test-topic", resp.Topic)
	}
	if len(resp.ReplicaNodeIds) != 0 {
		t.Errorf("expected 0 replica node ids, got %v", resp.ReplicaNodeIds)
	}

	// Topic should exist locally (created by createTopicFromEvent in Apply).
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
	baseDir := path.Join(t.TempDir(), "topic-exists")
	fake := NewFakeTopicCoordinator("node-1", "127.0.0.1:19002")
	tm, err := topic.NewTopicManager(baseDir, fake, testLogger("node-1"))
	if err != nil {
		t.Fatalf("NewTopicManager: %v", err)
	}
	fake.SetReplicationTarget(tm)
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
