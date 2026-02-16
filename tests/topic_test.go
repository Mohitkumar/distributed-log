package tests

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/protocol"
	"github.com/mohitkumar/mlog/topic"
)

func TestCreateTopic(t *testing.T) {
	servers := StartTwoNodesForTests(t, "topic-create-leader", "topic-create-follower")
	defer servers.Cleanup()

	ctx := context.Background()
	replClient, err := client.NewRemoteClient(servers.GetLeaderAddr())
	if err != nil {
		t.Fatalf("NewReplicationClient: %v", err)
	}

	topicName := "test-topic-1"
	leaderCoord := servers.Server1().Coordinator()
	resp, err := replClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic error: %v", err)
	}
	if resp.Topic != topicName {
		t.Fatalf("expected topic %s, got %s", topicName, resp.Topic)
	}
	// Fake has no Raft: apply the same create event on the follower so its TopicManager gets the topic.
	ev := topic.NewCreateTopicApplyEvent(topicName, 1, leaderCoord.NodeID, resp.ReplicaNodeIds)
	servers.Server2().Coordinator().ApplyEvent(ev)

	if _, err := os.Stat(filepath.Join(servers.Server1BaseDir(), topicName)); os.IsNotExist(err) {
		t.Fatalf("expected topic directory %s to exist on leader, got error: %v", topicName, err)
	}
	if _, err := os.Stat(filepath.Join(servers.Server2BaseDir(), topicName)); os.IsNotExist(err) {
		t.Fatalf("expected topic directory %s to exist on follower, got error: %v", topicName, err)
	}
}

func TestDeleteTopic(t *testing.T) {
	// Single node: CreateTopic and DeleteTopic both go to Raft leader; topic is created/deleted via Raft events.
	ts := StartTestServer(t, "topic-delete-single")
	defer ts.Cleanup()

	ctx := context.Background()
	raftLeaderClient, err := client.NewRemoteClient(ts.Addr)
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	producerClient, err := client.NewProducerClient(ts.Addr)
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()

	topicName := "test-delete-topic"
	_, err = raftLeaderClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 0,
	})
	if err != nil {
		t.Fatalf("CreateTopic error: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	_, err = producerClient.Produce(ctx, &protocol.ProduceRequest{
		Topic: topicName,
		Value: []byte("test-message"),
		Acks:  protocol.AckLeader,
	})
	if err != nil {
		t.Fatalf("Produce error: %v", err)
	}

	topicDir := filepath.Join(ts.BaseDir, topicName)
	if _, err := os.Stat(topicDir); os.IsNotExist(err) {
		t.Fatalf("expected topic directory %s to exist before deletion", topicDir)
	}

	deleteResp, err := raftLeaderClient.DeleteTopic(ctx, &protocol.DeleteTopicRequest{Topic: topicName})
	if err != nil {
		t.Fatalf("DeleteTopic error: %v", err)
	}
	if deleteResp.Topic != topicName {
		t.Fatalf("expected deleted topic %s, got %s", topicName, deleteResp.Topic)
	}

	time.Sleep(100 * time.Millisecond)

	if _, err := os.Stat(topicDir); !os.IsNotExist(err) {
		t.Fatalf("expected topic directory %s to be deleted, but it still exists", topicDir)
	}

	_, err = producerClient.Produce(ctx, &protocol.ProduceRequest{
		Topic: topicName,
		Value: []byte("should-fail"),
		Acks:  protocol.AckLeader,
	})
	if err == nil {
		t.Fatalf("expected Produce to fail for deleted topic, but it succeeded")
	}

	_, err = raftLeaderClient.DeleteTopic(ctx, &protocol.DeleteTopicRequest{Topic: "non-existent-topic"})
	if err == nil {
		t.Fatalf("expected DeleteTopic to fail for non-existent topic, but it succeeded")
	}
}
