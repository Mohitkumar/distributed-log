package tests

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/protocol"
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

	if _, err := os.Stat(filepath.Join(servers.Server1BaseDir(), topicName)); os.IsNotExist(err) {
		t.Fatalf("expected topic directory %s to exist on leader, got error: %v", topicName, err)
	}
	if _, err := os.Stat(filepath.Join(servers.Server2BaseDir(), topicName)); os.IsNotExist(err) {
		t.Fatalf("expected topic directory %s to exist on follower, got error: %v", topicName, err)
	}
}

func TestDeleteTopic(t *testing.T) {
	servers := StartTwoNodesForTests(t, "topic-delete-leader", "topic-delete-follower")
	defer servers.Cleanup()

	ctx := context.Background()
	leaderClient, err := client.NewRemoteClient(servers.GetLeaderAddr())
	if err != nil {
		t.Fatalf("NewReplicationClient: %v", err)
	}
	producerClient, err := client.NewProducerClient(servers.GetLeaderAddr())
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()

	topicName := "test-delete-topic"
	_, err = leaderClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic error: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	_, err = producerClient.Produce(ctx, &protocol.ProduceRequest{
		Topic: topicName,
		Value: []byte("test-message"),
		Acks:  protocol.AckLeader,
	})
	if err != nil {
		t.Fatalf("Produce error: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	leaderTopicDir := filepath.Join(servers.Server1BaseDir(), topicName)
	followerTopicDir := filepath.Join(servers.Server2BaseDir(), topicName)

	if _, err := os.Stat(leaderTopicDir); os.IsNotExist(err) {
		t.Fatalf("expected leader topic directory %s to exist before deletion", leaderTopicDir)
	}
	if _, err := os.Stat(followerTopicDir); os.IsNotExist(err) {
		t.Fatalf("expected replica directory %s to exist before deletion", followerTopicDir)
	}

	deleteResp, err := leaderClient.DeleteTopic(ctx, &protocol.DeleteTopicRequest{Topic: topicName})
	if err != nil {
		t.Fatalf("DeleteTopic error: %v", err)
	}
	if deleteResp.Topic != topicName {
		t.Fatalf("expected deleted topic %s, got %s", topicName, deleteResp.Topic)
	}

	time.Sleep(500 * time.Millisecond)

	if _, err := os.Stat(leaderTopicDir); !os.IsNotExist(err) {
		t.Fatalf("expected leader topic directory %s to be deleted, but it still exists", leaderTopicDir)
	}
	if _, err := os.Stat(followerTopicDir); !os.IsNotExist(err) {
		t.Fatalf("expected follower topic directory %s to be deleted, but it still exists", followerTopicDir)
	}
	if _, err := os.Stat(followerTopicDir); !os.IsNotExist(err) {
		t.Fatalf("expected replica directory %s to be deleted, but it still exists", followerTopicDir)
	}

	_, err = producerClient.Produce(ctx, &protocol.ProduceRequest{
		Topic: topicName,
		Value: []byte("should-fail"),
		Acks:  protocol.AckLeader,
	})
	if err == nil {
		t.Fatalf("expected Produce to fail for deleted topic, but it succeeded")
	}

	_, err = leaderClient.DeleteTopic(ctx, &protocol.DeleteTopicRequest{Topic: "non-existent-topic"})
	if err == nil {
		t.Fatalf("expected DeleteTopic to fail for non-existent topic, but it succeeded")
	}
}
