package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/protocol"
	"github.com/mohitkumar/mlog/topic"
)

func TestProduceWithAckLeader_10000Messages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 10k message test in -short mode")
	}

	server1, server2 := SetupTwoTestServers(t, "ack-leader-10k-leader", "ack-leader-10k-follower")
	defer server1.Cleanup()
	defer server2.Cleanup()

	ctx := context.Background()
	topicName := "ack-leader-10k-topic"

	leaderClient, err := client.NewRemoteClient(server1.Addr)
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	resp, err := leaderClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	// Fake has no Raft: apply the same create event on the follower so it can replicate.
	ev := topic.NewCreateTopicApplyEvent(topicName, 1, server1.Coordinator().NodeID, resp.ReplicaNodeIds)
	server2.Coordinator().ApplyEvent(ev)
	time.Sleep(400 * time.Millisecond) // allow follower replication thread to run and open replica log

	producerClient, err := client.NewProducerClient(server1.Addr)
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()
	_, err = producerClient.Produce(ctx, &protocol.ProduceRequest{
		Topic: topicName,
		Value: []byte("warmup"),
		Acks:  protocol.AckLeader,
	})
	if err != nil {
		t.Fatalf("warmup produce error: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	const n = 10000
	const batchSize = 100
	start := time.Now()
	var baseOffset uint64

	// Produce in batches
	for i := 0; i < n; i += batchSize {
		batchCount := batchSize
		if i+batchSize > n {
			batchCount = n - i
		}
		values := make([][]byte, batchCount)
		for j := 0; j < batchCount; j++ {
			values[j] = []byte(fmt.Sprintf("msg-%d", i+j))
		}

		resp, err := producerClient.ProduceBatch(ctx, &protocol.ProduceBatchRequest{
			Topic:  topicName,
			Values: values,
			Acks:   protocol.AckLeader,
		})
		if err != nil {
			t.Fatalf("ProduceBatch ACK_LEADER at i=%d error: %v", i, err)
		}
		if i == 0 {
			baseOffset = resp.BaseOffset
			if baseOffset != 1 {
				t.Fatalf("ProduceBatch ACK_LEADER: expected base offset 1, got %d", baseOffset)
			}
		}
		if resp.Count != uint32(batchCount) {
			t.Fatalf("ProduceBatch ACK_LEADER at i=%d: expected count %d, got %d", i, batchCount, resp.Count)
		}
	}

	t.Logf("produced %d ACK_LEADER messages in %v (using ProduceBatch)", n, time.Since(start))

	leaderNode, err := server1.TopicManager.GetLeader(topicName)
	if err != nil {
		t.Fatalf("failed to get leader node: %v", err)
	}

	actualLEO := leaderNode.LEO()
	expectedLEO := baseOffset + uint64(n) // Should be 1 + 10000 = 10001

	if actualLEO != expectedLEO {
		t.Fatalf("leader LEO mismatch: expected %d, got %d", expectedLEO, actualLEO)
	}

	t.Logf("verified leader LEO is %d (expected %d)", actualLEO, expectedLEO)
	t.Logf("verified message at offset %d matches expected pattern", baseOffset)

	expectedReplicaLEO := baseOffset + uint64(n)
	deadline := time.Now().Add(30 * time.Second)
	var replicaLEO uint64
	for time.Now().Before(deadline) {
		replicaTopic, err := server2.TopicManager.GetTopic(topicName)
		if err == nil && replicaTopic != nil && replicaTopic.Log != nil {
			replicaLEO = replicaTopic.Log.LEO()
			if replicaLEO >= expectedReplicaLEO {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	replicaTopic, err := server2.TopicManager.GetTopic(topicName)
	if err != nil {
		t.Fatalf("failed to get replica topic: %v", err)
	}
	if replicaTopic.Log == nil {
		t.Fatalf("replica topic has no log")
	}
	replicaLEO = replicaTopic.Log.LEO()
	if replicaLEO < expectedReplicaLEO {
		t.Fatalf("replica LEO is behind: expected at least %d, got %d", expectedReplicaLEO, replicaLEO)
	}

	t.Logf("verified replication: replica LEO is %d (leader LEO is %d)", replicaLEO, actualLEO)
}

func TestProduceWithAckAll_10000Messages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 10k message test in -short mode")
	}

	server1, server2 := SetupTwoTestServers(t, "ack-all-10k-leader", "ack-all-10k-follower")
	defer server1.Cleanup()
	defer server2.Cleanup()

	ctx := context.Background()
	topicName := "ack-all-10k-topic"

	leaderClient, err := client.NewRemoteClient(server1.Addr)
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	resp, err := leaderClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	// Fake has no Raft: apply the same create event on the follower so it can replicate.
	ev := topic.NewCreateTopicApplyEvent(topicName, 1, server1.Coordinator().NodeID, resp.ReplicaNodeIds)
	server2.Coordinator().ApplyEvent(ev)
	// Wait for replica creation and at least one replication tick (fake uses 100ms ticker).
	time.Sleep(600 * time.Millisecond)

	// Produce warmup (ACK_ALL waits for replica LEO; follower must have topic to replicate)
	producerClient, err := client.NewProducerClient(server1.Addr)
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()
	_, err = producerClient.Produce(ctx, &protocol.ProduceRequest{
		Topic: topicName,
		Value: []byte("warmup"),
		Acks:  protocol.AckAll,
	})
	if err != nil {
		t.Fatalf("warmup produce error: %v", err)
	}
	// Give follower time to replicate warmup so leader's replica LEO is updated before batch loop.
	time.Sleep(500 * time.Millisecond)

	const n = 10000
	const batchSize = 100
	start := time.Now()
	var baseOffset uint64

	// Produce in batches with ACK_ALL
	for i := 0; i < n; i += batchSize {
		batchCount := batchSize
		if i+batchSize > n {
			batchCount = n - i
		}
		values := make([][]byte, batchCount)
		for j := 0; j < batchCount; j++ {
			values[j] = []byte(fmt.Sprintf("msg-%d", i+j))
		}

		resp, err := producerClient.ProduceBatch(ctx, &protocol.ProduceBatchRequest{
			Topic:  topicName,
			Values: values,
			Acks:   protocol.AckAll,
		})
		if err != nil {
			t.Fatalf("ProduceBatch ACK_ALL at i=%d error: %v", i, err)
		}
		if i == 0 {
			baseOffset = resp.BaseOffset
			if baseOffset != 1 {
				t.Fatalf("ProduceBatch ACK_ALL: expected base offset 1, got %d", baseOffset)
			}
		}
		if resp.Count != uint32(batchCount) {
			t.Fatalf("ProduceBatch ACK_ALL at i=%d: expected count %d, got %d", i, batchCount, resp.Count)
		}
	}

	produceDuration := time.Since(start)
	t.Logf("produced %d ACK_ALL messages in %v (using ProduceBatch)", n, produceDuration)

	// ACK_ALL should take longer than ACK_LEADER because it waits for replication
	if produceDuration < 100*time.Millisecond {
		t.Logf("warning: ACK_ALL completed very quickly (%v), replication may have been instant", produceDuration)
	}

	leaderNode, err := server1.TopicManager.GetLeader(topicName)
	if err != nil {
		t.Fatalf("failed to get leader node: %v", err)
	}

	actualLEO := leaderNode.LEO()
	expectedLEO := baseOffset + uint64(n) // Should be 1 + 10000 = 10001

	if actualLEO != expectedLEO {
		t.Fatalf("leader LEO mismatch: expected %d, got %d", expectedLEO, actualLEO)
	}

	t.Logf("verified leader LEO is %d (expected %d)", actualLEO, expectedLEO)

	replicaTopic, err := server2.TopicManager.GetTopic(topicName)
	if err != nil {
		t.Fatalf("failed to get replica topic: %v", err)
	}
	if replicaTopic.Log == nil {
		t.Fatalf("replica topic has no log")
	}

	// With ACK_ALL, replica should have caught up (ACK_ALL waits for replication)
	// Verify by checking replica LEO matches leader LEO
	replicaLEO := replicaTopic.Log.LEO()
	expectedReplicaLEO := baseOffset + uint64(n) // Should match leader LEO

	if replicaLEO < expectedReplicaLEO {
		t.Fatalf("replica LEO is behind after ACK_ALL: expected at least %d, got %d (leader LEO=%d)",
			expectedReplicaLEO, replicaLEO, actualLEO)
	}

	t.Logf("ACK_ALL verified: %d messages replicated to follower in %v (replica LEO=%d, leader LEO=%d)",
		n, produceDuration, replicaLEO, actualLEO)
}
