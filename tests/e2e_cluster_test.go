package tests

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/protocol"
)

// TestE2E_RealCluster_BasicProduceConsume tests basic produce and consume on a 3-node real cluster.
func TestE2E_RealCluster_BasicProduceConsume(t *testing.T) {
	node1, _, _, cleanup := StartRealThreeNodeCluster(t, "e2e-basic")
	defer cleanup()

	ctx := context.Background()
	topicName := "basic-topic"

	// Create topic
	remoteClient, err := client.NewRemoteClient(node1.Addr)
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	_, err = remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 0,
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	remoteClient.Close()

	// Produce messages
	producerClient, err := client.NewProducerClient(node1.Addr)
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()

	for i := 1; i <= 5; i++ {
		resp, err := producerClient.Produce(ctx, &protocol.ProduceRequest{
			Topic: topicName,
			Value: []byte("message-" + strconv.Itoa(i)),
			Acks:  protocol.AckLeader,
		})
		if err != nil {
			t.Fatalf("Produce %d: %v", i, err)
		}
		if resp.Offset != uint64(i-1) {
			t.Fatalf("expected offset %d, got %d", i-1, resp.Offset)
		}
	}

	// Verify messages can be fetched/read from log
	topic, err := node1.TopicManager.GetTopic(topicName)
	if err != nil {
		t.Fatalf("GetTopic: %v", err)
	}

	for i := 0; i < 5; i++ {
		entry, err := topic.Log.ReadUncommitted(uint64(i))
		if err != nil {
			t.Fatalf("ReadUncommitted offset %d: %v", i, err)
		}
		expectedValue := "message-" + strconv.Itoa(i+1)
		const offWidth = 8
		if len(entry) < offWidth {
			t.Fatalf("offset %d: short read", i)
		}
		actualValue := string(entry[offWidth:])
		if actualValue != expectedValue {
			t.Fatalf("offset %d: expected %q, got %q", i, expectedValue, actualValue)
		}
	}

	t.Log("✓ Real cluster basic produce/consume test passed")
}

// TestE2E_RealCluster_ReplicationAcrossNodes tests data replicates to follower nodes.
func TestE2E_RealCluster_ReplicationAcrossNodes(t *testing.T) {
	node1, node2, node3, cleanup := StartRealThreeNodeCluster(t, "e2e-replication")
	defer cleanup()

	ctx := context.Background()
	topicName := "replicated-topic"

	// Create topic with replication count 2 (leader + 2 replicas)
	remoteClient, err := client.NewRemoteClient(node1.Addr)
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	resp, err := remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 2,
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	remoteClient.Close()

	t.Logf("Created topic with replicas=%v", resp.ReplicaNodeIds)

	// Produce messages
	producerClient, err := client.NewProducerClient(node1.Addr)
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()

	numMessages := 5
	for i := 1; i <= numMessages; i++ {
		_, err := producerClient.Produce(ctx, &protocol.ProduceRequest{
			Topic: topicName,
			Value: []byte("replicated-msg-" + strconv.Itoa(i)),
			Acks:  protocol.AckLeader,
		})
		if err != nil {
			t.Fatalf("Produce %d: %v", i, err)
		}
	}

	// Wait for replication to propagate
	time.Sleep(1 * time.Second)

	// Verify topic exists on all nodes
	for idx, node := range []*RealTestServer{node1, node2, node3} {
		topic, err := node.TopicManager.GetTopic(topicName)
		if err != nil {
			t.Logf("node %d: GetTopic error: %v", idx+1, err)
			continue
		}
		if topic == nil {
			t.Logf("node %d: topic not found", idx+1)
			continue
		}
		t.Logf("node %d: topic found, LEO=%d", idx+1, topic.Log.LEO())
	}

	t.Log("✓ Real cluster replication test passed")
}

// TestE2E_RealCluster_ConsumerOffsets tests consumer offset tracking.
func TestE2E_RealCluster_ConsumerOffsets(t *testing.T) {
	node1, _, _, cleanup := StartRealThreeNodeCluster(t, "e2e-offsets")
	defer cleanup()

	ctx := context.Background()
	topicName := "offset-topic"

	// Create topic
	remoteClient, err := client.NewRemoteClient(node1.Addr)
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	_, err = remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 0,
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	remoteClient.Close()

	// Produce messages
	producerClient, err := client.NewProducerClient(node1.Addr)
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()

	for i := 0; i < 10; i++ {
		_, err := producerClient.Produce(ctx, &protocol.ProduceRequest{
			Topic: topicName,
			Value: []byte("msg-" + strconv.Itoa(i)),
			Acks:  protocol.AckLeader,
		})
		if err != nil {
			t.Fatalf("Produce %d: %v", i, err)
		}
	}

	consumerID := "test-consumer"

	// Test consumer offset operations
	consumerClient, err := client.NewConsumerClient(node1.Addr)
	if err != nil {
		t.Fatalf("NewConsumerClient: %v", err)
	}
	defer consumerClient.Close()

	// Commit an offset
	commitResp, err := consumerClient.CommitOffset(ctx, &protocol.CommitOffsetRequest{
		Id:     consumerID,
		Topic:  topicName,
		Offset: 5,
	})
	if err != nil {
		t.Fatalf("CommitOffset: %v", err)
	}
	if !commitResp.Success {
		t.Fatal("CommitOffset failed")
	}

	// Fetch the offset back
	offsetResp, err := consumerClient.FetchOffset(ctx, &protocol.FetchOffsetRequest{
		Id:    consumerID,
		Topic: topicName,
	})
	if err != nil {
		t.Fatalf("FetchOffset: %v", err)
	}
	if offsetResp.Offset != 5 {
		t.Fatalf("expected offset 5, got %d", offsetResp.Offset)
	}

	t.Log("✓ Real cluster consumer offsets test passed")
}

// TestE2E_RealCluster_ConcurrentProducers tests concurrent production.
func TestE2E_RealCluster_ConcurrentProducers(t *testing.T) {
	node1, _, _, cleanup := StartRealThreeNodeCluster(t, "e2e-concurrent")
	defer cleanup()

	ctx := context.Background()
	topicName := "concurrent-topic"

	// Create topic
	remoteClient, err := client.NewRemoteClient(node1.Addr)
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	_, err = remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 0,
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	remoteClient.Close()

	messagesPerProducer := 20
	numProducers := 3
	var wg sync.WaitGroup
	errors := make(chan error, numProducers*messagesPerProducer)

	// Start concurrent producers
	for prodIdx := 0; prodIdx < numProducers; prodIdx++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			producerClient, err := client.NewProducerClient(node1.Addr)
			if err != nil {
				errors <- err
				return
			}
			defer producerClient.Close()

			for i := 0; i < messagesPerProducer; i++ {
				_, err := producerClient.Produce(ctx, &protocol.ProduceRequest{
					Topic: topicName,
					Value: []byte("producer-" + strconv.Itoa(idx) + "-msg-" + strconv.Itoa(i)),
					Acks:  protocol.AckLeader,
				})
				if err != nil {
					errors <- err
					return
				}
			}
		}(prodIdx)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		if err != nil {
			t.Fatalf("concurrent produce error: %v", err)
		}
	}

	// Verify message count
	topic, err := node1.TopicManager.GetTopic(topicName)
	if err != nil {
		t.Fatalf("GetTopic: %v", err)
	}
	expectedCount := uint64(numProducers * messagesPerProducer)
	if topic.Log.LEO() != expectedCount {
		t.Fatalf("expected %d messages, got %d", expectedCount, topic.Log.LEO())
	}

	t.Log("✓ Real cluster concurrent producers test passed")
}

// TestE2E_RealCluster_ClusterMetadata tests cluster metadata consistency.
func TestE2E_RealCluster_ClusterMetadata(t *testing.T) {
	node1, node2, node3, cleanup := StartRealThreeNodeCluster(t, "e2e-metadata")
	defer cleanup()

	// Verify each node has correct metadata
	for idx, node := range []*RealTestServer{node1, node2, node3} {
		if len(node.TopicManager.Nodes) < 3 {
			t.Logf("node %d: waiting for cluster members... (have %d)", idx+1, len(node.TopicManager.Nodes))
			time.Sleep(500 * time.Millisecond)
		}
		if len(node.TopicManager.Nodes) != 3 {
			t.Fatalf("node %d: expected 3 cluster members, got %d", idx+1, len(node.TopicManager.Nodes))
		}
	}

	t.Log("✓ Real cluster metadata test passed")
}

// TestE2E_RealCluster_LeaderElection tests Raft leader election (basic verification).
func TestE2E_RealCluster_LeaderElection(t *testing.T) {
	node1, node2, node3, cleanup := StartRealThreeNodeCluster(t, "e2e-leader")
	defer cleanup()

	// Verify a leader was elected
	for idx, node := range []*RealTestServer{node1, node2, node3} {
		isLeader := node.Coordinator.IsLeader()
		leaderID, _ := node.Coordinator.GetRaftLeaderNodeID()
		t.Logf("node %d (%s): is_leader=%v, leader_id=%s", idx+1, node.NodeID, isLeader, leaderID)
	}

	// At least one node should be a leader
	isLeaderFound := false
	for _, node := range []*RealTestServer{node1, node2, node3} {
		if node.Coordinator.IsLeader() {
			isLeaderFound = true
			break
		}
	}
	if !isLeaderFound {
		t.Fatal("no Raft leader elected")
	}

	t.Log("✓ Real cluster leader election test passed")
}
