package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/protocol"
	"github.com/mohitkumar/mlog/topic"
)

// producerTestServers holds the two-node cluster used by all producer tests (setup once).
type producerTestServers struct {
	server1 *TestServer
	server2 *TestServer
}

func (s *producerTestServers) getLeaderAddr() string {
	// With fake coordinators we deterministically treat server1 as the Raft leader.
	return s.server1.Addr
}

// getTopicLeaderTopicMgr returns the TopicManager of the node that is the topic leader for topicName (from metadata).
// Use this for verification when the topic leader may be different from the Raft leader.
func (s *producerTestServers) getTopicLeaderTopicMgr(topicName string) *topic.TopicManager {
	// Look up the topic on each server and return the TopicManager
	// whose CurrentNodeID matches the topic's LeaderNodeID.
	for _, srv := range []*TestServer{s.server1, s.server2} {
		tm := srv.TopicManager
		t, err := tm.GetTopic(topicName)
		if err != nil || t == nil {
			continue
		}
		if t.LeaderNodeID == tm.CurrentNodeID {
			return tm
		}
	}
	// Fallback to server1 if we couldn't resolve; tests will fail in that case.
	return s.server1.TopicManager
}

// getTopicLeaderAddr resolves the current topic leader RPC address via FindLeader.
func (s *producerTestServers) getTopicLeaderAddr(ctx context.Context, topicName string) (string, error) {
	discClient, err := client.NewRemoteClient(s.getLeaderAddr())
	if err != nil {
		return "", err
	}
	defer discClient.Close()
	resp, err := discClient.FindTopicLeader(ctx, &protocol.FindTopicLeaderRequest{Topic: topicName})
	if err != nil {
		return "", err
	}
	if resp.LeaderAddr == "" {
		return "", fmt.Errorf("empty leader address for topic %q", topicName)
	}
	return resp.LeaderAddr, nil
}

// TestProducer runs all producer tests with a single two-node cluster setup.
func TestProducer(t *testing.T) {
	server1, server2 := StartTwoNodes(t, "producer-server1", "producer-server2")
	defer server1.Cleanup()
	defer server2.Cleanup()

	servers := &producerTestServers{server1: server1, server2: server2}

	t.Run("Produce", func(t *testing.T) {
		ctx := context.Background()
		remoteClient, err := client.NewRemoteClient(servers.getLeaderAddr())
		if err != nil {
			t.Fatalf("NewRemoteClient: %v", err)
		}
		_, err = remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
			Topic:        "test-topic",
			ReplicaCount: 0,
		})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		remoteClient.Close()

		leaderAddr, err := servers.getTopicLeaderAddr(ctx, "test-topic")
		if err != nil {
			t.Fatalf("getTopicLeaderAddr: %v", err)
		}

		producerClient, err := client.NewProducerClient(leaderAddr)
		if err != nil {
			t.Fatalf("NewProducerClient: %v", err)
		}
		defer producerClient.Close()
		resp, err := producerClient.Produce(ctx, &protocol.ProduceRequest{
			Topic: "test-topic",
			Value: []byte("hello"),
			Acks:  protocol.AckLeader,
		})
		if err != nil {
			t.Fatalf("Produce: %v", err)
		}
		if resp.Offset != 0 {
			t.Fatalf("expected offset 0, got %d", resp.Offset)
		}

		resp2, err := producerClient.Produce(ctx, &protocol.ProduceRequest{
			Topic: "test-topic",
			Value: []byte("world"),
			Acks:  protocol.AckLeader,
		})
		if err != nil {
			t.Fatalf("Produce second: %v", err)
		}
		if resp2.Offset != 1 {
			t.Fatalf("expected offset 1, got %d", resp2.Offset)
		}

		leaderNode, err := servers.getTopicLeaderTopicMgr("test-topic").GetLeader("test-topic")
		if err != nil {
			t.Fatalf("GetLeader: %v", err)
		}
		const offWidth = 8
		for i, want := range []string{"hello", "world"} {
			entry, err := leaderNode.ReadUncommitted(uint64(i))
			if err != nil {
				t.Fatalf("ReadUncommitted(%d): %v", i, err)
			}
			if len(entry) < offWidth {
				t.Fatalf("offset %d: short read", i)
			}
			if got := string(entry[offWidth:]); got != want {
				t.Fatalf("offset %d: got %q, want %q", i, got, want)
			}
		}
	})

	t.Run("TopicNotFound", func(t *testing.T) {
		ctx := context.Background()
		producerClient, err := client.NewProducerClient(servers.getLeaderAddr())
		if err != nil {
			t.Fatalf("NewProducerClient: %v", err)
		}
		defer producerClient.Close()
		_, err = producerClient.Produce(ctx, &protocol.ProduceRequest{
			Topic: "nonexistent-topic",
			Value: []byte("x"),
			Acks:  protocol.AckLeader,
		})
		if err == nil {
			t.Fatal("expected error for unknown topic")
		}
	})

	t.Run("WithAckLeader", func(t *testing.T) {
		ctx := context.Background()
		remoteClient, err := client.NewRemoteClient(servers.getLeaderAddr())
		if err != nil {
			t.Fatalf("NewRemoteClient: %v", err)
		}
		_, err = remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
			Topic:        "test-topic-ack",
			ReplicaCount: 0,
		})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		remoteClient.Close()

		leaderAddr, err := servers.getTopicLeaderAddr(ctx, "test-topic-ack")
		if err != nil {
			t.Fatalf("getTopicLeaderAddr: %v", err)
		}

		producerClient, err := client.NewProducerClient(leaderAddr)
		if err != nil {
			t.Fatalf("NewProducerClient: %v", err)
		}
		defer producerClient.Close()
		resp, err := producerClient.Produce(ctx, &protocol.ProduceRequest{
			Topic: "test-topic-ack",
			Value: []byte("ack-leader"),
			Acks:  protocol.AckLeader,
		})
		if err != nil {
			t.Fatalf("Produce: %v", err)
		}
		if resp.Offset != 0 {
			t.Fatalf("expected offset 0, got %d", resp.Offset)
		}
	})

	t.Run("Verify", func(t *testing.T) {
		ctx := context.Background()
		remoteClient, err := client.NewRemoteClient(servers.getLeaderAddr())
		if err != nil {
			t.Fatalf("NewRemoteClient: %v", err)
		}
		_, err = remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
			Topic:        "test-topic-verify",
			ReplicaCount: 0,
		})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		remoteClient.Close()

		leaderAddr, err := servers.getTopicLeaderAddr(ctx, "test-topic-verify")
		if err != nil {
			t.Fatalf("getTopicLeaderAddr: %v", err)
		}

		producerClient, err := client.NewProducerClient(leaderAddr)
		if err != nil {
			t.Fatalf("NewProducerClient: %v", err)
		}
		defer producerClient.Close()
		for i := 0; i < 100; i++ {
			resp, err := producerClient.Produce(ctx, &protocol.ProduceRequest{
				Topic: "test-topic-verify",
				Value: []byte(fmt.Sprintf("message-%d", i)),
				Acks:  protocol.AckLeader,
			})
			if err != nil {
				t.Fatalf("Produce: %v", err)
			}
			if resp.Offset != uint64(i) {
				t.Fatalf("expected offset %d, got %d", i, resp.Offset)
			}
		}
	})

	t.Run("WithAckAll_NoReplicas", func(t *testing.T) {
		ctx := context.Background()
		remoteClient, err := client.NewRemoteClient(servers.getLeaderAddr())
		if err != nil {
			t.Fatalf("NewRemoteClient: %v", err)
		}
		_, err = remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
			Topic:        "test-topic-ackall",
			ReplicaCount: 0,
		})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		remoteClient.Close()

		leaderAddr, err := servers.getTopicLeaderAddr(ctx, "test-topic-ackall")
		if err != nil {
			t.Fatalf("getTopicLeaderAddr: %v", err)
		}

		producerClient, err := client.NewProducerClient(leaderAddr)
		if err != nil {
			t.Fatalf("NewProducerClient: %v", err)
		}
		defer producerClient.Close()
		resp, err := producerClient.Produce(ctx, &protocol.ProduceRequest{
			Topic: "test-topic-ackall",
			Value: []byte("ack-all"),
			Acks:  protocol.AckAll,
		})
		if err != nil {
			t.Fatalf("Produce: %v", err)
		}
		if resp.Offset != 0 {
			t.Fatalf("expected offset 0, got %d", resp.Offset)
		}
	})
}

// TestProducerBatch runs all produce-batch tests with a single two-node cluster setup.
func TestProducerBatch(t *testing.T) {
	server1, server2 := StartTwoNodes(t, "producer-batch-server1", "producer-batch-server2")
	defer server1.Cleanup()
	defer server2.Cleanup()

	servers := &producerTestServers{server1: server1, server2: server2}

	t.Run("ProduceBatch", func(t *testing.T) {
		ctx := context.Background()
		remoteClient, err := client.NewRemoteClient(servers.getLeaderAddr())
		if err != nil {
			t.Fatalf("NewRemoteClient: %v", err)
		}
		_, err = remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
			Topic:        "test-topic",
			ReplicaCount: 0,
		})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		remoteClient.Close()

		leaderAddr, err := servers.getTopicLeaderAddr(ctx, "test-topic")
		if err != nil {
			t.Fatalf("getTopicLeaderAddr: %v", err)
		}

		producerClient, err := client.NewProducerClient(leaderAddr)
		if err != nil {
			t.Fatalf("NewProducerClient: %v", err)
		}
		defer producerClient.Close()
		resp, err := producerClient.ProduceBatch(ctx, &protocol.ProduceBatchRequest{
			Topic:  "test-topic",
			Values: [][]byte{[]byte("a"), []byte("b"), []byte("c")},
			Acks:   protocol.AckLeader,
		})
		if err != nil {
			t.Fatalf("ProduceBatch: %v", err)
		}
		if resp.Count != 3 {
			t.Fatalf("expected count 3, got %d", resp.Count)
		}
		if resp.BaseOffset != 0 || resp.LastOffset != 2 {
			t.Fatalf("expected offsets base=0,last=2 got base=%d,last=%d", resp.BaseOffset, resp.LastOffset)
		}
	})

	t.Run("Verify", func(t *testing.T) {
		ctx := context.Background()
		remoteClient, err := client.NewRemoteClient(servers.getLeaderAddr())
		if err != nil {
			t.Fatalf("NewRemoteClient: %v", err)
		}
		_, err = remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
			Topic:        "test-topic-batch",
			ReplicaCount: 0,
		})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		remoteClient.Close()

		leaderAddr, err := servers.getTopicLeaderAddr(ctx, "test-topic-batch")
		if err != nil {
			t.Fatalf("getTopicLeaderAddr: %v", err)
		}

		producerClient, err := client.NewProducerClient(leaderAddr)
		if err != nil {
			t.Fatalf("NewProducerClient: %v", err)
		}
		defer producerClient.Close()
		messages := make([][]byte, 0)
		for i := 0; i < 100; i++ {
			messages = append(messages, []byte(fmt.Sprintf("message-%d", i)))
		}
		resp, err := producerClient.ProduceBatch(ctx, &protocol.ProduceBatchRequest{
			Topic:  "test-topic-batch",
			Values: messages,
			Acks:   protocol.AckLeader,
		})
		if err != nil {
			t.Fatalf("Produce: %v", err)
		}
		if resp.BaseOffset != 0 {
			t.Fatalf("expected base offset 0 got %d", resp.BaseOffset)
		}
		if resp.LastOffset != 99 {
			t.Fatalf("expected last offset 99 got %d", resp.LastOffset)
		}
	})

	t.Run("TopicNotFound", func(t *testing.T) {
		producerClient, err := client.NewProducerClient(servers.getLeaderAddr())
		if err != nil {
			t.Fatalf("NewProducerClient: %v", err)
		}
		defer producerClient.Close()
		_, err = producerClient.ProduceBatch(context.Background(), &protocol.ProduceBatchRequest{
			Topic:  "missing",
			Values: [][]byte{[]byte("x")},
			Acks:   protocol.AckLeader,
		})
		if err == nil {
			t.Fatal("expected error for unknown topic")
		}
	})

	t.Run("InvalidArgs", func(t *testing.T) {
		ctx := context.Background()
		remoteClient, err := client.NewRemoteClient(servers.getLeaderAddr())
		if err != nil {
			t.Fatalf("NewRemoteClient: %v", err)
		}
		_, err = remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
			Topic:        "test-topic-2",
			ReplicaCount: 1,
		})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		remoteClient.Close()

		producerClient, err := client.NewProducerClient(servers.getLeaderAddr())
		if err != nil {
			t.Fatalf("NewProducerClient: %v", err)
		}
		defer producerClient.Close()

		_, err = producerClient.ProduceBatch(context.Background(), &protocol.ProduceBatchRequest{
			Topic:  "",
			Values: [][]byte{[]byte("x")},
			Acks:   protocol.AckLeader,
		})
		if err == nil {
			t.Fatal("expected error for empty topic")
		}

		_, err = producerClient.ProduceBatch(context.Background(), &protocol.ProduceBatchRequest{
			Topic:  "test-topic",
			Values: nil,
			Acks:   protocol.AckLeader,
		})
		if err == nil {
			t.Fatal("expected error for empty values")
		}
	})
}

func BenchmarkProduce(b *testing.B) {
	server1, server2 := StartTwoNodes(b, "bench-produce-server1", "bench-produce-server2")
	defer server1.Cleanup()
	defer server2.Cleanup()

	servers := &producerTestServers{server1: server1, server2: server2}

	ctx := context.Background()
	remoteClient, err := client.NewRemoteClient(servers.getLeaderAddr())
	if err != nil {
		b.Fatalf("NewRemoteClient: %v", err)
	}
	_, err = remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        "test-topic",
		ReplicaCount: 0,
	})
	if err != nil {
		b.Fatalf("CreateTopic: %v", err)
	}
	remoteClient.Close()

	leaderAddr, err := servers.getTopicLeaderAddr(ctx, "test-topic")
	if err != nil {
		b.Fatalf("getTopicLeaderAddr: %v", err)
	}

	producerClient, err := client.NewProducerClient(leaderAddr)
	if err != nil {
		b.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()
	req := &protocol.ProduceRequest{
		Topic: "test-topic",
		Value: []byte("bench-value"),
		Acks:  protocol.AckLeader,
	}

	for i := 0; i < b.N; i++ {
		_, err = producerClient.Produce(ctx, req)
		if err != nil {
			b.Fatalf("Produce: %v", err)
		}
	}
	seconds := b.Elapsed().Seconds()
	if seconds > 0 {
		b.ReportMetric(float64(b.N)/seconds, "req/s")
	}
}

func BenchmarkProduceBatch(b *testing.B) {
	server1, server2 := StartTwoNodes(b, "bench-batch-server1", "bench-batch-server2")
	defer server1.Cleanup()
	defer server2.Cleanup()

	servers := &producerTestServers{server1: server1, server2: server2}

	ctx := context.Background()
	remoteClient, err := client.NewRemoteClient(servers.getLeaderAddr())
	if err != nil {
		b.Fatalf("NewRemoteClient: %v", err)
	}
	_, err = remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        "test-topic",
		ReplicaCount: 0,
	})
	if err != nil {
		b.Fatalf("CreateTopic: %v", err)
	}
	remoteClient.Close()

	leaderAddr, err := servers.getTopicLeaderAddr(ctx, "test-topic")
	if err != nil {
		b.Fatalf("getTopicLeaderAddr: %v", err)
	}

	producerClient, err := client.NewProducerClient(leaderAddr)
	if err != nil {
		b.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()
	req := &protocol.ProduceBatchRequest{
		Topic:  "test-topic",
		Values: [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("f"), []byte("g"), []byte("h"), []byte("i"), []byte("j")},
		Acks:   protocol.AckLeader,
	}

	for i := 0; i < b.N; i++ {
		_, err = producerClient.ProduceBatch(ctx, req)
		if err != nil {
			b.Fatalf("ProduceBatch: %v", err)
		}
	}
	seconds := b.Elapsed().Seconds()
	if seconds > 0 {
		b.ReportMetric(float64(b.N)/seconds, "batches/s")
		b.ReportMetric(float64(b.N*len(req.Values))/seconds, "msgs/s")
	}
}
