package rpc

import (
	"context"
	"fmt"
	"testing"

	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/protocol"
	"github.com/mohitkumar/mlog/testutil"
)

func TestProduce(t *testing.T) {
	ts := SetupTestServer(t, "127.0.0.1:0", "node-1", "producer-test")
	defer ts.Cleanup()

	ctx := context.Background()
	client, err := client.NewProducerClient(ts.Broker.GetAddr())
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer client.Close()
	resp, err := client.Produce(ctx, &protocol.ProduceRequest{
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

	resp2, err := client.Produce(ctx, &protocol.ProduceRequest{
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
}

func TestProduce_TopicNotFound(t *testing.T) {
	ts := testutil.SetupTestServerWithTopic(t, "node-1", "producer-test", "test-topic", 0, func(comps *testutil.TestServerComponents) testutil.RPCServer {
		return NewServer(comps.TopicManager, comps.ConsumerManager)
	})
	defer ts.Cleanup()

	ctx := context.Background()
	client, err := client.NewProducerClient(ts.Broker.GetAddr())
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer client.Close()
	_, err = client.Produce(ctx, &protocol.ProduceRequest{
		Topic: "nonexistent-topic",
		Value: []byte("x"),
		Acks:  protocol.AckLeader,
	})
	if err == nil {
		t.Fatal("expected error for unknown topic")
	}
}

func TestProduce_WithAckLeader(t *testing.T) {
	ts := testutil.SetupTestServerWithTopic(t, "node-1", "producer-test", "test-topic", 0, func(comps *testutil.TestServerComponents) testutil.RPCServer {
		return NewServer(comps.TopicManager, comps.ConsumerManager)
	})
	defer ts.Cleanup()

	ctx := context.Background()
	client, err := client.NewProducerClient(ts.Broker.GetAddr())
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer client.Close()
	resp, err := client.Produce(ctx, &protocol.ProduceRequest{
		Topic: "test-topic",
		Value: []byte("ack-leader"),
		Acks:  protocol.AckLeader,
	})
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if resp.Offset != 0 {
		t.Fatalf("expected offset 0, got %d", resp.Offset)
	}
}

func TestProduce_Verify(t *testing.T) {
	ts := testutil.SetupTestServerWithTopic(t, "node-1", "producer-test", "test-topic", 0, func(comps *testutil.TestServerComponents) testutil.RPCServer {
		return NewServer(comps.TopicManager, comps.ConsumerManager)
	})
	defer ts.Cleanup()

	ctx := context.Background()
	client, err := client.NewProducerClient(ts.Broker.GetAddr())
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer client.Close()
	for i := 0; i < 100; i++ {
		resp, err := client.Produce(ctx, &protocol.ProduceRequest{
			Topic: "test-topic",
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

}

func TestProduceBatch_Verify(t *testing.T) {
	ts := testutil.SetupTestServerWithTopic(t, "node-1", "producer-test", "test-topic", 0, func(comps *testutil.TestServerComponents) testutil.RPCServer {
		return NewServer(comps.TopicManager, comps.ConsumerManager)
	})
	defer ts.Cleanup()

	ctx := context.Background()
	client, err := client.NewProducerClient(ts.Broker.GetAddr())
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer client.Close()
	messages := make([][]byte, 0)
	for i := 0; i < 100; i++ {
		messages = append(messages, []byte(fmt.Sprintf("message-%d", i)))
	}
	resp, err := client.ProduceBatch(ctx, &protocol.ProduceBatchRequest{
		Topic:  "test-topic",
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
		t.Fatalf("expected base offset 0 got %d", resp.LastOffset)
	}
}

func TestProduce_WithAckAll_NoReplicas(t *testing.T) {
	// With 0 replicas, ACK_ALL behaves like ACK_LEADER (returns immediately).
	ts := testutil.SetupTestServerWithTopic(t, "node-1", "producer-test", "test-topic", 0, func(comps *testutil.TestServerComponents) testutil.RPCServer {
		return NewServer(comps.TopicManager, comps.ConsumerManager)
	})
	defer ts.Cleanup()

	ctx := context.Background()
	client, err := client.NewProducerClient(ts.Broker.GetAddr())
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer client.Close()
	resp, err := client.Produce(ctx, &protocol.ProduceRequest{
		Topic: "test-topic",
		Value: []byte("ack-all"),
		Acks:  protocol.AckAll,
	})
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if resp.Offset != 0 {
		t.Fatalf("expected offset 0, got %d", resp.Offset)
	}
}

func BenchmarkProduce(b *testing.B) {
	ts := testutil.SetupTestServerWithTopic(b, "node-1", "producer-bench", "test-topic", 0, func(comps *testutil.TestServerComponents) testutil.RPCServer {
		return NewServer(comps.TopicManager, comps.ConsumerManager)
	})
	defer ts.Cleanup()

	client, err := client.NewProducerClient(ts.Broker.GetAddr())
	if err != nil {
		b.Fatalf("NewProducerClient: %v", err)
	}
	defer client.Close()
	ctx := context.Background()
	req := &protocol.ProduceRequest{
		Topic: "test-topic",
		Value: []byte("bench-value"),
		Acks:  protocol.AckLeader,
	}

	for b.Loop() {
		_, err = client.Produce(ctx, req)
		if err != nil {
			b.Fatalf("Produce: %v", err)
		}
	}
	seconds := b.Elapsed().Seconds()
	if seconds > 0 {
		b.ReportMetric(float64(b.N)/seconds, "req/s")
	}
}

func TestProduceBatch(t *testing.T) {
	ts := testutil.SetupTestServerWithTopic(t, "node-1", "producer-test", "test-topic", 0, func(comps *testutil.TestServerComponents) testutil.RPCServer {
		return NewServer(comps.TopicManager, comps.ConsumerManager)
	})
	defer ts.Cleanup()

	client, err := client.NewProducerClient(ts.Broker.GetAddr())
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer client.Close()
	ctx := context.Background()
	resp, err := client.ProduceBatch(ctx, &protocol.ProduceBatchRequest{
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
}

func TestProduceBatch_TopicNotFound(t *testing.T) {
	ts := testutil.SetupTestServerWithTopic(t, "node-1", "producer-test", "test-topic", 0, func(comps *testutil.TestServerComponents) testutil.RPCServer {
		return NewServer(comps.TopicManager, comps.ConsumerManager)
	})
	defer ts.Cleanup()

	client, err := client.NewProducerClient(ts.Broker.GetAddr())
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer client.Close()
	_, err = client.ProduceBatch(context.Background(), &protocol.ProduceBatchRequest{
		Topic:  "missing",
		Values: [][]byte{[]byte("x")},
		Acks:   protocol.AckLeader,
	})
	if err == nil {
		t.Fatal("expected error for unknown topic")
	}
}

func TestProduceBatch_InvalidArgs(t *testing.T) {
	ts := testutil.SetupTestServerWithTopic(t, "node-1", "producer-test", "test-topic", 0, func(comps *testutil.TestServerComponents) testutil.RPCServer {
		return NewServer(comps.TopicManager, comps.ConsumerManager)
	})
	defer ts.Cleanup()

	client, err := client.NewProducerClient(ts.Broker.GetAddr())
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer client.Close()

	_, err = client.ProduceBatch(context.Background(), &protocol.ProduceBatchRequest{
		Topic:  "",
		Values: [][]byte{[]byte("x")},
		Acks:   protocol.AckLeader,
	})
	if err == nil {
		t.Fatal("expected error for empty topic")
	}

	_, err = client.ProduceBatch(context.Background(), &protocol.ProduceBatchRequest{
		Topic:  "test-topic",
		Values: nil,
		Acks:   protocol.AckLeader,
	})
	if err == nil {
		t.Fatal("expected error for empty values")
	}
}

func BenchmarkProduceBatch(b *testing.B) {
	ts := testutil.SetupTestServerWithTopic(b, "node-1", "producer-bench", "test-topic", 0, func(comps *testutil.TestServerComponents) testutil.RPCServer {
		return NewServer(comps.TopicManager, comps.ConsumerManager)
	})
	defer ts.Cleanup()

	client, err := client.NewProducerClient(ts.Broker.GetAddr())
	if err != nil {
		b.Fatalf("NewProducerClient: %v", err)
	}
	defer client.Close()
	ctx := context.Background()
	req := &protocol.ProduceBatchRequest{
		Topic:  "test-topic",
		Values: [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("f"), []byte("g"), []byte("h"), []byte("i"), []byte("j")},
		Acks:   protocol.AckLeader,
	}

	for b.Loop() {
		_, err := client.ProduceBatch(ctx, req)
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
