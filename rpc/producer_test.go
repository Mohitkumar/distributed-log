package rpc

import (
	"context"
	"fmt"
	"testing"

	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/protocol"
)

func TestProduce(t *testing.T) {
	ts := StartTestServer(t, "leader")
	defer ts.Cleanup()
	if err := ts.TopicManager.CreateTopic("test-topic", 0); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ctx := context.Background()
	producerClient, err := client.NewProducerClient(ts.Addr)
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

	// Verify messages on leader
	leaderNode, err := ts.TopicManager.GetLeader("test-topic")
	if err != nil {
		t.Fatalf("GetLeader: %v", err)
	}
	const offWidth = 8
	for i, want := range []string{"hello", "world"} {
		entry, err := leaderNode.Log.ReadUncommitted(uint64(i))
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
}

func TestProduce_TopicNotFound(t *testing.T) {
	ts := StartTestServer(t, "leader")
	defer ts.Cleanup()

	ctx := context.Background()
	producerClient, err := client.NewProducerClient(ts.Addr)
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
}

func TestProduce_WithAckLeader(t *testing.T) {
	ts := StartTestServer(t, "leader")
	defer ts.Cleanup()

	if err := ts.TopicManager.CreateTopic("test-topic", 0); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	ctx := context.Background()
	producerClient, err := client.NewProducerClient(ts.Addr)
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()
	resp, err := producerClient.Produce(ctx, &protocol.ProduceRequest{
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
	ts := StartTestServer(t, "leader")
	if err := ts.TopicManager.CreateTopic("test-topic", 0); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	defer ts.Cleanup()

	ctx := context.Background()
	producerClient, err := client.NewProducerClient(ts.Addr)
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()
	for i := 0; i < 100; i++ {
		resp, err := producerClient.Produce(ctx, &protocol.ProduceRequest{
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
	ts := StartTestServer(t, "leader")
	if err := ts.TopicManager.CreateTopic("test-topic", 0); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	defer ts.Cleanup()

	ctx := context.Background()
	producerClient, err := client.NewProducerClient(ts.Addr)
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()
	messages := make([][]byte, 0)
	for i := 0; i < 100; i++ {
		messages = append(messages, []byte(fmt.Sprintf("message-%d", i)))
	}
	resp, err := producerClient.ProduceBatch(ctx, &protocol.ProduceBatchRequest{
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
	ts := StartTestServer(t, "leader")
	if err := ts.TopicManager.CreateTopic("test-topic", 0); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	defer ts.Cleanup()

	ctx := context.Background()
	producerClient, err := client.NewProducerClient(ts.Addr)
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()
	resp, err := producerClient.Produce(ctx, &protocol.ProduceRequest{
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
	ts := StartTestServer(b, "leader")
	if err := ts.TopicManager.CreateTopic("test-topic", 0); err != nil {
		b.Fatalf("CreateTopic: %v", err)
	}
	defer ts.Cleanup()

	producerClient, err := client.NewProducerClient(ts.Addr)
	if err != nil {
		b.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()
	ctx := context.Background()
	req := &protocol.ProduceRequest{
		Topic: "test-topic",
		Value: []byte("bench-value"),
		Acks:  protocol.AckLeader,
	}

	for b.Loop() {
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

func TestProduceBatch(t *testing.T) {
	ts := StartTestServer(t, "leader")
	if err := ts.TopicManager.CreateTopic("test-topic", 0); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	defer ts.Cleanup()

	producerClient, err := client.NewProducerClient(ts.Addr)
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()
	ctx := context.Background()
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
}

func TestProduceBatch_TopicNotFound(t *testing.T) {
	ts := StartTestServer(t, "leader")
	defer ts.Cleanup()

	producerClient, err := client.NewProducerClient(ts.Addr)
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
}

func TestProduceBatch_InvalidArgs(t *testing.T) {
	ts := StartTestServer(t, "leader")
	if err := ts.TopicManager.CreateTopic("test-topic", 0); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	defer ts.Cleanup()

	producerClient, err := client.NewProducerClient(ts.Addr)
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
}

func BenchmarkProduceBatch(b *testing.B) {
	ts := StartTestServer(b, "leader")
	if err := ts.TopicManager.CreateTopic("test-topic", 0); err != nil {
		b.Fatalf("CreateTopic: %v", err)
	}
	defer ts.Cleanup()

	producerClient, err := client.NewProducerClient(ts.Addr)
	if err != nil {
		b.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()
	ctx := context.Background()
	req := &protocol.ProduceBatchRequest{
		Topic:  "test-topic",
		Values: [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("f"), []byte("g"), []byte("h"), []byte("i"), []byte("j")},
		Acks:   protocol.AckLeader,
	}

	for b.Loop() {
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
