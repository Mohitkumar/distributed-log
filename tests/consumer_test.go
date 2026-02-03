package tests

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/protocol"
)

func TestFetch(t *testing.T) {
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
	_, err = producerClient.Produce(ctx, &protocol.ProduceRequest{
		Topic: "test-topic",
		Value: []byte("hello"),
		Acks:  protocol.AckLeader,
	})
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	consumerClient, err := client.NewConsumerClient(ts.Addr)
	if err != nil {
		t.Fatalf("NewConsumerClient: %v", err)
	}
	defer consumerClient.Close()
	resp, err := consumerClient.Fetch(ctx, &protocol.FetchRequest{
		Id:     "test-consumer",
		Topic:  "test-topic",
		Offset: 0,
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if resp.Entry == nil {
		t.Fatal("expected non-nil entry")
	}
	if resp.Entry.Offset != 0 {
		t.Fatalf("expected offset 0, got %d", resp.Entry.Offset)
	}
	if string(resp.Entry.Value) != "hello" {
		t.Fatalf("expected value 'hello', got %s", string(resp.Entry.Value))
	}
}

func TestFetch_TopicNotFound(t *testing.T) {
	ts := StartTestServer(t, "leader")
	defer ts.Cleanup()

	ctx := context.Background()
	consumerClient, err := client.NewConsumerClient(ts.Addr)
	if err != nil {
		t.Fatalf("NewConsumerClient: %v", err)
	}
	defer consumerClient.Close()
	_, err = consumerClient.Fetch(ctx, &protocol.FetchRequest{
		Id:     "test-consumer",
		Topic:  "nonexistent-topic",
		Offset: 0,
	})
	if err == nil {
		t.Fatal("expected error for unknown topic")
	}
}

func TestFetch_InvalidArguments(t *testing.T) {
	ts := StartTestServer(t, "leader")
	defer ts.Cleanup()

	ctx := context.Background()
	consumerClient, err := client.NewConsumerClient(ts.Addr)
	if err != nil {
		t.Fatalf("NewConsumerClient: %v", err)
	}
	defer consumerClient.Close()
	_, err = consumerClient.Fetch(ctx, &protocol.FetchRequest{
		Id:     "test-consumer",
		Offset: 0,
	})
	if err == nil {
		t.Fatal("expected error for missing topic")
	}
}

func TestFetchStream(t *testing.T) {
	ts := StartTestServer(t, "leader")
	if err := ts.TopicManager.CreateTopic("test-topic", 0); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	defer ts.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	producerClient, err := client.NewProducerClient(ts.Addr)
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()
	for i := 0; i < 3; i++ {
		_, err = producerClient.Produce(ctx, &protocol.ProduceRequest{
			Topic: "test-topic",
			Value: []byte("message-" + string(rune('0'+i))),
			Acks:  protocol.AckLeader,
		})
		if err != nil {
			t.Fatalf("Produce: %v", err)
		}
	}

	time.Sleep(300 * time.Millisecond)

	consumerClient, err := client.NewConsumerClient(ts.Addr)
	if err != nil {
		t.Fatalf("NewConsumerClient: %v", err)
	}
	defer consumerClient.Close()
	stream, err := consumerClient.FetchStream(ctx, &protocol.FetchRequest{
		Id:     "test-consumer",
		Topic:  "test-topic",
		Offset: 0,
	})
	if err != nil {
		t.Fatalf("FetchStream: %v", err)
	}

	received := 0
	seenOffsets := make(map[uint64]bool)
	for received < 3 {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			t.Fatalf("stream.Recv: %v", err)
		}
		if resp.Entry == nil {
			t.Fatal("expected non-nil entry")
		}
		if !seenOffsets[resp.Entry.Offset] {
			seenOffsets[resp.Entry.Offset] = true
			received++
		}
		if received >= 1 {
			break
		}
	}

	if received < 1 {
		t.Fatalf("expected at least 1 message from stream, got %d", received)
	}
}

func TestCommitOffset(t *testing.T) {
	ts := StartTestServer(t, "leader")
	if err := ts.TopicManager.CreateTopic("test-topic", 0); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	defer ts.Cleanup()

	ctx := context.Background()
	consumerClient, err := client.NewConsumerClient(ts.Addr)
	if err != nil {
		t.Fatalf("NewConsumerClient: %v", err)
	}
	defer consumerClient.Close()
	resp, err := consumerClient.CommitOffset(ctx, &protocol.CommitOffsetRequest{
		Id:     "test-consumer",
		Topic:  "test-topic",
		Offset: 42,
	})
	if err != nil {
		t.Fatalf("CommitOffset: %v", err)
	}
	if !resp.Success {
		t.Fatal("expected success=true")
	}
}

func TestCommitOffset_InvalidArguments(t *testing.T) {
	ts := StartTestServer(t, "leader")
	defer ts.Cleanup()

	ctx := context.Background()
	consumerClient, err := client.NewConsumerClient(ts.Addr)
	if err != nil {
		t.Fatalf("NewConsumerClient: %v", err)
	}
	defer consumerClient.Close()
	_, err = consumerClient.CommitOffset(ctx, &protocol.CommitOffsetRequest{
		Id:     "test-consumer",
		Offset: 42,
	})
	if err == nil {
		t.Fatal("expected error for missing topic")
	}
}

func TestFetchOffset(t *testing.T) {
	ts := StartTestServer(t, "leader")
	if err := ts.TopicManager.CreateTopic("test-topic", 0); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	defer ts.Cleanup()

	ctx := context.Background()
	consumerClient, err := client.NewConsumerClient(ts.Addr)
	if err != nil {
		t.Fatalf("NewConsumerClient: %v", err)
	}
	defer consumerClient.Close()

	resp, err := consumerClient.FetchOffset(ctx, &protocol.FetchOffsetRequest{
		Id:    "test-consumer",
		Topic: "test-topic",
	})
	if err != nil {
		t.Fatalf("FetchOffset: %v", err)
	}
	if resp.Offset != 0 {
		t.Fatalf("expected offset 0 for new consumer, got %d", resp.Offset)
	}

	_, err = consumerClient.CommitOffset(ctx, &protocol.CommitOffsetRequest{
		Id:     "test-consumer",
		Topic:  "test-topic",
		Offset: 10,
	})
	if err != nil {
		t.Fatalf("CommitOffset: %v", err)
	}

	resp, err = consumerClient.FetchOffset(ctx, &protocol.FetchOffsetRequest{
		Id:    "test-consumer",
		Topic: "test-topic",
	})
	if err != nil {
		t.Fatalf("FetchOffset: %v", err)
	}
	if resp.Offset != 10 {
		t.Fatalf("expected offset 10, got %d", resp.Offset)
	}
}

func TestFetchOffset_InvalidArguments(t *testing.T) {
	ts := StartTestServer(t, "leader")
	defer ts.Cleanup()

	ctx := context.Background()
	consumerClient, err := client.NewConsumerClient(ts.Addr)
	if err != nil {
		t.Fatalf("NewConsumerClient: %v", err)
	}
	defer consumerClient.Close()
	_, err = consumerClient.FetchOffset(ctx, &protocol.FetchOffsetRequest{
		Id: "test-consumer",
	})
	if err == nil {
		t.Fatal("expected error for missing topic")
	}
}

func TestFetch_WithCachedOffset(t *testing.T) {
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
	for i := 0; i < 3; i++ {
		_, err := producerClient.Produce(ctx, &protocol.ProduceRequest{
			Topic: "test-topic",
			Value: []byte("message"),
			Acks:  protocol.AckLeader,
		})
		if err != nil {
			t.Fatalf("Produce: %v", err)
		}
	}

	consumerClient, err := client.NewConsumerClient(ts.Addr)
	if err != nil {
		t.Fatalf("NewConsumerClient: %v", err)
	}
	defer consumerClient.Close()

	_, err = consumerClient.Fetch(ctx, &protocol.FetchRequest{
		Id:     "test-consumer",
		Topic:  "test-topic",
		Offset: 0,
	})
	if err != nil {
		t.Fatalf("Fetch offset 0: %v", err)
	}

	_, err = consumerClient.CommitOffset(ctx, &protocol.CommitOffsetRequest{
		Id:     "test-consumer",
		Topic:  "test-topic",
		Offset: 1,
	})
	if err != nil {
		t.Fatalf("CommitOffset: %v", err)
	}

	fetchOffsetResp, err := consumerClient.FetchOffset(ctx, &protocol.FetchOffsetRequest{
		Id:    "test-consumer",
		Topic: "test-topic",
	})
	if err != nil {
		t.Fatalf("FetchOffset: %v", err)
	}
	if fetchOffsetResp.Offset != 1 {
		t.Fatalf("expected cached offset 1, got %d", fetchOffsetResp.Offset)
	}
}

func BenchmarkFetch(b *testing.B) {
	ts := StartTestServer(b, "leader")
	if err := ts.TopicManager.CreateTopic("test-topic", 0); err != nil {
		b.Fatalf("CreateTopic: %v", err)
	}
	defer ts.Cleanup()

	ctx := context.Background()
	producerClient, err := client.NewProducerClient(ts.Addr)
	if err != nil {
		b.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()
	for i := 0; i < 100; i++ {
		_, err = producerClient.Produce(ctx, &protocol.ProduceRequest{
			Topic: "test-topic",
			Value: []byte("bench-value"),
			Acks:  protocol.AckLeader,
		})
		if err != nil {
			b.Fatalf("Produce: %v", err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	consumerClient, err := client.NewConsumerClient(ts.Addr)
	if err != nil {
		b.Fatalf("NewConsumerClient: %v", err)
	}
	defer consumerClient.Close()
	req := &protocol.FetchRequest{
		Id:     "bench-consumer",
		Topic:  "test-topic",
		Offset: 0,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = consumerClient.Fetch(ctx, req)
		if err != nil {
			b.Fatalf("Fetch: %v", err)
		}
	}
}
