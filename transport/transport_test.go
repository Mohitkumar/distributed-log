package transport

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/protocol"
)

func TestTransport_ListenAndServe_Send(t *testing.T) {
	addr := "127.0.0.1:19999"
	srv := NewTransport(addr)

	var received protocol.ProduceRequest
	var receivedMu sync.Mutex
	done := make(chan struct{})

	srv.RegisterHandler(protocol.MsgProduce, func(ctx context.Context, msg any) (any, error) {
		receivedMu.Lock()
		received = msg.(protocol.ProduceRequest)
		receivedMu.Unlock()
		close(done)
		return protocol.ProduceResponse{Offset: 1}, nil
	})

	go func() {
		_ = srv.ListenAndServe()
	}()
	time.Sleep(50 * time.Millisecond) // allow server to bind

	client := NewTransport(addr)
	req := protocol.ProduceRequest{Topic: "test", Value: []byte("ping"), Acks: protocol.AckLeader}
	if err := client.Send(req); err != nil {
		t.Fatal(err)
	}

	<-done
	receivedMu.Lock()
	got := received
	receivedMu.Unlock()
	if got.Topic != "test" || string(got.Value) != "ping" {
		t.Errorf("server received Topic=%q Value=%q, want Topic=test Value=ping", got.Topic, string(got.Value))
	}
}

func TestTransport_ListenAndServe_InvalidAddr(t *testing.T) {
	tr := NewTransport("invalid:addr:here")
	err := tr.ListenAndServe()
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
}

func TestTransport_Send_Unreachable(t *testing.T) {
	// Use an address that is not listening (no server).
	addr := "127.0.0.1:19998"
	client := NewTransport(addr)
	req := protocol.ProduceRequest{Topic: "test", Value: []byte("x"), Acks: protocol.AckNone}
	err := client.Send(req)
	if err == nil {
		t.Fatal("expected error when sending to unreachable address")
	}
}
