package transport

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/protocol"
)

func TestTransport_ListenAndServe_TransportClient_Call(t *testing.T) {
	addr := "127.0.0.1:19999"
	tr := NewTransport()

	var received protocol.ProduceRequest
	var receivedMu sync.Mutex
	done := make(chan struct{})

	tr.RegisterHandler(protocol.MsgProduce, func(ctx context.Context, msg any) (any, error) {
		receivedMu.Lock()
		received = msg.(protocol.ProduceRequest)
		receivedMu.Unlock()
		close(done)
		return protocol.ProduceResponse{Offset: 1}, nil
	})

	go func() {
		_ = tr.ListenAndServe(addr)
	}()
	time.Sleep(50 * time.Millisecond) // allow server to bind

	client, err := Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	req := protocol.ProduceRequest{Topic: "test", Value: []byte("ping"), Acks: protocol.AckLeader}
	resp, err := client.Call(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.(protocol.ProduceResponse).Offset != 1 {
		t.Errorf("expected offset 1, got %v", resp)
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
	tr := NewTransport()
	err := tr.ListenAndServe("invalid:addr:here")
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
}

func TestTransportClient_Dial_Unreachable(t *testing.T) {
	_, err := Dial("127.0.0.1:19998")
	if err == nil {
		t.Fatal("expected error when dialing unreachable address")
	}
}
