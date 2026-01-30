package transport

import (
	"bytes"
	"net"
	"sync"
	"testing"
)

func TestTransport_Connect_Send_Receive(t *testing.T) {
	tr := NewTransport()
	ln, err := tr.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	addr := ln.Addr().String()

	var clientRecv []byte
	var clientErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		client, err := tr.Connect(addr)
		if err != nil {
			clientErr = err
			return
		}
		defer client.Close()
		if err := client.Send([]byte("ping")); err != nil {
			clientErr = err
			return
		}
		clientRecv, clientErr = client.Receive()
	}()

	serverConn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	recv, err := serverConn.Receive()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(recv, []byte("ping")) {
		t.Errorf("server recv = %q, want ping", recv)
	}
	if err := serverConn.Send([]byte("pong")); err != nil {
		t.Fatal(err)
	}
	serverConn.Close()

	wg.Wait()
	if clientErr != nil {
		t.Fatal(clientErr)
	}
	if !bytes.Equal(clientRecv, []byte("pong")) {
		t.Errorf("client recv = %q, want pong", clientRecv)
	}
}

func TestTransport_Listen_InvalidAddr(t *testing.T) {
	tr := NewTransport()
	_, err := tr.Listen("invalid:addr:here")
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
}

func TestConn_Close(t *testing.T) {
	tr := NewTransport()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	conn, err := tr.Connect(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := conn.Send([]byte("x")); err != ErrClosed {
		t.Errorf("Send after Close: got %v, want ErrClosed", err)
	}
}
