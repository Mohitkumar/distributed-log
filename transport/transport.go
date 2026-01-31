package transport

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/mohitkumar/mlog/protocol"
)

// Transport manages TCP connections with a length-prefixed frame protocol.
// Producer, consumer, and replication use it to Send and Receive raw bytes.
type Transport struct {
	Addr     string
	Codec    *protocol.Codec
	handlers map[protocol.MessageType]func(context.Context, any) (any, error)
}

func NewTransport(addr string) *Transport {
	return &Transport{
		Addr:     addr,
		Codec:    &protocol.Codec{},
		handlers: make(map[protocol.MessageType]func(context.Context, any) (any, error)),
	}
}

func (t *Transport) RegisterHandler(msgType protocol.MessageType, handler func(context.Context, any) (any, error)) {
	t.handlers[msgType] = handler
}

func (t *Transport) ListenAndServe() error {
	ln, err := net.Listen("tcp", t.Addr)
	if err != nil {
		return err
	}
	fmt.Println("Listening on", ln.Addr())
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go t.handleConn(conn)
	}
}

func (t *Transport) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		mType, msg, err := t.Codec.Decode(conn)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Read error:", err)
			}
			return
		}
		resp, err := t.handlers[mType](context.Background(), msg)
		if err != nil {
			fmt.Println("Handler error:", err)
			continue
		}
		if err := t.Codec.Encode(conn, resp); err != nil {
			fmt.Println("Encode error:", err)
			continue
		}
	}
}

func (t *Transport) Send(msg any) error {
	conn, err := net.Dial("tcp", t.Addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	return t.Codec.Encode(conn, msg)
}
