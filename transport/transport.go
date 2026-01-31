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
	Codec    *protocol.Codec
	handlers map[protocol.MessageType]func(context.Context, any) (any, error)
	ln       net.Listener
}

func NewTransport() *Transport {
	return &Transport{
		Codec:    &protocol.Codec{},
		handlers: make(map[protocol.MessageType]func(context.Context, any) (any, error)),
	}
}

func (t *Transport) RegisterHandler(msgType protocol.MessageType, handler func(context.Context, any) (any, error)) {
	t.handlers[msgType] = handler
}

// Listen binds to addr and returns the listener. The transport keeps a reference so Close() can stop the server.
func (t *Transport) Listen(addr string) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	t.ln = ln
	return ln, nil
}

// Addr returns the listener's address (e.g. "127.0.0.1:12345") if Listen was called, otherwise "".
func (t *Transport) Addr() string {
	if t.ln != nil {
		return t.ln.Addr().String()
	}
	return ""
}

// Close closes the listener if one was created by Listen/ListenAndServe.
func (t *Transport) Close() error {
	if t.ln != nil {
		err := t.ln.Close()
		t.ln = nil
		return err
	}
	return nil
}

// Serve accepts connections on ln and handles them with the registered handlers.
func (t *Transport) Serve(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go t.handleConn(conn)
	}
}

func (t *Transport) ListenAndServe(addr string) error {
	ln, err := t.Listen(addr)
	if err != nil {
		return err
	}
	fmt.Println("Listening on", ln.Addr())
	t.Serve(ln)
	return nil
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
		handler := t.handlers[mType]
		if handler == nil {
			fmt.Println("No handler for message type:", mType)
			continue
		}
		resp, err := handler(context.Background(), msg)
		if err != nil {
			fmt.Println("Handler error:", err)
			return // close conn so client gets an error instead of hanging
		}
		if err := t.Codec.Encode(conn, resp); err != nil {
			fmt.Println("Encode error:", err)
			return
		}
	}
}

// TransportClient holds a persistent connection for request-response calls (avoids dialing each time).
type TransportClient struct {
	conn  net.Conn
	codec *protocol.Codec
}

// Dial creates a TransportClient connected to addr. Call Close when done.
func Dial(addr string) (*TransportClient, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &TransportClient{conn: conn, codec: &protocol.Codec{}}, nil
}

// Call encodes msg, writes to the connection, reads one decoded response, and returns it.
func (c *TransportClient) Call(msg any) (any, error) {
	if err := c.codec.Encode(c.conn, msg); err != nil {
		return nil, err
	}
	_, resp, err := c.codec.Decode(c.conn)
	return resp, err
}

// Close closes the connection.
func (c *TransportClient) Close() error {
	return c.conn.Close()
}
