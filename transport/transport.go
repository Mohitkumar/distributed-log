package transport

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/mohitkumar/mlog/protocol"
)

// StreamHandler is used for RPCs where the server writes multiple response frames on the same connection.
// After the handler returns, the connection is reused for the next request.
type StreamHandler func(ctx context.Context, msg any, conn net.Conn, codec *protocol.Codec) error

// Transport manages TCP connections with a length-prefixed frame protocol.
// Producer, consumer, and replication use it to Send and Receive raw bytes.
type Transport struct {
	Codec          *protocol.Codec
	handlers       map[protocol.MessageType]func(context.Context, any) (any, error)
	streamHandlers map[protocol.MessageType]StreamHandler
	ln             net.Listener
}

func NewTransport() *Transport {
	return &Transport{
		Codec:          &protocol.Codec{},
		handlers:       make(map[protocol.MessageType]func(context.Context, any) (any, error)),
		streamHandlers: make(map[protocol.MessageType]StreamHandler),
	}
}

func (t *Transport) RegisterHandler(msgType protocol.MessageType, handler func(context.Context, any) (any, error)) {
	t.handlers[msgType] = handler
}

// RegisterStreamHandler registers a handler that writes multiple response frames to the connection.
// The handler should encode and write to conn using codec; when it returns, the connection loop reads the next request.
func (t *Transport) RegisterStreamHandler(msgType protocol.MessageType, handler StreamHandler) {
	t.streamHandlers[msgType] = handler
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

// ServeWithListener sets ln as the transport's listener (so Close works) and serves on it in a goroutine.
// Use this when the listener was created outside the transport (e.g. tests that need the bound address before creating the node).
func (t *Transport) ServeWithListener(ln net.Listener) {
	t.ln = ln
	go t.Serve(ln)
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
		if streamHandler := t.streamHandlers[mType]; streamHandler != nil {
			if err := streamHandler(context.Background(), msg, conn, t.Codec); err != nil {
				fmt.Println("Stream handler error:", err)
				return
			}
			continue
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
	if err := c.Write(msg); err != nil {
		return nil, err
	}
	return c.Read()
}

// Write encodes and writes one message to the connection (for use with stream RPCs: write once, then Read multiple times).
func (c *TransportClient) Write(msg any) error {
	return c.codec.Encode(c.conn, msg)
}

// Read decodes and returns one message from the connection.
func (c *TransportClient) Read() (any, error) {
	_, resp, err := c.codec.Decode(c.conn)
	return resp, err
}

// Close closes the connection.
func (c *TransportClient) Close() error {
	return c.conn.Close()
}
