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

func (t *Transport) RegisterStreamHandler(msgType protocol.MessageType, handler StreamHandler) {
	t.streamHandlers[msgType] = handler
}

func (t *Transport) Listen(addr string) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	t.ln = ln
	return ln, nil
}

func (t *Transport) Addr() string {
	if t.ln != nil {
		return t.ln.Addr().String()
	}
	return ""
}

func (t *Transport) Close() error {
	if t.ln != nil {
		err := t.ln.Close()
		t.ln = nil
		return err
	}
	return nil
}

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

type TransportClient struct {
	conn  net.Conn
	codec *protocol.Codec
}

func Dial(addr string) (*TransportClient, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &TransportClient{conn: conn, codec: &protocol.Codec{}}, nil
}

func (c *TransportClient) Call(msg any) (any, error) {
	if err := c.Write(msg); err != nil {
		return nil, err
	}
	return c.Read()
}

func (c *TransportClient) Write(msg any) error {
	return c.codec.Encode(c.conn, msg)
}

func (c *TransportClient) Read() (any, error) {
	_, resp, err := c.codec.Decode(c.conn)
	return resp, err
}

func (c *TransportClient) Close() error {
	return c.conn.Close()
}
