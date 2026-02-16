package transport

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/mohitkumar/mlog/protocol"
)

// StreamHandler is used for RPCs where the server writes multiple response frames on the same connection.
// After the handler returns, the connection is reused for the next request.
type StreamHandler func(ctx context.Context, msg any, conn net.Conn, codec *protocol.Codec) error

// Transport manages TCP connections with a length-prefixed frame protocol.
// Producer, consumer, and replication use it to Send and Receive raw bytes.
type Transport struct {
	Codec    *protocol.Codec
	handlers map[protocol.MessageType]func(context.Context, any) (any, error)
	ln       net.Listener
	mu       sync.Mutex              // protects conns
	conns    map[net.Conn]struct{}    // active connections for graceful shutdown
	wg       sync.WaitGroup           // tracks active connection goroutines
}

func NewTransport() *Transport {
	return &Transport{
		Codec:    &protocol.Codec{},
		handlers: make(map[protocol.MessageType]func(context.Context, any) (any, error)),
		conns:    make(map[net.Conn]struct{}),
	}
}

func (t *Transport) RegisterHandler(msgType protocol.MessageType, handler func(context.Context, any) (any, error)) {
	t.handlers[msgType] = handler
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

// Close stops accepting new connections, closes all active connections, and waits for goroutines to exit.
func (t *Transport) Close() error {
	var err error
	if t.ln != nil {
		err = t.ln.Close()
		t.ln = nil
	}
	// Close all tracked connections so handleConn goroutines unblock.
	t.mu.Lock()
	for c := range t.conns {
		_ = c.Close()
	}
	t.mu.Unlock()
	t.wg.Wait()
	return err
}

func (t *Transport) trackConn(c net.Conn) {
	t.mu.Lock()
	t.conns[c] = struct{}{}
	t.mu.Unlock()
}

func (t *Transport) untrackConn(c net.Conn) {
	t.mu.Lock()
	delete(t.conns, c)
	t.mu.Unlock()
}

func (t *Transport) Serve(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			t.handleConn(conn)
		}()
	}
}

func (t *Transport) ListenAndServe(addr string) error {
	ln, err := t.Listen(addr)
	if err != nil {
		return err
	}
	slog.Info("listening", "addr", ln.Addr())
	t.Serve(ln)
	return nil
}

const (
	// idleTimeout is how long a connection can sit idle before we close it.
	idleTimeout = 5 * time.Minute
	// handlerTimeout is the max time a single handler invocation may take.
	handlerTimeout = 30 * time.Second
)

func (t *Transport) handleConn(conn net.Conn) {
	t.trackConn(conn)
	defer func() {
		t.untrackConn(conn)
		conn.Close()
	}()
	for {
		// Set a read deadline so idle connections don't hang forever.
		_ = conn.SetReadDeadline(time.Now().Add(idleTimeout))

		mType, msg, err := t.Codec.Decode(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				slog.Warn("read error", "remote", conn.RemoteAddr(), "err", err)
			}
			return
		}
		handler := t.handlers[mType]
		if handler == nil {
			slog.Warn("no handler", "msgType", mType, "remote", conn.RemoteAddr())
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
		resp, err := handler(ctx, msg)
		cancel()

		if err != nil {
			slog.Debug("handler error", "msgType", mType, "err", err)
			code := protocol.CodeUnknown
			message := err.Error()
			var rpcErr *protocol.RPCError
			if errors.As(err, &rpcErr) {
				code = rpcErr.Code
				message = rpcErr.Message
			}
			_ = t.Codec.Encode(conn, &protocol.RPCErrorResponse{Code: code, Message: message})
			continue
		}
		if err := t.Codec.Encode(conn, resp); err != nil {
			slog.Warn("encode error", "remote", conn.RemoteAddr(), "err", err)
			return
		}
	}
}

type TransportClient struct {
	mu    sync.Mutex
	conn  net.Conn
	codec *protocol.Codec
}

// Dial opens a single TCP connection to addr and enables keepalive so the connection
// stays alive for node-to-node RPC/stream use (one connection per peer).
func Dial(addr string) (*TransportClient, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(30 * time.Second)
	}
	return &TransportClient{conn: conn, codec: &protocol.Codec{}}, nil
}

// Call sends a request and reads the response. Safe for concurrent use.
func (c *TransportClient) Call(msg any) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.codec.Encode(c.conn, msg); err != nil {
		return nil, err
	}
	return c.readResponse()
}

// ReadResponse reads the next frame and returns the decoded value. If the server sent an RPC
// error frame, returns (nil, *protocol.RPCError) so the client can check e.Code.
func (c *TransportClient) ReadResponse() (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readResponse()
}

// readResponse is the internal unlocked version.
func (c *TransportClient) readResponse() (any, error) {
	mType, value, err := c.codec.Decode(c.conn)
	if err != nil {
		return nil, err
	}
	if mType == protocol.MsgRPCError {
		if r, ok := value.(protocol.RPCErrorResponse); ok {
			return nil, &protocol.RPCError{Code: r.Code, Message: r.Message}
		}
		return nil, &protocol.RPCError{Code: protocol.CodeUnknown, Message: "rpc error"}
	}
	return value, nil
}

// Write sends a single frame. Safe for concurrent use.
func (c *TransportClient) Write(msg any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.codec.Encode(c.conn, msg)
}

// Read reads the next frame. Safe for concurrent use.
func (c *TransportClient) Read() (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, resp, err := c.codec.Decode(c.conn)
	return resp, err
}

func (c *TransportClient) Close() error {
	return c.conn.Close()
}
