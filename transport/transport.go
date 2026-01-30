package transport

import (
	"bufio"
	"net"
	"sync"
)

// Transport manages TCP connections with a length-prefixed frame protocol.
// Producer, consumer, and replication use it to Send and Receive raw bytes.
type Transport struct {
	// MaxFrameSize can be overridden per-transport; default is from codec.
	MaxFrameSize uint32
}

func NewTransport() *Transport {
	return &Transport{
		MaxFrameSize: MaxFrameSize,
	}
}

// Conn is a TCP connection that encodes/decodes length-prefixed frames.
// Safe for concurrent Send; Receive should typically be used from a single goroutine per Conn.
type Conn struct {
	conn   net.Conn
	rbuf   *bufio.Reader
	wbuf   *bufio.Writer
	mu     sync.Mutex
	closed bool
}

// Connect establishes a TCP connection to addr and returns a frame-based Conn.
func (t *Transport) Connect(addr string) (*Conn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return t.wrapConn(conn), nil
}

// wrapConn wraps an existing net.Conn with buffered I/O and frame codec.
func (t *Transport) wrapConn(conn net.Conn) *Conn {
	return &Conn{
		conn: conn,
		rbuf: bufio.NewReader(conn),
		wbuf: bufio.NewWriter(conn),
	}
}

// Send encodes payload as a length-prefixed frame and writes it. Thread-safe.
func (c *Conn) Send(payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	return EncodeFrame(c.wbuf, payload)
}

// Receive reads one length-prefixed frame and returns the payload.
// Typically used from a single goroutine; not safe for concurrent use with other Receive calls.
func (c *Conn) Receive() ([]byte, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	c.mu.Unlock()
	return DecodeFrame(c.rbuf)
}

// Close closes the underlying TCP connection.
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return c.conn.Close()
}

// RemoteAddr returns the remote network address.
func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// LocalAddr returns the local network address.
func (c *Conn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// Listener is a TCP listener that accepts frame-based Conns.
type Listener struct {
	ln        net.Listener
	transport *Transport
}

// Listen starts listening on addr and returns a Listener that accepts Conns.
func (t *Transport) Listen(addr string) (*Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Listener{ln: ln, transport: t}, nil
}

// Accept blocks until a new connection is established and returns a frame-based Conn.
func (l *Listener) Accept() (*Conn, error) {
	conn, err := l.ln.Accept()
	if err != nil {
		return nil, err
	}
	return l.transport.wrapConn(conn), nil
}

// Addr returns the listener's address.
func (l *Listener) Addr() net.Addr {
	return l.ln.Addr()
}

// Close stops accepting new connections.
func (l *Listener) Close() error {
	return l.ln.Close()
}
