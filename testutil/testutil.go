package testutil

import (
	"path"
	"sync"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/broker"
	consumermgr "github.com/mohitkumar/mlog/consumer"
	"github.com/mohitkumar/mlog/node"
	"github.com/mohitkumar/mlog/transport"
)

// TransportHandler handles TCP transport connections (e.g. rpc.Server).
type TransportHandler interface {
	ServeTransportConn(conn *transport.Conn)
}

// TestServerComponents contains the components needed to create a test server.
type TestServerComponents struct {
	TopicManager    *node.TopicManager
	ConsumerManager *consumermgr.ConsumerManager
	BrokerManager   *broker.BrokerManager
	Broker          *broker.Broker
	BaseDir         string
}

// TestServer represents a test TCP transport server with its components.
type TestServer struct {
	*TestServerComponents
	Listener *transport.Listener
	Addr     string
	conn     *transport.Conn
	connMu   sync.Mutex
	tr       *transport.Transport
	handler  TransportHandler
}

// Cleanup closes all resources associated with the test server.
func (ts *TestServer) Cleanup() {
	ts.connMu.Lock()
	if ts.conn != nil {
		_ = ts.conn.Close()
		ts.conn = nil
	}
	ts.connMu.Unlock()

	if ts.Listener != nil {
		_ = ts.Listener.Close()
	}
	if ts.Broker != nil {
		_ = ts.Broker.Close()
	}
}

// GetConn returns a TCP transport connection to the test server, reusing the connection if available.
func (ts *TestServer) GetConn() (*transport.Conn, error) {
	ts.connMu.Lock()
	defer ts.connMu.Unlock()

	if ts.conn != nil {
		return ts.conn, nil
	}

	conn, err := ts.tr.Connect(ts.Addr)
	if err != nil {
		return nil, err
	}
	ts.conn = conn
	return ts.conn, nil
}

// SetupTestServerComponents creates the components needed for a test server.
func SetupTestServerComponents(t testing.TB, nodeID string, baseDirSuffix string) *TestServerComponents {
	t.Helper()

	baseDir := path.Join(t.TempDir(), baseDirSuffix)
	bm := broker.NewBrokerManager()
	b := broker.NewBroker(nodeID, "127.0.0.1:0")
	bm.AddBroker(b)

	topicMgr, err := node.NewTopicManager(baseDir, bm, b)
	if err != nil {
		t.Fatalf("NewTopicManager: %v", err)
	}

	consumerMgr, err := consumermgr.NewConsumerManager(baseDir)
	if err != nil {
		t.Fatalf("NewConsumerManager: %v", err)
	}

	return &TestServerComponents{
		TopicManager:    topicMgr,
		ConsumerManager: consumerMgr,
		BrokerManager:   bm,
		Broker:          b,
		BaseDir:         baseDir,
	}
}

// StartTestServer starts a TCP transport server using the provided components and handler.
func StartTestServer(t testing.TB, comps *TestServerComponents, handler TransportHandler) *TestServer {
	t.Helper()

	tr := transport.NewTransport()
	ln, err := tr.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	comps.Broker.Addr = ln.Addr().String()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handler.ServeTransportConn(conn)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	return &TestServer{
		TestServerComponents: comps,
		Listener:             ln,
		Addr:                 ln.Addr().String(),
		tr:                   tr,
		handler:              handler,
	}
}

// SetupTestServerWithTopic creates a test server and creates a topic with the specified replica count.
// handlerFactory is called with the components to create the transport handler (e.g. rpc.NewServer(comps.TopicManager, comps.ConsumerManager)).
func SetupTestServerWithTopic(t testing.TB, nodeID string, baseDirSuffix string, topic string, replicaCount int, handlerFactory func(*TestServerComponents) TransportHandler) *TestServer {
	t.Helper()

	comps := SetupTestServerComponents(t, nodeID, baseDirSuffix)
	if err := comps.TopicManager.CreateTopic(topic, replicaCount); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	return StartTestServer(t, comps, handlerFactory(comps))
}

// SetupTwoTestServers creates two test servers (useful for replication tests).
func SetupTwoTestServers(t testing.TB, handlerFactory func(*TestServerComponents) TransportHandler) (*TestServer, *TestServer) {
	t.Helper()

	leaderComps := SetupTestServerComponents(t, "node-1", "leader")
	followerComps := SetupTestServerComponents(t, "node-2", "follower")

	leaderComps.BrokerManager.AddBroker(followerComps.Broker)
	followerComps.BrokerManager.AddBroker(leaderComps.Broker)

	leader := StartTestServer(t, leaderComps, handlerFactory(leaderComps))
	follower := StartTestServer(t, followerComps, handlerFactory(followerComps))

	return leader, follower
}
