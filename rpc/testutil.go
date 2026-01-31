package rpc

import (
	"path"
	"testing"

	"github.com/mohitkumar/mlog/broker"
	consumermgr "github.com/mohitkumar/mlog/consumer"
	"github.com/mohitkumar/mlog/node"
)

// TestServerComponents contains the components needed for a test server.
type TestServerComponents struct {
	TopicManager    *node.TopicManager
	ConsumerManager *consumermgr.ConsumerManager
	BrokerManager   *broker.BrokerManager
	Broker          *broker.Broker
	BaseDir         string
	Addr            string
}

// TestServer represents a test TCP transport server with its components.
type TestServer struct {
	*TestServerComponents
	Addr string
	srv  *RpcServer
}

// Cleanup closes all resources associated with the test server.
func (ts *TestServer) Cleanup() {
	_ = ts.srv.Stop()
	_ = ts.Broker.Close()
}

// SetupTestServerComponents creates the components needed for a test server.
func SetupTestServerComponents(t testing.TB, addr string, nodeID string, baseDirSuffix string) *TestServerComponents {
	t.Helper()
	baseDir := path.Join(t.TempDir(), baseDirSuffix)
	bm := broker.NewBrokerManager()
	b := broker.NewBroker(nodeID, addr)
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
		Addr:            addr,
	}
}

// StartTestServer starts a TCP transport server using the provided components and RPC server (handler-style).
func StartTestServer(t testing.TB, comps *TestServerComponents) *TestServer {
	t.Helper()

	srv := NewServer(comps.Addr, comps.TopicManager, comps.ConsumerManager)
	srv.Start()

	return &TestServer{
		TestServerComponents: comps,
		Addr:                 srv.Addr,
		srv:                  srv,
	}
}

// SetupTwoTestServers creates two test servers (useful for replication tests).
func SetupTwoTestServers(t testing.TB) (*TestServer, *TestServer) {
	t.Helper()

	leaderComps := SetupTestServerComponents(t, "127.0.0.1:0", "node-1", "leader")
	followerComps := SetupTestServerComponents(t, "127.0.0.1:1", "node-2", "follower")

	leaderComps.BrokerManager.AddBroker(followerComps.Broker)
	followerComps.BrokerManager.AddBroker(leaderComps.Broker)

	leader := StartTestServer(t, leaderComps)
	follower := StartTestServer(t, followerComps)

	return leader, follower
}
