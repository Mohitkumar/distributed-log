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
	Addr string // bound address after Start (use for client dial)
	srv  *RpcServer
}

// Cleanup closes all resources associated with the test server.
func (ts *TestServer) Cleanup() {
	_ = ts.srv.Stop()
	_ = ts.Broker.Close()
}

// StartTestServer creates and starts a single test server with its own TopicManager,
// BrokerManager, ConsumerManager, and baseDir.
func StartTestServer(t testing.TB, baseDirSuffix string) *TestServer {
	t.Helper()
	addr := "127.0.0.1:0"
	baseDir := path.Join(t.TempDir(), baseDirSuffix)
	bm := broker.NewBrokerManager()
	b := broker.NewBroker("node-1", addr)
	bm.AddBroker(b)

	topicMgr, err := node.NewTopicManager(baseDir, bm, b)
	if err != nil {
		t.Fatalf("NewTopicManager: %v", err)
	}
	consumerMgr, err := consumermgr.NewConsumerManager(baseDir)
	if err != nil {
		t.Fatalf("NewConsumerManager: %v", err)
	}

	comps := &TestServerComponents{
		TopicManager:    topicMgr,
		ConsumerManager: consumerMgr,
		BrokerManager:   bm,
		Broker:          b,
		BaseDir:         baseDir,
		Addr:            addr,
	}
	return startServer(t, comps)
}

// SetupTwoTestServers creates two test servers with shared BrokerManager but separate
// TopicManager, ConsumerManager, and baseDir per server (leaderBaseDirSuffix, followerBaseDirSuffix).
// Both brokers are in the shared BrokerManager so the leader can create replicas on the follower.
func SetupTwoTestServers(t testing.TB, leaderBaseDirSuffix string, followerBaseDirSuffix string) (*TestServer, *TestServer) {
	t.Helper()

	// Shared broker manager so both servers know about each other
	bm := broker.NewBrokerManager()
	leaderAddr := "127.0.0.1:0"
	followerAddr := "127.0.0.1:0"
	leaderBroker := broker.NewBroker("node-1", leaderAddr)
	followerBroker := broker.NewBroker("node-2", followerAddr)
	bm.AddBroker(leaderBroker)
	bm.AddBroker(followerBroker)

	// Leader: own baseDir, TopicManager, ConsumerManager
	leaderBaseDir := path.Join(t.TempDir(), leaderBaseDirSuffix)
	leaderTopicMgr, err := node.NewTopicManager(leaderBaseDir, bm, leaderBroker)
	if err != nil {
		t.Fatalf("NewTopicManager (leader): %v", err)
	}
	leaderConsumerMgr, err := consumermgr.NewConsumerManager(leaderBaseDir)
	if err != nil {
		t.Fatalf("NewConsumerManager (leader): %v", err)
	}
	leaderComps := &TestServerComponents{
		TopicManager:    leaderTopicMgr,
		ConsumerManager: leaderConsumerMgr,
		BrokerManager:   bm,
		Broker:          leaderBroker,
		BaseDir:         leaderBaseDir,
		Addr:            leaderAddr,
	}

	// Follower: different baseDir, own TopicManager, ConsumerManager; same BrokerManager
	followerBaseDir := path.Join(t.TempDir(), followerBaseDirSuffix)
	followerTopicMgr, err := node.NewTopicManager(followerBaseDir, bm, followerBroker)
	if err != nil {
		t.Fatalf("NewTopicManager (follower): %v", err)
	}
	followerConsumerMgr, err := consumermgr.NewConsumerManager(followerBaseDir)
	if err != nil {
		t.Fatalf("NewConsumerManager (follower): %v", err)
	}
	followerComps := &TestServerComponents{
		TopicManager:    followerTopicMgr,
		ConsumerManager: followerConsumerMgr,
		BrokerManager:   bm,
		Broker:          followerBroker,
		BaseDir:         followerBaseDir,
		Addr:            followerAddr,
	}

	leader := startServer(t, leaderComps)
	follower := startServer(t, followerComps)
	return leader, follower
}

// startServer starts an RPC server with the given components and updates Broker.Addr to the bound address.
func startServer(t testing.TB, comps *TestServerComponents) *TestServer {
	t.Helper()
	srv := NewRpcServer(comps.Addr, comps.TopicManager, comps.ConsumerManager)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	comps.Broker.Addr = srv.Addr
	comps.Addr = srv.Addr
	return &TestServer{
		TestServerComponents: comps,
		Addr:                 srv.Addr,
		srv:                  srv,
	}
}
