package rpc

import (
	"path"
	"testing"

	consumermgr "github.com/mohitkumar/mlog/consumer"
	"github.com/mohitkumar/mlog/topic"
)

// TestServerComponents contains the components needed for a test server.
type TestServerComponents struct {
	TopicManager    *topic.TopicManager
	ConsumerManager *consumermgr.ConsumerManager
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
}

// StartTestServer creates and starts a single test server with its own TopicManager,
// ConsumerManager, and baseDir.
func StartTestServer(t testing.TB, baseDirSuffix string) *TestServer {
	t.Helper()
	addr := "127.0.0.1:0"
	baseDir := path.Join(t.TempDir(), baseDirSuffix)

	topicMgr, err := topic.NewTopicManager(baseDir, "node-1", addr, nil)
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
		BaseDir:         baseDir,
		Addr:            addr,
	}
	return startServer(t, comps)
}

// SetupTwoTestServers creates two test servers with separate TopicManager, ConsumerManager,
// and baseDir per server. Each server's TopicManager uses GetOtherNodes that returns the
// other server's (nodeID, addr) so the leader can create replicas on the follower.
func SetupTwoTestServers(t testing.TB, leaderBaseDirSuffix string, followerBaseDirSuffix string) (*TestServer, *TestServer) {
	t.Helper()

	leaderAddr := "127.0.0.1:0"
	followerAddr := "127.0.0.1:0"
	// Pointers so GetOtherNodes sees bound addresses after Start()
	leaderAddrPtr := &leaderAddr
	followerAddrPtr := &followerAddr

	getLeaderOtherNodes := func() []topic.NodeInfo {
		return []topic.NodeInfo{{NodeID: "node-2", Addr: *followerAddrPtr}}
	}
	getFollowerOtherNodes := func() []topic.NodeInfo {
		return []topic.NodeInfo{{NodeID: "node-1", Addr: *leaderAddrPtr}}
	}

	leaderBaseDir := path.Join(t.TempDir(), leaderBaseDirSuffix)
	leaderTopicMgr, err := topic.NewTopicManager(leaderBaseDir, "node-1", leaderAddr, getLeaderOtherNodes)
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
		BaseDir:         leaderBaseDir,
		Addr:            leaderAddr,
	}

	followerBaseDir := path.Join(t.TempDir(), followerBaseDirSuffix)
	followerTopicMgr, err := topic.NewTopicManager(followerBaseDir, "node-2", followerAddr, getFollowerOtherNodes)
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
		BaseDir:         followerBaseDir,
		Addr:            followerAddr,
	}

	leader := startServer(t, leaderComps)
	follower := startServer(t, followerComps)
	*leaderAddrPtr = leader.Addr
	*followerAddrPtr = follower.Addr
	// So CreateTopic uses bound addresses when creating replicas (LeaderAddr) and when resolving other nodes
	leader.TopicManager.CurrentNodeAddr = leader.Addr
	follower.TopicManager.CurrentNodeAddr = follower.Addr
	return leader, follower
}

// startServer starts an RPC server with the given components and updates Addr to the bound address.
func startServer(t testing.TB, comps *TestServerComponents) *TestServer {
	t.Helper()
	srv := NewRpcServer(comps.Addr, comps.TopicManager, comps.ConsumerManager)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	comps.Addr = srv.Addr
	return &TestServer{
		TestServerComponents: comps,
		Addr:                 srv.Addr,
		srv:                  srv,
	}
}
