package rpc

import (
	"net"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/config"
	consumermgr "github.com/mohitkumar/mlog/consumer"
	"github.com/mohitkumar/mlog/node"
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
	node *node.Node
}

// Cleanup closes all resources associated with the test server.
func (ts *TestServer) Cleanup() {
	_ = ts.srv.Stop()
	if ts.node != nil {
		_ = ts.node.Shutdown()
	}
}

// buildTestConfig creates a config for a test node. bindAddr is the address we will listen on (e.g. from ln.Addr().String()).
func buildTestConfig(t testing.TB, baseDir, nodeID, bindAddr, raftAddr, raftDir string, bootstrap bool) config.Config {
	t.Helper()
	_, portStr, err := net.SplitHostPort(bindAddr)
	if err != nil {
		t.Fatalf("SplitHostPort(%q): %v", bindAddr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("port %q: %v", portStr, err)
	}
	return config.Config{
		BindAddr: bindAddr,
		NodeConfig: config.NodeConfig{
			ID:      nodeID,
			RPCPort: port,
			DataDir: baseDir,
		},
		RaftConfig: config.RaftConfig{
			ID:         nodeID,
			Address:    raftAddr,
			Dir:        raftDir,
			Boostatrap: bootstrap,
		},
	}
}

// StartTestServer creates and starts a single test server with a real node (Raft), TopicManager, and ConsumerManager.
func StartTestServer(t testing.TB, baseDirSuffix string) *TestServer {
	t.Helper()
	baseDir := path.Join(t.TempDir(), baseDirSuffix)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	addr := ln.Addr().String()
	cfg := buildTestConfig(t, baseDir, "node-1", addr, "127.0.0.1:0", path.Join(baseDir, "raft"), false)
	n, err := node.NewNodeFromConfig(cfg)
	if err != nil {
		ln.Close()
		t.Fatalf("NewNodeFromConfig: %v", err)
	}
	topicMgr, err := topic.NewTopicManager(baseDir, n)
	if err != nil {
		ln.Close()
		n.Shutdown()
		t.Fatalf("NewTopicManager: %v", err)
	}
	consumerMgr, err := consumermgr.NewConsumerManager(baseDir)
	if err != nil {
		ln.Close()
		n.Shutdown()
		t.Fatalf("NewConsumerManager: %v", err)
	}
	srv := NewRpcServer(addr, topicMgr, consumerMgr)
	srv.ServeOnListener(ln)
	return &TestServer{
		TestServerComponents: &TestServerComponents{
			TopicManager:    topicMgr,
			ConsumerManager: consumerMgr,
			BaseDir:         baseDir,
			Addr:            addr,
		},
		Addr: addr,
		srv:  srv,
		node: n,
	}
}

// SetupTwoTestServers creates two test servers with real nodes. Node1 is Raft bootstrap; node2 joins node1's cluster
// so GetOtherNodes() returns the other node (for CreateTopic with replicas).
// Note: Two-node Raft formation in tests can hit "offset 1 out of range" in the follower; replica tests that need
// two nodes may be skipped until Raft test setup is refined.
func SetupTwoTestServers(t testing.TB, leaderBaseDirSuffix string, followerBaseDirSuffix string) (*TestServer, *TestServer) {
	t.Helper()
	leaderBaseDir := path.Join(t.TempDir(), leaderBaseDirSuffix)
	followerBaseDir := path.Join(t.TempDir(), followerBaseDirSuffix)

	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen leader: %v", err)
	}
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		ln1.Close()
		t.Fatalf("Listen follower: %v", err)
	}
	leaderAddr := ln1.Addr().String()
	followerAddr := ln2.Addr().String()

	// Raft ports must be distinct; use a fixed offset from RPC port or separate ports
	_, leaderPortStr, _ := net.SplitHostPort(leaderAddr)
	leaderPort, _ := strconv.Atoi(leaderPortStr)
	_, followerPortStr, _ := net.SplitHostPort(followerAddr)
	followerPort, _ := strconv.Atoi(followerPortStr)
	leaderRaftAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(leaderPort+1000))
	followerRaftAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(followerPort+1000))

	leaderCfg := buildTestConfig(t, leaderBaseDir, "node-1", leaderAddr, leaderRaftAddr, path.Join(leaderBaseDir, "raft"), true)
	followerCfg := buildTestConfig(t, followerBaseDir, "node-2", followerAddr, followerRaftAddr, path.Join(followerBaseDir, "raft"), false)

	node1, err := node.NewNodeFromConfig(leaderCfg)
	if err != nil {
		ln1.Close()
		ln2.Close()
		t.Fatalf("NewNodeFromConfig leader: %v", err)
	}
	node2, err := node.NewNodeFromConfig(followerCfg)
	if err != nil {
		ln1.Close()
		ln2.Close()
		node1.Shutdown()
		t.Fatalf("NewNodeFromConfig follower: %v", err)
	}

	leaderTopicMgr, err := topic.NewTopicManager(leaderBaseDir, node1)
	if err != nil {
		ln1.Close()
		ln2.Close()
		node1.Shutdown()
		node2.Shutdown()
		t.Fatalf("NewTopicManager leader: %v", err)
	}
	leaderConsumerMgr, err := consumermgr.NewConsumerManager(leaderBaseDir)
	if err != nil {
		ln1.Close()
		ln2.Close()
		node1.Shutdown()
		node2.Shutdown()
		t.Fatalf("NewConsumerManager leader: %v", err)
	}
	followerTopicMgr, err := topic.NewTopicManager(followerBaseDir, node2)
	if err != nil {
		ln1.Close()
		ln2.Close()
		node1.Shutdown()
		node2.Shutdown()
		t.Fatalf("NewTopicManager follower: %v", err)
	}
	followerConsumerMgr, err := consumermgr.NewConsumerManager(followerBaseDir)
	if err != nil {
		ln1.Close()
		ln2.Close()
		node1.Shutdown()
		node2.Shutdown()
		t.Fatalf("NewConsumerManager follower: %v", err)
	}

	leaderSrv := NewRpcServer(leaderAddr, leaderTopicMgr, leaderConsumerMgr)
	followerSrv := NewRpcServer(followerAddr, followerTopicMgr, followerConsumerMgr)
	leaderSrv.ServeOnListener(ln1)
	followerSrv.ServeOnListener(ln2)

	// Give node1 (bootstrap) time to become Raft leader before adding node2
	for i := 0; i < 50; i++ {
		if node1.IsLeader() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Add node2 to node1's Raft cluster so node1.GetOtherNodes() returns node2
	if err := node1.Join("node-2", followerRaftAddr); err != nil {
		t.Logf("node1.Join (may be ok if already joined): %v", err)
	}

	leader := &TestServer{
		TestServerComponents: &TestServerComponents{
			TopicManager:    leaderTopicMgr,
			ConsumerManager: leaderConsumerMgr,
			BaseDir:         leaderBaseDir,
			Addr:            leaderAddr,
		},
		Addr: leaderAddr,
		srv:  leaderSrv,
		node: node1,
	}
	follower := &TestServer{
		TestServerComponents: &TestServerComponents{
			TopicManager:    followerTopicMgr,
			ConsumerManager: followerConsumerMgr,
			BaseDir:         followerBaseDir,
			Addr:            followerAddr,
		},
		Addr: followerAddr,
		srv:  followerSrv,
		node: node2,
	}
	return leader, follower
}
