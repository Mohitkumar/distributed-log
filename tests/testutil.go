package tests

import (
	"net"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/config"
	consumermgr "github.com/mohitkumar/mlog/consumer"
	"github.com/mohitkumar/mlog/node"
	"github.com/mohitkumar/mlog/rpc"
	"github.com/mohitkumar/mlog/topic"
	"github.com/travisjeffery/go-dynaport"
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
	srv  *rpc.RpcServer
	node *node.Node
}

// Cleanup closes all resources associated with the test server.
func (ts *TestServer) Cleanup() {
	_ = ts.srv.Stop()
	if ts.node != nil {
		_ = ts.node.Shutdown()
	}
}

// buildTestConfigFromPorts builds a Config using dynaport-allocated ports for RPC, Raft, and Serf (same shape as helper).
// rpcPort, raftPort, serfPort are used for NodeConfig.RPCPort, RaftConfig.Address, and BindAddr (Serf) respectively.
func buildTestConfigFromPorts(t testing.TB, baseDir, nodeID string, rpcPort, raftPort, serfPort int, bootstrap bool) config.Config {
	t.Helper()
	return config.Config{
		BindAddr: net.JoinHostPort("127.0.0.1", strconv.Itoa(serfPort)),
		NodeConfig: config.NodeConfig{
			ID:      nodeID,
			RPCPort: rpcPort,
			DataDir: baseDir,
		},
		RaftConfig: config.RaftConfig{
			ID:         nodeID,
			Address:    net.JoinHostPort("127.0.0.1", strconv.Itoa(raftPort)),
			Dir:        path.Join(baseDir, "raft"),
			Boostatrap: bootstrap,
		},
	}
}

// StartSingleNode creates and starts a single test server with a real node (Raft), TopicManager, and ConsumerManager, and RPC server.
// Uses dynaport for RPC, Raft, and Serf ports. Same pattern as cmd/server/helper: node from config, then TopicManager, ConsumerManager, RpcServer.
// Caller should defer ts.Cleanup().
func StartSingleNode(t testing.TB, baseDirSuffix string) *TestServer {
	t.Helper()
	ports := dynaport.Get(3)
	rpcPort, raftPort, serfPort := ports[0], ports[1], ports[2]
	baseDir := path.Join(t.TempDir(), baseDirSuffix)
	rpcAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(rpcPort))
	cfg := buildTestConfigFromPorts(t, baseDir, "node-1", rpcPort, raftPort, serfPort, false)
	n, err := node.NewNodeFromConfig(cfg)
	if err != nil {
		t.Fatalf("NewNodeFromConfig: %v", err)
	}
	topicMgr, err := topic.NewTopicManager(baseDir, n)
	if err != nil {
		n.Shutdown()
		t.Fatalf("NewTopicManager: %v", err)
	}
	consumerMgr, err := consumermgr.NewConsumerManager(baseDir)
	if err != nil {
		n.Shutdown()
		t.Fatalf("NewConsumerManager: %v", err)
	}
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		n.Shutdown()
		t.Fatalf("Listen: %v", err)
	}
	srv := rpc.NewRpcServer(rpcAddr, topicMgr, consumerMgr)
	srv.ServeOnListener(ln)
	return &TestServer{
		TestServerComponents: &TestServerComponents{
			TopicManager:    topicMgr,
			ConsumerManager: consumerMgr,
			BaseDir:         baseDir,
			Addr:            rpcAddr,
		},
		Addr: rpcAddr,
		srv:  srv,
		node: n,
	}
}

// StartTestServer creates and starts a single test server (alias for StartSingleNode for backward compatibility).
func StartTestServer(t testing.TB, baseDirSuffix string) *TestServer {
	return StartSingleNode(t, baseDirSuffix)
}

// StartTwoNodes creates two test servers with real nodes (same pattern as helper: node, TopicManager, ConsumerManager, RpcServer per node).
// Node1 is Raft bootstrap; node2 joins node1's cluster so GetOtherNodes() returns the other node (for CreateTopic with replicas).
// Callers should defer leader.Cleanup() and follower.Cleanup().
// Note: Two-node Raft formation in tests can hit "offset 1 out of range" in the follower; replica tests that need
// two nodes may be skipped until Raft test setup is refined.
func StartTwoNodes(t testing.TB, leaderBaseDirSuffix string, followerBaseDirSuffix string) (*TestServer, *TestServer) {
	return setupTwoTestServersImpl(t, leaderBaseDirSuffix, followerBaseDirSuffix)
}

// setupTwoTestServersImpl is the implementation shared by StartTwoNodes and SetupTwoTestServers.
func setupTwoTestServersImpl(t testing.TB, leaderBaseDirSuffix string, followerBaseDirSuffix string) (*TestServer, *TestServer) {
	t.Helper()
	ports := dynaport.Get(6)
	leaderRPC, leaderRaft, leaderSerf := ports[0], ports[1], ports[2]
	followerRPC, followerRaft, followerSerf := ports[3], ports[4], ports[5]
	leaderBaseDir := path.Join(t.TempDir(), leaderBaseDirSuffix)
	followerBaseDir := path.Join(t.TempDir(), followerBaseDirSuffix)
	leaderAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(leaderRPC))
	followerAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(followerRPC))

	leaderCfg := buildTestConfigFromPorts(t, leaderBaseDir, "node-1", leaderRPC, leaderRaft, leaderSerf, true)
	followerCfg := buildTestConfigFromPorts(t, followerBaseDir, "node-2", followerRPC, followerRaft, followerSerf, false)
	followerRaftAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(followerRaft))

	ln1, err := net.Listen("tcp", leaderAddr)
	if err != nil {
		t.Fatalf("Listen leader: %v", err)
	}
	ln2, err := net.Listen("tcp", followerAddr)
	if err != nil {
		ln1.Close()
		t.Fatalf("Listen follower: %v", err)
	}

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

	leaderSrv := rpc.NewRpcServer(leaderAddr, leaderTopicMgr, leaderConsumerMgr)
	followerSrv := rpc.NewRpcServer(followerAddr, followerTopicMgr, followerConsumerMgr)
	leaderSrv.ServeOnListener(ln1)
	followerSrv.ServeOnListener(ln2)

	// Give node1 (bootstrap) time to become Raft leader before adding node2
	for i := 0; i < 50; i++ {
		if node1.IsLeader() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Add node2 to node1's Raft cluster so node1.GetOtherNodes() returns node2.
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

// SetupTwoTestServers creates two test servers (alias for StartTwoNodes for backward compatibility).
func SetupTwoTestServers(t testing.TB, leaderBaseDirSuffix string, followerBaseDirSuffix string) (*TestServer, *TestServer) {
	return StartTwoNodes(t, leaderBaseDirSuffix, followerBaseDirSuffix)
}
