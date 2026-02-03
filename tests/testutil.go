package tests

import (
	"net"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/config"
	consumermgr "github.com/mohitkumar/mlog/consumer"
	"github.com/mohitkumar/mlog/node"
	"github.com/mohitkumar/mlog/rpc"
	"github.com/mohitkumar/mlog/topic"
)

// Hardcoded port range for tests. Each StartSingleNode uses 3 ports; each StartTwoNodes uses 2 blocks of 3 (leader + follower).
const (
	testPortBase = 15000
	testPortStep = 20 // next cluster starts at base + step
)

var testPortMu sync.Mutex
var testNextPort = testPortBase

func allocPorts(n int) []int {
	testPortMu.Lock()
	start := testNextPort
	testNextPort += testPortStep
	testPortMu.Unlock()
	ports := make([]int, n)
	for i := 0; i < n; i++ {
		ports[i] = start + i
	}
	return ports
}

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

// Node returns the node for tests that need to call node methods (e.g. Raft-related).
func (ts *TestServer) Node() *node.Node {
	return ts.node
}

// Cleanup closes all resources associated with the test server.
func (ts *TestServer) Cleanup() {
	_ = ts.srv.Stop()
	if ts.node != nil {
		_ = ts.node.Shutdown()
	}
}

// buildTestConfigFromPorts builds a Config using the given ports for RPC, Raft, and Serf (same shape as helper).
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
// Uses hardcoded port range (testPortBase + alloc). Caller should defer ts.Cleanup().
func StartSingleNode(t testing.TB, baseDirSuffix string) *TestServer {
	t.Helper()
	ports := allocPorts(3)
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
	srv := rpc.NewRpcServer(rpcAddr, topicMgr, consumerMgr)
	if err := srv.Start(); err != nil {
		n.Shutdown()
		t.Fatalf("Start: %v", err)
	}
	return &TestServer{
		TestServerComponents: &TestServerComponents{
			TopicManager:    topicMgr,
			ConsumerManager: consumerMgr,
			BaseDir:         baseDir,
			Addr:            srv.Addr,
		},
		Addr: srv.Addr,
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
	ports := allocPorts(6)
	leaderRPC, leaderRaft, leaderSerf := ports[0], ports[1], ports[2]
	followerRPC, followerRaft, followerSerf := ports[3], ports[4], ports[5]
	leaderBaseDir := path.Join(t.TempDir(), leaderBaseDirSuffix)
	followerBaseDir := path.Join(t.TempDir(), followerBaseDirSuffix)
	leaderAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(leaderRPC))
	followerAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(followerRPC))
	leaderCfg := buildTestConfigFromPorts(t, leaderBaseDir, "node-1", leaderRPC, leaderRaft, leaderSerf, true)
	followerCfg := buildTestConfigFromPorts(t, followerBaseDir, "node-2", followerRPC, followerRaft, followerSerf, false)
	followerRaftAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(followerRaft))

	node1, err := node.NewNodeFromConfig(leaderCfg)
	if err != nil {
		t.Fatalf("NewNodeFromConfig leader: %v", err)
	}
	node2, err := node.NewNodeFromConfig(followerCfg)
	if err != nil {
		node1.Shutdown()
		t.Fatalf("NewNodeFromConfig follower: %v", err)
	}

	leaderTopicMgr, err := topic.NewTopicManager(leaderBaseDir, node1)
	if err != nil {
		node1.Shutdown()
		node2.Shutdown()
		t.Fatalf("NewTopicManager leader: %v", err)
	}
	leaderConsumerMgr, err := consumermgr.NewConsumerManager(leaderBaseDir)
	if err != nil {
		node1.Shutdown()
		node2.Shutdown()
		t.Fatalf("NewConsumerManager leader: %v", err)
	}
	followerTopicMgr, err := topic.NewTopicManager(followerBaseDir, node2)
	if err != nil {
		node1.Shutdown()
		node2.Shutdown()
		t.Fatalf("NewTopicManager follower: %v", err)
	}
	followerConsumerMgr, err := consumermgr.NewConsumerManager(followerBaseDir)
	if err != nil {
		node1.Shutdown()
		node2.Shutdown()
		t.Fatalf("NewConsumerManager follower: %v", err)
	}

	leaderRpcSrv := rpc.NewRpcServer(leaderAddr, leaderTopicMgr, leaderConsumerMgr)
	followerRpcSrv := rpc.NewRpcServer(followerAddr, followerTopicMgr, followerConsumerMgr)
	if err := leaderRpcSrv.Start(); err != nil {
		node1.Shutdown()
		node2.Shutdown()
		t.Fatalf("leader RPC Start: %v", err)
	}
	if err := followerRpcSrv.Start(); err != nil {
		leaderRpcSrv.Stop()
		node1.Shutdown()
		node2.Shutdown()
		t.Fatalf("follower RPC Start: %v", err)
	}

	// Wait for node1 (bootstrap) to become Raft leader, then add node2 so they form a cluster.
	for i := 0; i < 150; i++ {
		if node1.IsLeader() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	var joinErr error
	for j := 0; j < 30; j++ {
		joinErr = node1.Join("node-2", followerRaftAddr, followerRpcSrv.Addr)
		if joinErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if joinErr != nil {
		t.Logf("node1.Join failed after retries (cluster may be single-node): %v", joinErr)
	}

	leader := &TestServer{
		TestServerComponents: &TestServerComponents{
			TopicManager:    leaderTopicMgr,
			ConsumerManager: leaderConsumerMgr,
			BaseDir:         leaderBaseDir,
			Addr:            leaderRpcSrv.Addr,
		},
		Addr: leaderRpcSrv.Addr,
		srv:  leaderRpcSrv,
		node: node1,
	}
	follower := &TestServer{
		TestServerComponents: &TestServerComponents{
			TopicManager:    followerTopicMgr,
			ConsumerManager: followerConsumerMgr,
			BaseDir:         followerBaseDir,
			Addr:            followerRpcSrv.Addr,
		},
		Addr: followerRpcSrv.Addr,
		srv:  followerRpcSrv,
		node: node2,
	}
	return leader, follower
}

// SetupTwoTestServers creates two test servers (alias for StartTwoNodes for backward compatibility).
func SetupTwoTestServers(t testing.TB, leaderBaseDirSuffix string, followerBaseDirSuffix string) (*TestServer, *TestServer) {
	return StartTwoNodes(t, leaderBaseDirSuffix, followerBaseDirSuffix)
}

// getLeaderNode returns the node that is currently Raft leader; nil if neither is leader.
func getLeaderNode(leaderSrv, followerSrv *TestServer) *node.Node {
	if leaderSrv.Node().IsLeader() {
		return leaderSrv.Node()
	}
	if followerSrv.Node().IsLeader() {
		return followerSrv.Node()
	}
	return nil
}

// waitForLeader blocks until one of the two nodes is Raft leader or times out (then calls tb.Fatal).
func waitForLeader(tb testing.TB, a, b *node.Node) {
	tb.Helper()
	for i := 0; i < 100; i++ {
		if a.IsLeader() || b.IsLeader() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	tb.Fatal("no Raft leader within timeout")
}

// TwoNodeTestHelper holds two test servers and provides leader/follower access (getLeaderAddr, TopicMgrs, base dirs).
// Used by replication_acks_test, topic_test, etc. Call Cleanup() when done.
type TwoNodeTestHelper struct {
	leaderSrv   *TestServer
	followerSrv *TestServer
}

// Cleanup closes both servers.
func (h *TwoNodeTestHelper) Cleanup() {
	if h.leaderSrv != nil {
		h.leaderSrv.Cleanup()
	}
	if h.followerSrv != nil {
		h.followerSrv.Cleanup()
	}
}

// GetLeaderAddr returns the RPC address of the current Raft leader.
func (h *TwoNodeTestHelper) GetLeaderAddr() string {
	raftLeader := getLeaderNode(h.leaderSrv, h.followerSrv)
	if raftLeader == nil {
		return h.leaderSrv.Addr
	}
	if h.leaderSrv.Node() == raftLeader {
		return h.leaderSrv.Addr
	}
	return h.followerSrv.Addr
}

// GetLeaderTopicMgr returns the TopicManager of the current Raft leader.
func (h *TwoNodeTestHelper) GetLeaderTopicMgr() *topic.TopicManager {
	raftLeader := getLeaderNode(h.leaderSrv, h.followerSrv)
	if raftLeader == nil {
		return h.leaderSrv.TopicManager
	}
	if h.leaderSrv.Node() == raftLeader {
		return h.leaderSrv.TopicManager
	}
	return h.followerSrv.TopicManager
}

// GetFollowerTopicMgr returns the TopicManager of the non-leader node.
func (h *TwoNodeTestHelper) GetFollowerTopicMgr() *topic.TopicManager {
	raftLeader := getLeaderNode(h.leaderSrv, h.followerSrv)
	if h.leaderSrv.Node() == raftLeader {
		return h.followerSrv.TopicManager
	}
	return h.leaderSrv.TopicManager
}

// LeaderBaseDir returns the leader server's base directory.
func (h *TwoNodeTestHelper) LeaderBaseDir() string { return h.leaderSrv.BaseDir }

// FollowerBaseDir returns the follower server's base directory.
func (h *TwoNodeTestHelper) FollowerBaseDir() string { return h.followerSrv.BaseDir }

// WaitReplicaCatchUp polls until the follower's topic log LEO >= targetLEO or timeout. Returns catch-up duration and true if caught up.
func (h *TwoNodeTestHelper) WaitReplicaCatchUp(topicName string, targetLEO uint64, timeout time.Duration) (time.Duration, bool) {
	pollMs := 10 * time.Millisecond
	deadline := time.Now().Add(timeout)
	start := time.Now()
	for time.Now().Before(deadline) {
		replicaTopic, err := h.GetFollowerTopicMgr().GetTopic(topicName)
		if err != nil || replicaTopic == nil || replicaTopic.Log == nil {
			time.Sleep(pollMs)
			continue
		}
		if replicaTopic.Log.LEO() >= targetLEO {
			return time.Since(start), true
		}
		time.Sleep(pollMs)
	}
	return time.Since(start), false
}

// StartTwoNodesForTests starts two nodes, waits for a Raft leader, and returns a helper. Caller must call Cleanup().
func StartTwoNodesForTests(tb testing.TB, leaderSuffix, followerSuffix string) *TwoNodeTestHelper {
	leaderSrv, followerSrv := StartTwoNodes(tb, leaderSuffix, followerSuffix)
	waitForLeader(tb, leaderSrv.Node(), followerSrv.Node())
	return &TwoNodeTestHelper{leaderSrv: leaderSrv, followerSrv: followerSrv}
}
