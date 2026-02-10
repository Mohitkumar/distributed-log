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
	"github.com/mohitkumar/mlog/coordinator"
	"github.com/mohitkumar/mlog/rpc"
	"github.com/mohitkumar/mlog/topic"
	"go.uber.org/zap"
)

// testLogger returns a zap logger for tests (development style, with node_id). Use for node and topic manager.
func testLogger(nodeID string) *zap.Logger {
	logger, _ := zap.NewDevelopment()
	return logger.With(zap.String("node_id", nodeID))
}

// Hardcoded port range for tests. Each StartSingleNode uses 3 ports; each StartTwoNodes uses 2 blocks of 3 (server1 + server2).
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
	Addr  string // bound address after Start (use for client dial)
	srv   *rpc.RpcServer
	coord *coordinator.Coordinator
}

// Coordinator returns the coordinator for tests that need to call coordinator methods (e.g. Raft-related).
func (ts *TestServer) Coordinator() *coordinator.Coordinator {
	return ts.coord
}

// Cleanup closes all resources associated with the test server.
func (ts *TestServer) Cleanup() {
	_ = ts.srv.Stop()
	if ts.coord != nil {
		_ = ts.coord.Shutdown()
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

func StartSingleNode(t testing.TB, baseDirSuffix string) *TestServer {
	t.Helper()
	ports := allocPorts(3)
	rpcPort, raftPort, serfPort := ports[0], ports[1], ports[2]
	baseDir := path.Join(t.TempDir(), baseDirSuffix)
	rpcAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(rpcPort))
	cfg := buildTestConfigFromPorts(t, baseDir, "node-1", rpcPort, raftPort, serfPort, false)
	logger := testLogger("node-1")
	coord, err := coordinator.NewCoordinatorFromConfig(cfg, logger)
	if err != nil {
		t.Fatalf("NewCoordinatorFromConfig: %v", err)
	}
	topicMgr, err := topic.NewTopicManager(baseDir, coord, coord.Logger)
	if err != nil {
		coord.Shutdown()
		t.Fatalf("NewTopicManager: %v", err)
	}
	consumerMgr, err := consumermgr.NewConsumerManager(baseDir)
	if err != nil {
		coord.Shutdown()
		t.Fatalf("NewConsumerManager: %v", err)
	}
	srv := rpc.NewRpcServer(rpcAddr, topicMgr, consumerMgr)
	if err := srv.Start(); err != nil {
		coord.Shutdown()
		t.Fatalf("Start: %v", err)
	}
	return &TestServer{
		TestServerComponents: &TestServerComponents{
			TopicManager:    topicMgr,
			ConsumerManager: consumerMgr,
			BaseDir:         baseDir,
			Addr:            srv.Addr,
		},
		Addr:  srv.Addr,
		srv:   srv,
		coord: coord,
	}
}

func StartTestServer(t testing.TB, baseDirSuffix string) *TestServer {
	return StartSingleNode(t, baseDirSuffix)
}

func StartTwoNodes(t testing.TB, server1BaseDirSuffix string, server2BaseDirSuffix string) (*TestServer, *TestServer) {
	return setupTwoTestServersImpl(t, server1BaseDirSuffix, server2BaseDirSuffix)
}

// setupTwoTestServersImpl is the implementation shared by StartTwoNodes and SetupTwoTestServers.
// Returns server1 and server2; which one becomes Raft leader is decided at runtime (server1 bootstraps first).
func setupTwoTestServersImpl(t testing.TB, server1BaseDirSuffix string, server2BaseDirSuffix string) (*TestServer, *TestServer) {
	t.Helper()
	ports := allocPorts(6)
	server1RPC, server1Raft, server1Serf := ports[0], ports[1], ports[2]
	server2RPC, server2Raft, server2Serf := ports[3], ports[4], ports[5]
	server1BaseDir := path.Join(t.TempDir(), server1BaseDirSuffix)
	server2BaseDir := path.Join(t.TempDir(), server2BaseDirSuffix)
	server1Addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(server1RPC))
	server2Addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(server2RPC))
	server1Cfg := buildTestConfigFromPorts(t, server1BaseDir, "node-1", server1RPC, server1Raft, server1Serf, true)
	server2Cfg := buildTestConfigFromPorts(t, server2BaseDir, "node-2", server2RPC, server2Raft, server2Serf, false)
	server2RaftAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(server2Raft))

	logger1 := testLogger("node-1")
	logger2 := testLogger("node-2")
	coord1, err := coordinator.NewCoordinatorFromConfig(server1Cfg, logger1)
	if err != nil {
		t.Fatalf("NewCoordinatorFromConfig server1: %v", err)
	}
	coord2, err := coordinator.NewCoordinatorFromConfig(server2Cfg, logger2)
	if err != nil {
		coord1.Shutdown()
		t.Fatalf("NewCoordinatorFromConfig server2: %v", err)
	}

	server1TopicMgr, err := topic.NewTopicManager(server1BaseDir, coord1, coord1.Logger)
	if err != nil {
		coord1.Shutdown()
		coord2.Shutdown()
		t.Fatalf("NewTopicManager server1: %v", err)
	}
	server1ConsumerMgr, err := consumermgr.NewConsumerManager(server1BaseDir)
	if err != nil {
		coord1.Shutdown()
		coord2.Shutdown()
		t.Fatalf("NewConsumerManager server1: %v", err)
	}
	server2TopicMgr, err := topic.NewTopicManager(server2BaseDir, coord2, coord2.Logger)
	if err != nil {
		coord1.Shutdown()
		coord2.Shutdown()
		t.Fatalf("NewTopicManager server2: %v", err)
	}
	server2ConsumerMgr, err := consumermgr.NewConsumerManager(server2BaseDir)
	if err != nil {
		coord1.Shutdown()
		coord2.Shutdown()
		t.Fatalf("NewConsumerManager server2: %v", err)
	}

	server1RpcSrv := rpc.NewRpcServer(server1Addr, server1TopicMgr, server1ConsumerMgr)
	server2RpcSrv := rpc.NewRpcServer(server2Addr, server2TopicMgr, server2ConsumerMgr)
	if err := server1RpcSrv.Start(); err != nil {
		coord1.Shutdown()
		coord2.Shutdown()
		t.Fatalf("server1 RPC Start: %v", err)
	}
	if err := server2RpcSrv.Start(); err != nil {
		server1RpcSrv.Stop()
		coord1.Shutdown()
		coord2.Shutdown()
		t.Fatalf("server2 RPC Start: %v", err)
	}

	for i := 0; i < 150; i++ {
		if coord1.IsLeader() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	var joinErr error
	for j := 0; j < 30; j++ {
		joinErr = coord1.Join("node-2", server2RaftAddr, server2RpcSrv.Addr)
		if joinErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if joinErr != nil {
		t.Logf("coord1.Join failed after retries (cluster may be single-node): %v", joinErr)
	}

	server1 := &TestServer{
		TestServerComponents: &TestServerComponents{
			TopicManager:    server1TopicMgr,
			ConsumerManager: server1ConsumerMgr,
			BaseDir:         server1BaseDir,
			Addr:            server1RpcSrv.Addr,
		},
		Addr:  server1RpcSrv.Addr,
		srv:   server1RpcSrv,
		coord: coord1,
	}
	server2 := &TestServer{
		TestServerComponents: &TestServerComponents{
			TopicManager:    server2TopicMgr,
			ConsumerManager: server2ConsumerMgr,
			BaseDir:         server2BaseDir,
			Addr:            server2RpcSrv.Addr,
		},
		Addr:  server2RpcSrv.Addr,
		srv:   server2RpcSrv,
		coord: coord2,
	}
	return server1, server2
}

func SetupTwoTestServers(t testing.TB, server1BaseDirSuffix string, server2BaseDirSuffix string) (*TestServer, *TestServer) {
	return StartTwoNodes(t, server1BaseDirSuffix, server2BaseDirSuffix)
}

// getRaftLeaderCoordinator returns the coordinator that is currently Raft leader; nil if neither is leader.
func getRaftLeaderCoordinator(server1, server2 *TestServer) *coordinator.Coordinator {
	if server1.Coordinator().IsLeader() {
		return server1.Coordinator()
	}
	if server2.Coordinator().IsLeader() {
		return server2.Coordinator()
	}
	return nil
}

// waitForLeader blocks until one of the two coordinators is Raft leader or times out (then calls tb.Fatal).
func waitForLeader(tb testing.TB, a, b *coordinator.Coordinator) {
	tb.Helper()
	for i := 0; i < 100; i++ {
		if a.IsLeader() || b.IsLeader() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	tb.Fatal("no Raft leader within timeout")
}

// TwoNodeTestHelper holds two test servers (server1, server2) and provides role-based access
// (GetLeaderAddr, GetLeaderTopicMgr, GetFollowerTopicMgr) and base dirs. Leader/follower is decided by Raft at runtime.
// Used by replication_acks_test, topic_test, etc. Call Cleanup() when done.
type TwoNodeTestHelper struct {
	server1 *TestServer
	server2 *TestServer
}

// Cleanup closes both servers.
func (h *TwoNodeTestHelper) Cleanup() {
	if h.server1 != nil {
		h.server1.Cleanup()
	}
	if h.server2 != nil {
		h.server2.Cleanup()
	}
}

// GetLeaderAddr returns the RPC address of the current Raft leader.
func (h *TwoNodeTestHelper) GetLeaderAddr() string {
	raftLeader := getRaftLeaderCoordinator(h.server1, h.server2)
	if raftLeader == nil {
		return h.server1.Addr
	}
	if h.server1.Coordinator() == raftLeader {
		return h.server1.Addr
	}
	return h.server2.Addr
}

// GetLeaderTopicMgr returns the TopicManager of the current Raft leader.
func (h *TwoNodeTestHelper) GetLeaderTopicMgr() *topic.TopicManager {
	raftLeader := getRaftLeaderCoordinator(h.server1, h.server2)
	if raftLeader == nil {
		return h.server1.TopicManager
	}
	if h.server1.Coordinator() == raftLeader {
		return h.server1.TopicManager
	}
	return h.server2.TopicManager
}

// GetFollowerTopicMgr returns the TopicManager of the non-leader node.
func (h *TwoNodeTestHelper) GetFollowerTopicMgr() *topic.TopicManager {
	raftLeader := getRaftLeaderCoordinator(h.server1, h.server2)
	if h.server1.Coordinator() == raftLeader {
		return h.server2.TopicManager
	}
	return h.server1.TopicManager
}

// Server1BaseDir returns server1's base directory.
func (h *TwoNodeTestHelper) Server1BaseDir() string { return h.server1.BaseDir }

// Server2BaseDir returns server2's base directory.
func (h *TwoNodeTestHelper) Server2BaseDir() string { return h.server2.BaseDir }

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

// StartTwoNodesForTests starts two nodes (server1, server2), waits for a Raft leader, and returns a helper. Caller must call Cleanup().
func StartTwoNodesForTests(tb testing.TB, server1Suffix, server2Suffix string) *TwoNodeTestHelper {
	server1, server2 := StartTwoNodes(tb, server1Suffix, server2Suffix)
	waitForLeader(tb, server1.Coordinator(), server2.Coordinator())
	return &TwoNodeTestHelper{server1: server1, server2: server2}
}
