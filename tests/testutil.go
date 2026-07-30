package tests

import (
	"fmt"
	"net"
	"os"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/broker/cluster"
	"github.com/mohitkumar/mlog/broker/cluster/discovery"
	"github.com/mohitkumar/mlog/broker/config"
	consumermgr "github.com/mohitkumar/mlog/broker/consumer"
	"github.com/mohitkumar/mlog/broker/log"
	"github.com/mohitkumar/mlog/broker/rpc"
	"github.com/mohitkumar/mlog/broker/topic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// testLogger returns a zap logger for tests (development style, with node_id). Use for node and topic manager.
func testLogger(nodeID string) *zap.Logger {
	logger, _ := zap.NewDevelopment()
	return logger.With(zap.String("node_id", nodeID))
}

// Hardcoded port range for tests. Each StartSingleNode uses 1 port; each StartTwoNodes uses 2 ports.
const (
	testPortBase = 15000
	testPortStep = 20 // next cluster starts at base + step
)

var testPortMu sync.Mutex
var testNextPort = testPortBase

// SetPortBase overrides the starting port allocPorts hands out. Call once, before any
// test in the calling package runs (e.g. from a package-level TestMain) — needed
// because tests/integration, tests/endtoend, and tests/chaos are separate packages
// (separate `go test` processes), each importing this package fresh with its own copy
// of testNextPort; without distinct bases, `go test ./...` running them in parallel
// would have two processes independently bind the same 127.0.0.1:<port>.
func SetPortBase(base int) {
	testPortMu.Lock()
	defer testPortMu.Unlock()
	testNextPort = base
}

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
// For tests, we wire TopicManager to a FakeTopicCoordinator instead of a real Raft-based coordinator.
type TestServer struct {
	*TestServerComponents
	Addr  string // bound address after Start (use for client dial)
	srv   *rpc.RpcServer
	coord *FakeTopicCoordinator
}

// Coordinator returns the fake topic coordinator used by this test server.
func (ts *TestServer) Coordinator() *FakeTopicCoordinator {
	return ts.coord
}

// Cleanup closes all resources associated with the test server.
func (ts *TestServer) Cleanup() {
	ts.TopicManager.StopReplicationThread()
	ts.coord.StopReplicationThread()
	_ = ts.srv.Stop()
}

// syncFakeNodesToTopicManager sets topicMgr's current node ID to match the fake
// coordinator. Cluster membership itself (AliveNodeIDs/NodeRPCAddr) is answered
// directly from the FakeTopicCoordinator (see fake_coordinator.go), not from any
// state pushed into topicMgr — this mirrors production, where node membership comes
// from Raft's own voter configuration rather than a Raft-replicated event.
func syncFakeNodesToTopicManager(topicMgr *topic.TopicManager, fake *FakeTopicCoordinator) {
	topicMgr.SetCurrentNodeID(fake.NodeID)
}

// StartSingleNode starts a single-node server backed by a FakeTopicCoordinator.
func StartSingleNode(t testing.TB, baseDirSuffix string) *TestServer {
	t.Helper()

	baseDir := path.Join(t.TempDir(), baseDirSuffix)
	logger := testLogger("node-1")

	ports := allocPorts(1)
	rpcPort := ports[0]
	rpcAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(rpcPort))

	// Start RPC server on an ephemeral port.
	fakeCoord := NewFakeTopicCoordinator("node-1", rpcAddr)
	topicMgr, err := topic.NewTopicManager(baseDir, fakeCoord, logger)
	if err != nil {
		t.Fatalf("NewTopicManager: %v", err)
	}
	consumerMgr, err := consumermgr.NewConsumerManager(baseDir)
	if err != nil {
		t.Fatalf("NewConsumerManager: %v", err)
	}
	srv := rpc.NewRpcServer(rpcAddr, topicMgr, consumerMgr)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Update fake coordinator with the actual bound RPC address.
	fakeCoord.RPCAddr = srv.Addr
	fakeCoord.AddNode(fakeCoord.NodeID, srv.Addr)
	syncFakeNodesToTopicManager(topicMgr, fakeCoord)
	topicMgr.StartReplicationThread()

	return &TestServer{
		TestServerComponents: &TestServerComponents{
			TopicManager:    topicMgr,
			ConsumerManager: consumerMgr,
			BaseDir:         baseDir,
			Addr:            srv.Addr,
		},
		Addr:  srv.Addr,
		srv:   srv,
		coord: fakeCoord,
	}
}

// StartTestServer is a convenience wrapper used by existing tests.
func StartTestServer(t testing.TB, baseDirSuffix string) *TestServer {
	return StartSingleNode(t, baseDirSuffix)
}

// StartTwoNodes starts two test servers wired together via FakeTopicCoordinators (no Raft).
func StartTwoNodes(t testing.TB, server1BaseDirSuffix string, server2BaseDirSuffix string) (*TestServer, *TestServer) {
	t.Helper()

	// Create base dirs and loggers.
	server1BaseDir := path.Join(t.TempDir(), server1BaseDirSuffix)
	server2BaseDir := path.Join(t.TempDir(), server2BaseDirSuffix)
	logger1 := testLogger("node-1")
	logger2 := testLogger("node-2")

	ports := allocPorts(2)
	server1RPC := ports[0]
	server2RPC := ports[1]
	server1Addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(server1RPC))
	server2Addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(server2RPC))

	// Fake coordinators for each node.
	fake1 := NewFakeTopicCoordinator("node-1", server1Addr)
	fake2 := NewFakeTopicCoordinator("node-2", server2Addr)

	// Topic managers and consumer managers.
	server1TopicMgr, err := topic.NewTopicManager(server1BaseDir, fake1, logger1)
	if err != nil {
		t.Fatalf("NewTopicManager server1: %v", err)
	}
	server1ConsumerMgr, err := consumermgr.NewConsumerManager(server1BaseDir)
	if err != nil {
		t.Fatalf("NewConsumerManager server1: %v", err)
	}
	server2TopicMgr, err := topic.NewTopicManager(server2BaseDir, fake2, logger2)
	if err != nil {
		t.Fatalf("NewTopicManager server2: %v", err)
	}
	server2ConsumerMgr, err := consumermgr.NewConsumerManager(server2BaseDir)
	if err != nil {
		t.Fatalf("NewConsumerManager server2: %v", err)
	}

	// RPC servers bind to ephemeral ports.
	server1RpcSrv := rpc.NewRpcServer(server1Addr, server1TopicMgr, server1ConsumerMgr)
	server2RpcSrv := rpc.NewRpcServer(server2Addr, server2TopicMgr, server2ConsumerMgr)
	if err := server1RpcSrv.Start(); err != nil {
		t.Fatalf("server1 RPC Start: %v", err)
	}
	if err := server2RpcSrv.Start(); err != nil {
		_ = server1RpcSrv.Stop()
		t.Fatalf("server2 RPC Start: %v", err)
	}

	// Wire fake coordinators with actual node addresses and cluster membership.
	fake1.RPCAddr = server1RpcSrv.Addr
	fake2.RPCAddr = server2RpcSrv.Addr

	// Both coordinators know about both nodes so GetOtherNodes and GetRpcClient work.
	fake1.AddNode("node-1", server1RpcSrv.Addr)
	fake1.AddNode("node-2", server2RpcSrv.Addr)
	fake2.AddNode("node-1", server1RpcSrv.Addr)
	fake2.AddNode("node-2", server2RpcSrv.Addr)

	// Sync fake nodes into each TopicManager so CreateTopic and replication see the cluster.
	syncFakeNodesToTopicManager(server1TopicMgr, fake1)
	syncFakeNodesToTopicManager(server2TopicMgr, fake2)

	// Deterministically treat server1 as "leader" for tests that care.
	fake1.IsRaftLeader = true
	fake2.IsRaftLeader = false

	// TopicManager owns its replication thread — every node needs it running, both to
	// replicate from a leader and to reconcile its own local topic state (leader-side
	// log opens included) against cluster metadata.
	server1TopicMgr.StartReplicationThread()
	server2TopicMgr.StartReplicationThread()

	server1 := &TestServer{
		TestServerComponents: &TestServerComponents{
			TopicManager:    server1TopicMgr,
			ConsumerManager: server1ConsumerMgr,
			BaseDir:         server1BaseDir,
			Addr:            server1RpcSrv.Addr,
		},
		Addr:  server1RpcSrv.Addr,
		srv:   server1RpcSrv,
		coord: fake1,
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
		coord: fake2,
	}
	return server1, server2
}

// SetupTwoTestServers is used by replica tests; it's now backed by FakeTopicCoordinator.
func SetupTwoTestServers(t testing.TB, server1BaseDirSuffix string, server2BaseDirSuffix string) (*TestServer, *TestServer) {
	return StartTwoNodes(t, server1BaseDirSuffix, server2BaseDirSuffix)
}

// TwoNodeTestHelper holds two test servers (server1, server2) and provides role-based access
// (GetLeaderAddr, GetLeaderTopicMgr, GetFollowerTopicMgr) and base dirs.
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

// Server1 returns the leader test server (server1).
func (h *TwoNodeTestHelper) Server1() *TestServer { return h.server1 }

// Server2 returns the follower test server (server2).
func (h *TwoNodeTestHelper) Server2() *TestServer { return h.server2 }

// GetLeaderAddr returns the RPC address of the logical leader (server1).
func (h *TwoNodeTestHelper) GetLeaderAddr() string {
	return h.server1.Addr
}

// GetLeaderTopicMgr returns the TopicManager of the logical leader (server1).
func (h *TwoNodeTestHelper) GetLeaderTopicMgr() *topic.TopicManager {
	return h.server1.TopicManager
}

// GetFollowerTopicMgr returns the TopicManager of the follower node (server2).
func (h *TwoNodeTestHelper) GetFollowerTopicMgr() *topic.TopicManager {
	return h.server2.TopicManager
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
		replicaLog, err := h.GetFollowerTopicMgr().GetLog(topicName)
		if err != nil || replicaLog == nil {
			time.Sleep(pollMs)
			continue
		}
		if replicaLog.LEO() >= targetLEO {
			return time.Since(start), true
		}
		time.Sleep(pollMs)
	}
	return time.Since(start), false
}

// WaitForTopicOpen polls tm.GetLog(topicName) until it returns a local log, or fails
// the test after timeout. Local log-opening is now reconciled on a periodic tick (see
// topic.TopicManager.reconcileLocalTopics) rather than synchronously with the metadata
// event that created/assigned the topic, so tests that apply a metadata event and then
// immediately need the local log (directly, not through a client that already retries
// — see client.RetryTopicNotReady) must poll.
func WaitForTopicOpen(t testing.TB, tm *topic.TopicManager, topicName string, timeout time.Duration) *log.LogManager {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		l, err := tm.GetLog(topicName)
		if err == nil && l != nil {
			return l
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("topic %q not open locally within %s", topicName, timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// StartTwoNodesForTests starts two nodes (server1, server2) and returns a helper. Caller must call Cleanup().
func StartTwoNodesForTests(tb testing.TB, server1Suffix, server2Suffix string) *TwoNodeTestHelper {
	server1, server2 := StartTwoNodes(tb, server1Suffix, server2Suffix)
	return &TwoNodeTestHelper{server1: server1, server2: server2}
}

// testLoggerSilent returns a silent zap logger for tests with real coordinators (avoid log spam).
func testLoggerSilent(nodeID string) *zap.Logger {
	enc := zapcore.NewConsoleEncoder(zap.NewProductionEncoderConfig())
	core := zapcore.NewCore(enc, zapcore.AddSync(os.Stderr), zapcore.ErrorLevel)
	return zap.New(core).With(zap.String("node_id", nodeID))
}

// RealTestServer represents a test server with a real coordinator.
type RealTestServer struct {
	NodeID         string
	Coordinator    *cluster.Cluster
	TopicManager   *topic.TopicManager
	ConsumerMgr    *consumermgr.ConsumerManager
	RpcServer      *rpc.RpcServer
	Membership     *discovery.Membership
	BaseDir        string
	Addr           string
	raftAddr       string
	cancelShutdown func()
	startCfg       nodeStartConfig // captured for Restart after Kill
}

// nodeStartConfig captures what's needed to (re)start a node against its existing
// on-disk state (Raft log/snapshots, segments) — used by RealTestServer.Restart after
// Kill. Ports and basePath are fixed for the node's lifetime, so the same config works
// for every restart.
type nodeStartConfig struct {
	nodeID        string
	basePath      string
	serfPort      int
	raftPort      int
	rpcPort       int
	peerSerfPorts []int // other nodes' Serf ports, to rejoin the cluster via any of them
}

func (nc nodeStartConfig) addr(port int) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
}

func (nc nodeStartConfig) joinAddrs() []string {
	addrs := make([]string, 0, len(nc.peerSerfPorts))
	for _, p := range nc.peerSerfPorts {
		addrs = append(addrs, nc.addr(p))
	}
	return addrs
}

// startRealNode (re)starts a node from nc's on-disk state as a rejoining member (never
// a fresh bootstrap — the persisted Raft log already has cluster configuration). Used
// by RealTestServer.Restart.
func startRealNode(t testing.TB, nc nodeStartConfig) *RealTestServer {
	t.Helper()

	cfg := config.Config{
		BindAddr:       nc.addr(nc.serfPort),
		AdvertiseAddr:  "127.0.0.1",
		StartJoinAddrs: nc.joinAddrs(),
		NodeConfig: config.NodeConfig{
			ID:      nc.nodeID,
			RPCPort: nc.rpcPort,
			DataDir: nc.basePath,
		},
		RaftConfig: config.RaftConfig{
			ID:          nc.nodeID,
			Address:     nc.addr(nc.raftPort),
			BindAddress: nc.addr(nc.raftPort),
			Dir:         nc.basePath,
			Boostatrap:  false,
		},
	}

	logger := testLoggerSilent(nc.nodeID)

	coord, err := cluster.NewCluster(cfg, logger)
	if err != nil {
		t.Fatalf("NewCluster %s: %v", nc.nodeID, err)
	}
	tm, err := topic.NewTopicManager(nc.basePath, coord, logger)
	if err != nil {
		t.Fatalf("NewTopicManager %s: %v", nc.nodeID, err)
	}
	tm.SetCurrentNodeID(nc.nodeID)
	coord.SetOnNodeRemoved(tm.ReassignLeadersForDeadNode)

	consumerMgr, err := consumermgr.NewConsumerManager(nc.basePath)
	if err != nil {
		t.Fatalf("NewConsumerManager %s: %v", nc.nodeID, err)
	}

	rpcSrv := rpc.NewRpcServer(nc.addr(nc.rpcPort), tm, consumerMgr)
	if err := rpcSrv.Start(); err != nil {
		t.Fatalf("RpcServer Start %s: %v", nc.nodeID, err)
	}

	membership, err := discovery.New(coord, cfg)
	if err != nil {
		t.Fatalf("discovery.New %s: %v", nc.nodeID, err)
	}
	coord.SetMemberLister(membership)

	if err := coord.WaitforRaftReady(15 * time.Second); err != nil {
		t.Fatalf("node %s: raft not ready after restart: %v", nc.nodeID, err)
	}
	if err := tm.RestoreFromMetadata(); err != nil {
		t.Fatalf("RestoreFromMetadata %s: %v", nc.nodeID, err)
	}
	tm.StartReplicationThread()

	return &RealTestServer{
		NodeID:       nc.nodeID,
		Coordinator:  coord,
		TopicManager: tm,
		ConsumerMgr:  consumerMgr,
		RpcServer:    rpcSrv,
		Membership:   membership,
		BaseDir:      nc.basePath,
		Addr:         rpcSrv.Addr,
		raftAddr:     cfg.RaftConfig.Address,
		startCfg:     nc,
	}
}

// Kill hard-stops the node the way a process crash would: RPCs stop being served,
// Raft stops, and Serf is hard-stopped (no graceful leave broadcast) so surviving
// nodes detect the failure via Serf's own failure detector — the same path a real
// crash takes, including the resulting leader re-election and ISR shrink. Safe to
// follow with Restart, or leave as-is for a permanent node loss.
func (rts *RealTestServer) Kill() error {
	var errs []error
	// Must stop first, before anything else: TopicManager's replication goroutines
	// (StartReplicationThread) make outbound Fetch calls to whichever node currently
	// leads each topic, entirely independent of this node's own RpcServer/Raft/Serf.
	// A real process death takes those goroutines down for free; here they're just
	// goroutines in the same test binary, so leaving them running after "killing" a
	// node lets it keep zombie-replicating from the leader in the background —
	// silently healing its own LEO/ISR and masking the exact fault this simulates.
	if rts.TopicManager != nil {
		rts.TopicManager.StopReplicationThread()
	}
	if rts.RpcServer != nil {
		if err := rts.RpcServer.Stop(); err != nil {
			errs = append(errs, err)
		}
	}
	if rts.Membership != nil {
		if err := rts.Membership.Shutdown(); err != nil {
			errs = append(errs, err)
		}
	}
	if rts.Coordinator != nil {
		if err := rts.Coordinator.Shutdown(); err != nil {
			errs = append(errs, err)
		}
	}
	rts.RpcServer = nil
	rts.Membership = nil
	rts.Coordinator = nil
	rts.TopicManager = nil
	rts.ConsumerMgr = nil
	if len(errs) > 0 {
		return fmt.Errorf("kill errors: %v", errs)
	}
	return nil
}

// Restart brings a killed node back up from its existing on-disk state (Raft
// log/snapshots, segments) as a rejoining member. Call after Kill.
func (rts *RealTestServer) Restart(t testing.TB) {
	t.Helper()
	fresh := startRealNode(t, rts.startCfg)
	rts.Coordinator = fresh.Coordinator
	rts.TopicManager = fresh.TopicManager
	rts.ConsumerMgr = fresh.ConsumerMgr
	rts.RpcServer = fresh.RpcServer
	rts.Membership = fresh.Membership
	rts.Addr = fresh.Addr
	rts.raftAddr = fresh.raftAddr
}

// Cleanup stops the server and cleans up resources.
func (rts *RealTestServer) Cleanup() error {
	var errs []error
	if rts.cancelShutdown != nil {
		rts.cancelShutdown()
	}
	if rts.TopicManager != nil {
		rts.TopicManager.StopReplicationThread()
	}
	if rts.Membership != nil {
		if err := rts.Membership.Leave(); err != nil {
			errs = append(errs, err)
		}
	}
	if rts.RpcServer != nil {
		if err := rts.RpcServer.Stop(); err != nil {
			errs = append(errs, err)
		}
	}
	if rts.Coordinator != nil {
		if err := rts.Coordinator.Shutdown(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("cleanup errors: %v", errs)
	}
	return nil
}

// StartRealThreeNodeCluster starts a 3-node cluster with real coordinators using Raft consensus.
// Node1 bootstraps the cluster, nodes 2 and 3 join. Returns the three servers and a function to cleanup all.
func StartRealThreeNodeCluster(t testing.TB, baseDirPrefix string) (*RealTestServer, *RealTestServer, *RealTestServer, func()) {
	t.Helper()

	// Allocate ports for all 3 nodes (each node needs: Serf bind, Raft addr, RPC port)
	ports := allocPorts(9) // 3 ports per node
	basePort := ports[0]

	// Node 1: bootstrap node
	serf1 := basePort
	raft1 := basePort + 1
	rpc1 := basePort + 2

	// Node 2
	serf2 := basePort + 3
	raft2 := basePort + 4
	rpc2 := basePort + 5

	// Node 3
	serf3 := basePort + 6
	raft3 := basePort + 7
	rpc3 := basePort + 8

	allSerfPorts := []int{serf1, serf2, serf3}
	peerSerfPorts := func(self int) []int {
		peers := make([]int, 0, 2)
		for _, p := range allSerfPorts {
			if p != self {
				peers = append(peers, p)
			}
		}
		return peers
	}

	baseDirs := [3]string{
		path.Join(t.TempDir(), baseDirPrefix+"-node1"),
		path.Join(t.TempDir(), baseDirPrefix+"-node2"),
		path.Join(t.TempDir(), baseDirPrefix+"-node3"),
	}

	nodeConfigs := []struct {
		nodeID    string
		basePath  string
		serfPort  int
		raftPort  int
		rpcPort   int
		bootstrap bool
		joinAddrs []string
	}{
		{
			nodeID:    "node-1",
			basePath:  baseDirs[0],
			serfPort:  serf1,
			raftPort:  raft1,
			rpcPort:   rpc1,
			bootstrap: true,
			joinAddrs: nil,
		},
		{
			nodeID:    "node-2",
			basePath:  baseDirs[1],
			serfPort:  serf2,
			raftPort:  raft2,
			rpcPort:   rpc2,
			bootstrap: false,
			joinAddrs: []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(serf1))},
		},
		{
			nodeID:    "node-3",
			basePath:  baseDirs[2],
			serfPort:  serf3,
			raftPort:  raft3,
			rpcPort:   rpc3,
			bootstrap: false,
			joinAddrs: []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(serf1))},
		},
	}

	servers := make([]*RealTestServer, 3)
	var allCleanup []func()

	// Start node 1 (bootstrap)
	for i := 0; i < 1; i++ {
		nc := nodeConfigs[i]
		cfg := config.Config{
			BindAddr:       net.JoinHostPort("127.0.0.1", strconv.Itoa(nc.serfPort)),
			AdvertiseAddr:  "127.0.0.1",
			StartJoinAddrs: nc.joinAddrs,
			NodeConfig: config.NodeConfig{
				ID:      nc.nodeID,
				RPCPort: nc.rpcPort,
				DataDir: nc.basePath,
			},
			RaftConfig: config.RaftConfig{
				ID:          nc.nodeID,
				Address:     net.JoinHostPort("127.0.0.1", strconv.Itoa(nc.raftPort)),
				BindAddress: net.JoinHostPort("127.0.0.1", strconv.Itoa(nc.raftPort)),
				Dir:         nc.basePath,
				Boostatrap:  nc.bootstrap,
			},
		}

		logger := testLoggerSilent(nc.nodeID)

		// Create coordinator first (owns the Raft-replicated metadata store).
		coord, err := cluster.NewCluster(cfg, logger)
		if err != nil {
			t.Fatalf("NewCluster %s: %v", nc.nodeID, err)
		}

		// Create topic manager
		tm, err := topic.NewTopicManager(nc.basePath, coord, logger)
		if err != nil {
			t.Fatalf("NewTopicManager %s: %v", nc.nodeID, err)
		}
		tm.SetCurrentNodeID(nc.nodeID)
		coord.SetOnNodeRemoved(tm.ReassignLeadersForDeadNode)

		// Start coordinator for bootstrap
		if err := coord.Start(); err != nil {
			t.Fatalf("coord.Start %s: %v", nc.nodeID, err)
		}

		// Wait for Raft to be ready
		if err := coord.WaitforRaftReady(10 * time.Second); err != nil {
			t.Fatalf("bootstrap node %s: raft not ready: %v", nc.nodeID, err)
		}

		// Restore topic manager state
		if err := tm.RestoreFromMetadata(); err != nil {
			t.Fatalf("RestoreFromMetadata %s: %v", nc.nodeID, err)
		}
		tm.StartReplicationThread()

		// Create consumer manager
		consumerMgr, err := consumermgr.NewConsumerManager(nc.basePath)
		if err != nil {
			t.Fatalf("NewConsumerManager %s: %v", nc.nodeID, err)
		}

		// Create and start RPC server
		rpcAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(nc.rpcPort))
		rpcSrv := rpc.NewRpcServer(rpcAddr, tm, consumerMgr)
		if err := rpcSrv.Start(); err != nil {
			t.Fatalf("RpcServer Start %s: %v", nc.nodeID, err)
		}

		// Create membership discovery
		membership, err := discovery.New(coord, cfg)
		if err != nil {
			t.Fatalf("discovery.New %s: %v", nc.nodeID, err)
		}
		coord.SetMemberLister(membership)

		servers[i] = &RealTestServer{
			NodeID:       nc.nodeID,
			Coordinator:  coord,
			TopicManager: tm,
			ConsumerMgr:  consumerMgr,
			RpcServer:    rpcSrv,
			Membership:   membership,
			BaseDir:      nc.basePath,
			Addr:         rpcSrv.Addr,
			raftAddr:     cfg.RaftConfig.Address,
			startCfg: nodeStartConfig{
				nodeID:        nc.nodeID,
				basePath:      nc.basePath,
				serfPort:      nc.serfPort,
				raftPort:      nc.raftPort,
				rpcPort:       nc.rpcPort,
				peerSerfPorts: peerSerfPorts(nc.serfPort),
			},
		}

		allCleanup = append(allCleanup, func(s *RealTestServer) func() {
			return func() { _ = s.Cleanup() }
		}(servers[i]))
	}

	// Give bootstrap node time to stabilize
	time.Sleep(1 * time.Second)

	// Start nodes 2 and 3 (join nodes)
	for i := 1; i < 3; i++ {
		nc := nodeConfigs[i]
		cfg := config.Config{
			BindAddr:       net.JoinHostPort("127.0.0.1", strconv.Itoa(nc.serfPort)),
			AdvertiseAddr:  "127.0.0.1",
			StartJoinAddrs: nc.joinAddrs,
			NodeConfig: config.NodeConfig{
				ID:      nc.nodeID,
				RPCPort: nc.rpcPort,
				DataDir: nc.basePath,
			},
			RaftConfig: config.RaftConfig{
				ID:          nc.nodeID,
				Address:     net.JoinHostPort("127.0.0.1", strconv.Itoa(nc.raftPort)),
				BindAddress: net.JoinHostPort("127.0.0.1", strconv.Itoa(nc.raftPort)),
				Dir:         nc.basePath,
				Boostatrap:  nc.bootstrap,
			},
		}

		logger := testLoggerSilent(nc.nodeID)

		// Create coordinator first (owns the Raft-replicated metadata store).
		coord, err := cluster.NewCluster(cfg, logger)
		if err != nil {
			t.Fatalf("NewCluster %s: %v", nc.nodeID, err)
		}

		// Create topic manager
		tm, err := topic.NewTopicManager(nc.basePath, coord, logger)
		if err != nil {
			t.Fatalf("NewTopicManager %s: %v", nc.nodeID, err)
		}
		tm.SetCurrentNodeID(nc.nodeID)
		coord.SetOnNodeRemoved(tm.ReassignLeadersForDeadNode)

		// Create consumer manager
		consumerMgr, err := consumermgr.NewConsumerManager(nc.basePath)
		if err != nil {
			t.Fatalf("NewConsumerManager %s: %v", nc.nodeID, err)
		}

		// Create and start RPC server
		rpcAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(nc.rpcPort))
		rpcSrv := rpc.NewRpcServer(rpcAddr, tm, consumerMgr)
		if err := rpcSrv.Start(); err != nil {
			t.Fatalf("RpcServer Start %s: %v", nc.nodeID, err)
		}

		// Create membership discovery
		membership, err := discovery.New(coord, cfg)
		if err != nil {
			t.Fatalf("discovery.New %s: %v", nc.nodeID, err)
		}
		coord.SetMemberLister(membership)

		// Start coordinator (will join via Serf)
		if err := coord.Start(); err != nil {
			t.Fatalf("coord.Start %s: %v", nc.nodeID, err)
		}

		// Wait for Raft to be ready (will wait for discovery to work)
		if err := coord.WaitforRaftReady(15 * time.Second); err != nil {
			t.Fatalf("node %s: raft not ready: %v", nc.nodeID, err)
		}

		// Restore topic manager state
		if err := tm.RestoreFromMetadata(); err != nil {
			t.Fatalf("RestoreFromMetadata %s: %v", nc.nodeID, err)
		}
		tm.StartReplicationThread()

		servers[i] = &RealTestServer{
			NodeID:       nc.nodeID,
			Coordinator:  coord,
			TopicManager: tm,
			ConsumerMgr:  consumerMgr,
			RpcServer:    rpcSrv,
			Membership:   membership,
			BaseDir:      nc.basePath,
			Addr:         rpcSrv.Addr,
			raftAddr:     cfg.RaftConfig.Address,
			startCfg: nodeStartConfig{
				nodeID:        nc.nodeID,
				basePath:      nc.basePath,
				serfPort:      nc.serfPort,
				raftPort:      nc.raftPort,
				rpcPort:       nc.rpcPort,
				peerSerfPorts: peerSerfPorts(nc.serfPort),
			},
		}

		allCleanup = append(allCleanup, func(s *RealTestServer) func() {
			return func() { _ = s.Cleanup() }
		}(servers[i]))

		// Wait between node starts
		time.Sleep(500 * time.Millisecond)
	}

	// Allow cluster to stabilize
	time.Sleep(1 * time.Second)

	cleanup := func() {
		for i := len(allCleanup) - 1; i >= 0; i-- {
			allCleanup[i]()
		}
	}

	return servers[0], servers[1], servers[2], cleanup
}
