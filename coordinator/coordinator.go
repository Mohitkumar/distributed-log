package coordinator

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/config"
	"github.com/mohitkumar/mlog/discovery"
	"go.uber.org/zap"
)

var _ discovery.Handler = (*Coordinator)(nil)

const (
	SnapshotThreshold   = 10000
	SnapshotInterval    = 10
	RetainSnapshotCount = 10
)

// Coordinator handles cluster-wide responsibility: Raft, metadata store, and membership (Join/Leave, Apply* events).
// Addresses and RPC clients to other nodes live in Node; Coordinator holds the local Node and delegates to it where appropriate.
type Coordinator struct {
	mu            sync.RWMutex
	Logger        *zap.Logger
	raft          *raft.Raft
	cfg           config.Config
	metadataStore *MetadataStore
	nodes         map[string]*Node
	stopReconcile chan struct{}
}

// NewCoordinatorFromConfig creates a Coordinator from full config (replaces NewNodeFromConfig).
func NewCoordinatorFromConfig(cfg config.Config, logger *zap.Logger) (*Coordinator, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	metadataStore := NewMetadataStore()
	fsm, err := NewCoordinatorFSM(cfg.RaftConfig.Dir, metadataStore)
	if err != nil {
		return nil, err
	}
	raftNode, err := SetupRaft(fsm, cfg.RaftConfig)
	if err != nil {
		return nil, err
	}
	rpcAddr, err := cfg.RPCAddr()
	if err != nil {
		return nil, err
	}
	c := &Coordinator{
		Logger:        logger,
		raft:          raftNode,
		cfg:           cfg,
		metadataStore: metadataStore,
		nodes:         make(map[string]*Node),
		stopReconcile: make(chan struct{}),
	}
	c.nodes[cfg.NodeConfig.ID] = newNode(cfg.NodeConfig.ID, rpcAddr)
	c.Logger.Info("coordinator started", zap.String("raft_addr", cfg.RaftConfig.Address), zap.String("rpc_addr", rpcAddr))
	return c, nil
}

// SetupRaft creates a Raft node. BindAddress is the listen address (e.g. 0.0.0.0:9093); Address is what others use to reach this node.
func SetupRaft(fsm raft.FSM, cfg config.RaftConfig) (*raft.Raft, error) {
	raftBindAddr := cfg.Address
	if cfg.BindAddress != "" {
		raftBindAddr = cfg.BindAddress
	}
	raftAdvertiseAddr := cfg.Address
	raftConfig := raft.DefaultConfig()
	raftConfig.SnapshotThreshold = uint64(SnapshotThreshold)
	raftConfig.SnapshotInterval = time.Duration(SnapshotInterval) * time.Second
	raftConfig.LocalID = raft.ServerID(cfg.ID)
	raftConfig.LogLevel = cfg.LogLevel

	advertiseAddr, err := net.ResolveTCPAddr("tcp", raftAdvertiseAddr)
	if err != nil {
		log.Fatalf("failed to resolve Raft advertise address %s: %s", raftAdvertiseAddr, err)
	}
	transport, err := raft.NewTCPTransport(raftBindAddr, advertiseAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		log.Fatalf("failed to make TCP transport bind %s advertise %s: %s", raftBindAddr, raftAdvertiseAddr, err.Error())
	}
	snapshots, err := raft.NewFileSnapshotStore(cfg.Dir, RetainSnapshotCount, os.Stderr)
	if err != nil {
		log.Fatalf("failed to create snapshot store at %s: %s", cfg.Dir, err.Error())
	}
	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(cfg.Dir, "raft.db"))
	if err != nil {
		log.Fatalf("failed to create new bolt store: %s", err)
	}
	logStore, err := NewLogStore(cfg.Dir)
	if err != nil {
		log.Fatalf("failed to create log store: %s", err)
	}
	ra, err := raft.NewRaft(raftConfig, fsm, logStore, boltDB, snapshots, transport)
	if err != nil {
		return nil, ErrNewRaft(err)
	}
	if cfg.Boostatrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		if err := ra.BootstrapCluster(configuration).Error(); err != nil {
			return nil, ErrBootstrapCluster(err)
		}
	}
	return ra, nil
}

// EnsureSelfInMetadata adds this node to the metadata Nodes map (e.g. bootstrap node).
func (c *Coordinator) EnsureSelfInMetadata() error {
	if c.raft.State() != raft.Leader {
		return nil
	}
	rpcAddr, err := c.cfg.RPCAddr()
	if err != nil {
		return err
	}
	return c.ApplyNodeAddEvent(c.cfg.NodeConfig.ID, c.cfg.RaftConfig.Address, rpcAddr)
}

func (c *Coordinator) GetRpcClient(nodeID string) (*client.RemoteClient, error) {
	return c.nodes[nodeID].GetRpcClient()
}

func (c *Coordinator) GetRpcStreamClient(nodeID string) (*client.RemoteStreamClient, error) {
	return c.nodes[nodeID].GetRpcStreamClient()
}

func (c *Coordinator) GetRaftLeaderRemoteClient() (*client.RemoteClient, error) {
	if c.raft.State() == raft.Leader {
		return c.GetRpcClient(c.cfg.NodeConfig.ID)
	}
	_, leaderId := c.raft.LeaderWithID()
	if leaderId == "" {
		return nil, ErrRaftNoLeader
	}
	return c.GetRpcClient(string(leaderId))
}

func (c *Coordinator) GetTopicLeaderRemoteClient(topic string) (*client.RemoteClient, error) {
	tm := c.metadataStore.GetTopic(topic)
	if tm == nil {
		return nil, fmt.Errorf("topic %s not found", topic)
	}
	return c.GetRpcClient(tm.LeaderNodeID)
}

func (c *Coordinator) GetTopicLeaderRemoteStreamClient(topic string) (*client.RemoteStreamClient, error) {
	tm := c.metadataStore.GetTopic(topic)
	if tm == nil {
		return nil, fmt.Errorf("topic %s not found", topic)
	}
	return c.GetRpcStreamClient(tm.LeaderNodeID)
}

func (c *Coordinator) GetClusterNodes() []*Node {
	nodes := make([]*Node, 0)
	for _, node := range c.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

func (c *Coordinator) GetNodeIDWithLeastTopics() (*Node, error) {
	nodes := c.GetClusterNodes()
	if len(nodes) == 0 {
		return nil, ErrNoNodesInCluster
	}
	counts := c.metadataStore.TopicCountByLeader()
	best := nodes[0]
	minCount := counts[best.NodeID]
	for _, node := range nodes[1:] {
		cn := counts[node.NodeID]
		if cn < minCount {
			minCount = cn
			best = node
		}
	}
	return best, nil
}

func (c *Coordinator) TopicExists(topic string) bool {
	return c.metadataStore.GetTopic(topic) != nil
}

func (c *Coordinator) Join(id, raftAddr, rpcAddr string) error {
	c.WaitforRaftReadyWithRetryBackoff(2*time.Second, 5)
	if !c.IsLeader() {
		c.Logger.Error("not leader, skipping join", zap.String("joining_node_id", id), zap.String("raft_addr", raftAddr), zap.String("rpc_addr", rpcAddr))
		return nil
	}
	c.Logger.Info("joining cluster", zap.String("joining_node_id", id), zap.String("raft_addr", raftAddr), zap.String("rpc_addr", rpcAddr))
	configFuture := c.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(raftAddr)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				return nil
			}
			removeFuture := c.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}
	addFuture := c.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		c.Logger.Error("raft add voter failed", zap.Error(err))
		return err
	}
	c.ApplyNodeAddEvent(id, raftAddr, rpcAddr)
	c.Logger.Info("node joined cluster", zap.String("joined_node_id", id), zap.String("raft_addr", raftAddr), zap.String("rpc_addr", rpcAddr))
	return nil
}

func (c *Coordinator) Leave(id string) error {
	c.WaitforRaftReadyWithRetryBackoff(2*time.Second, 5)
	if !c.IsLeader() {
		c.Logger.Error("not leader, skipping leave", zap.String("leaving_node_id", id))
		return nil
	}
	c.Logger.Info("leaving cluster", zap.String("leaving_node_id", id))
	removeFuture := c.raft.RemoveServer(raft.ServerID(id), 0, 0)
	if err := removeFuture.Error(); err != nil {
		c.Logger.Error("raft remove server failed", zap.Error(err))
		return err
	}
	c.ApplyNodeRemoveEvent(id)
	c.maybeReassignTopicLeaders(id)
	return nil
}

func (c *Coordinator) maybeReassignTopicLeaders(nodeID string) {
	if c.raft.State() != raft.Leader {
		return
	}
	topicsCopy := c.metadataStore.GetTopicsCopy()
	for topic, tm := range topicsCopy {
		if tm == nil || tm.LeaderNodeID != nodeID {
			continue
		}
		var newLeader string
		for rid, rs := range tm.Replicas {
			if rid == nodeID || rs == nil || !rs.IsISR {
				continue
			}
			if c.metadataStore.GetNodeMetadata(rid) == nil {
				continue
			}
			newLeader = rid
			break
		}
		if newLeader == "" {
			c.Logger.Warn("no ISR replica available to take leadership", zap.String("topic", topic), zap.String("old_leader_node_id", nodeID))
			continue
		}
		nextEpoch := tm.LeaderEpoch + 1
		if err := c.ApplyLeaderChangeEvent(topic, newLeader, nextEpoch); err != nil {
			c.Logger.Warn("apply leader change failed", zap.String("topic", topic), zap.Error(err))
		}
	}
}

func (c *Coordinator) AddNode(node *Node) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes[node.NodeID] = node
}

func (c *Coordinator) RemoveNode(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.nodes, nodeID)
}

func (c *Coordinator) GetCurrentNode() *Node {
	return c.nodes[c.cfg.NodeConfig.ID]
}

func (c *Coordinator) GetOtherNodes() []*Node {
	servers := c.raft.GetConfiguration().Configuration().Servers
	out := make([]*Node, 0)
	for _, srv := range servers {
		if srv.ID == raft.ServerID(c.cfg.NodeConfig.ID) {
			continue
		}
		nodeID := string(srv.ID)
		out = append(out, c.nodes[nodeID])
	}
	return out
}

func (c *Coordinator) IsLeader() bool {
	return c.raft.State() == raft.Leader
}

func (c *Coordinator) ListTopicNames() []string {
	return c.metadataStore.ListTopicNames()
}

func (c *Coordinator) GetTopicLeaderNode(topic string) (*Node, error) {
	tm := c.metadataStore.GetTopic(topic)
	if tm == nil {
		return nil, fmt.Errorf("topic %s not found", topic)
	}
	return c.nodes[tm.LeaderNodeID], nil
}

func (c *Coordinator) GetTopicReplicaNodes(topic string) ([]*Node, error) {
	tm := c.metadataStore.GetTopic(topic)
	if tm == nil || tm.Replicas == nil {
		return nil, fmt.Errorf("topic %s not found", topic)
	}
	out := make([]*Node, 0, len(tm.Replicas))
	for id := range tm.Replicas {
		out = append(out, c.nodes[id])
	}
	return out, nil
}

func (c *Coordinator) GetTopicReplicaStates(topic string) map[string]*ReplicaState {
	tm := c.metadataStore.GetTopic(topic)
	if tm == nil || tm.Replicas == nil {
		return nil
	}
	return tm.Replicas
}

func (c *Coordinator) UpdateTopicReplicaLEO(topic, replicaNodeID string, leo int64, isr bool) {
	c.metadataStore.UpdateReplicaLEO(topic, replicaNodeID, leo, isr)
}

func (c *Coordinator) GetTopicReplicaState(topic, replicaNodeID string) (leo uint64, isISR bool) {
	tm := c.metadataStore.GetTopic(topic)
	if tm == nil || tm.Replicas == nil {
		return 0, false
	}
	rs := tm.Replicas[replicaNodeID]
	if rs == nil {
		return 0, false
	}
	if rs.LEO < 0 {
		return 0, rs.IsISR
	}
	return uint64(rs.LEO), rs.IsISR
}

func (c *Coordinator) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out waiting for leader")
		case <-ticker.C:
			if c.IsLeader() {
				return nil
			}
		}
	}
}

func (c *Coordinator) WaitforRaftReadyWithRetryBackoff(timeout time.Duration, retryCount int) error {
	timeoutc := time.After(timeout)
	backoff := time.Duration(1 * time.Second)
	for i := 0; i < retryCount; i++ {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out waiting for leader")
		default:
			if c.raft.Leader() != "" {
				return nil
			}
			time.Sleep(backoff)
			backoff *= 2
		}
	}
	return fmt.Errorf("timed out waiting for leader")
}

func (c *Coordinator) startReconcileNodes() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopReconcile:
			return
		case <-ticker.C:
			c.reconcileNodes()
		}
	}
}

func (c *Coordinator) stopReconcileNodes() {
	select {
	case <-c.stopReconcile:
		return
	default:
		close(c.stopReconcile)
	}
}

func (c *Coordinator) reconcileNodes() {
	metaIDs := c.metadataStore.ListNodeIDs()
	c.mu.RLock()
	var toAdd []struct {
		id      string
		rpcAddr string
	}
	ourIDs := make(map[string]struct{}, len(c.nodes))
	for id := range c.nodes {
		ourIDs[id] = struct{}{}
	}
	for _, id := range metaIDs {
		if _, ok := ourIDs[id]; !ok {
			meta := c.metadataStore.GetNodeMetadata(id)
			if meta != nil {
				toAdd = append(toAdd, struct {
					id      string
					rpcAddr string
				}{id, meta.RpcAddr})
			}
		}
	}
	var toRemove []string
	for id := range c.nodes {
		found := false
		for _, mid := range metaIDs {
			if mid == id {
				found = true
				break
			}
		}
		if !found {
			toRemove = append(toRemove, id)
		}
	}
	c.mu.RUnlock()

	for _, a := range toAdd {
		c.AddNode(newNode(a.id, a.rpcAddr))
	}
	for _, id := range toRemove {
		c.RemoveNode(id)
	}
}

func (c *Coordinator) Start() error {
	go c.startReconcileNodes()
	return nil
}

func (c *Coordinator) Shutdown() error {
	c.Logger.Info("node shutting down")
	c.metadataStore.StopPeriodicLog()
	c.stopReconcileNodes()
	f := c.raft.Shutdown()
	return f.Error()
}
