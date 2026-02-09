package node

import (
	"fmt"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/common"
	"github.com/mohitkumar/mlog/config"
	"github.com/mohitkumar/mlog/coordinator"
	"github.com/mohitkumar/mlog/discovery"
	"github.com/mohitkumar/mlog/raftmeta"
	"go.uber.org/zap"
)

var _ discovery.Handler = (*Node)(nil)

// Node represents a cluster node
type Node struct {
	NodeID            string
	NodeRPCAddr       string
	NodeRaftAddr      string
	Logger            *zap.Logger
	raft              *raft.Raft
	cfg               config.Config
	metadataStore     *raftmeta.MetadataStore
	nodeClients       map[string]*client.RemoteClient
	nodeStreamClients map[string]*client.RemoteStreamClient
}

func NewNodeFromConfig(config config.Config, logger *zap.Logger) (*Node, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	fsm, err := coordinator.NewCoordinatorFSM(config.RaftConfig.Dir)
	if err != nil {
		return nil, err
	}
	raftBindAddr := config.RaftConfig.Address
	if config.RaftConfig.BindAddress != "" {
		raftBindAddr = config.RaftConfig.BindAddress
	}
	raftNode, err := coordinator.SetupRaft(fsm, config.RaftConfig.ID, raftBindAddr, config.RaftConfig.Address, config.RaftConfig.Dir, config.RaftConfig.Boostatrap)
	if err != nil {
		return nil, err
	}
	rpcAddr, err := config.RPCAddr()
	if err != nil {
		return nil, err
	}
	n := &Node{
		NodeID:            config.NodeConfig.ID,
		NodeRPCAddr:       rpcAddr,
		NodeRaftAddr:      config.RaftConfig.Address,
		Logger:            logger,
		raft:              raftNode,
		cfg:               config,
		metadataStore:     fsm.MetadataStore,
		nodeClients:       make(map[string]*client.RemoteClient),
		nodeStreamClients: make(map[string]*client.RemoteStreamClient),
	}
	n.Logger.Info("node started", zap.String("raft_addr", config.RaftConfig.Address), zap.String("rpc_addr", rpcAddr))
	return n, nil
}

// EnsureSelfInMetadata adds this node to the metadata Nodes map so it appears in the store
// (e.g. for topic leader resolution and the periodic metadata log). The bootstrap node
// never triggers Join(), so it must add itself; call when this node is the Raft leader.
func (n *Node) EnsureSelfInMetadata() error {
	if n.raft.State() != raft.Leader {
		return nil
	}
	return n.ApplyNodeAddEvent(n.NodeID, n.NodeRaftAddr, n.NodeRPCAddr)
}

func (n *Node) GetRpcClient(nodeId string) (*client.RemoteClient, error) {
	cl, ok := n.nodeClients[nodeId]
	if !ok {
		return nil, ErrNodeNotFound
	}
	return cl, nil
}

func (n *Node) GetRpcStreamClient(nodeId string) (*client.RemoteStreamClient, error) {
	cl, ok := n.nodeStreamClients[nodeId]
	if !ok {
		return nil, ErrNodeNotFound
	}
	return cl, nil
}

func (n *Node) GetRpcAddrForNodeID(nodeID string) (string, error) {
	meta := n.metadataStore.GetNodeMetadata(nodeID)
	if meta == nil {
		return "", ErrNodeNotFound
	}
	return meta.RpcAddr, nil
}

func (n *Node) GetRaftLeaderId() (string, error) {
	if n.raft.State() == raft.Leader {
		return n.NodeID, nil
	}
	_, leaderId := n.raft.LeaderWithID()
	if leaderId == "" {
		return "", ErrRaftNoLeader
	}
	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return "", err
	}
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == leaderId {
			return string(leaderId), nil
		}
	}
	return "", fmt.Errorf("leader not found")
}

func (n *Node) GetTopicLeaderId(topic string) (string, error) {
	tm := n.metadataStore.GetTopic(topic)
	if tm == nil {
		return "", fmt.Errorf("topic %s not found", topic)
	}
	return tm.LeaderNodeID, nil
}

func (n *Node) GetClusterNodeIDs() []string {
	return n.metadataStore.ListNodeIDs()
}

// GetNodeIDWithLeastTopics returns the cluster node ID that is leader of the fewest topics.
// Use this to balance topic leadership (e.g. when choosing a topic leader for a new topic).
// Returns ErrNoNodesInCluster if there are no nodes in the cluster.
func (n *Node) GetNodeIDWithLeastTopics() (string, error) {
	ids := n.GetClusterNodeIDs()
	if len(ids) == 0 {
		return "", ErrNoNodesInCluster
	}
	counts := n.metadataStore.TopicCountByLeader()
	best := ids[0]
	minCount := counts[best]
	for _, id := range ids[1:] {
		c := counts[id]
		if c < minCount {
			minCount = c
			best = id
		}
	}
	return best, nil
}

func (n *Node) TopicExists(topic string) bool {
	return n.metadataStore.GetTopic(topic) != nil
}

func (n *Node) Join(id, raftAddr, rpcAddr string) error {
	n.WaitforRaftReadyWithRetryBackoff(2*time.Second, 5)
	if !n.IsLeader() {
		n.Logger.Error("not leader, skipping join", zap.String("joining_node_id", id), zap.String("raft_addr", raftAddr), zap.String("rpc_addr", rpcAddr))
		return nil
	}
	n.Logger.Info("joining cluster", zap.String("joining_node_id", id), zap.String("raft_addr", raftAddr), zap.String("rpc_addr", rpcAddr))
	configFuture := n.raft.GetConfiguration()
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
			// remove the existing server
			removeFuture := n.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}
	addFuture := n.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		n.Logger.Error("raft add voter failed", zap.Error(err))
		return err
	}
	cl, err := client.NewRemoteClient(rpcAddr)
	if err != nil {
		return err
	}
	n.nodeClients[id] = cl
	streamCl, err := client.NewRemoteStreamClient(rpcAddr)
	if err != nil {
		return err
	}
	n.nodeStreamClients[id] = streamCl
	n.ApplyNodeAddEvent(id, raftAddr, rpcAddr)

	n.Logger.Info("node joined cluster", zap.String("joined_node_id", id), zap.String("raft_addr", raftAddr), zap.String("rpc_addr", rpcAddr))
	return nil
}

func (n *Node) Leave(id string) error {
	n.WaitforRaftReadyWithRetryBackoff(2*time.Second, 5)
	if !n.IsLeader() {
		n.Logger.Error("not leader, skipping leave", zap.String("leaving_node_id", id))
		return nil
	}
	n.Logger.Info("leaving cluster", zap.String("leaving_node_id", id))
	removeFuture := n.raft.RemoveServer(raft.ServerID(id), 0, 0)
	if err := removeFuture.Error(); err != nil {
		n.Logger.Error("raft remove server failed", zap.Error(err))
		return err
	}
	cl, ok := n.nodeClients[id]
	if ok {
		cl.Close()
	}
	streamCl, ok := n.nodeStreamClients[id]
	if ok {
		streamCl.Close()
	}
	delete(n.nodeClients, id)
	delete(n.nodeStreamClients, id)
	n.ApplyNodeRemoveEvent(id)
	// If the leaving node was topic leader for any topics, shift leadership to an ISR replica.
	n.maybeReassignTopicLeaders(id)
	return nil
}

// maybeReassignTopicLeaders reassigns topic leaders away from nodeID if it is currently a leader.
// Only safe to call on the Raft leader.
func (n *Node) maybeReassignTopicLeaders(nodeID string) {
	if n.raft.State() != raft.Leader {
		return
	}
	topicsCopy := n.metadataStore.GetTopicsCopy()
	for topic, tm := range topicsCopy {
		if tm == nil || tm.LeaderNodeID != nodeID {
			continue
		}
		// Pick the first in-sync replica that's not the failed node.
		var newLeader string
		for rid, rs := range tm.Replicas {
			if rid == nodeID || rs == nil || !rs.IsISR {
				continue
			}
			if n.metadataStore.GetNodeMetadata(rid) == nil {
				continue
			}
			newLeader = rid
			break
		}
		if newLeader == "" {
			n.Logger.Warn("no ISR replica available to take leadership", zap.String("topic", topic), zap.String("old_leader_node_id", nodeID))
			continue
		}
		nextEpoch := tm.LeaderEpoch + 1
		if err := n.ApplyLeaderChangeEvent(topic, newLeader, nextEpoch); err != nil {
			n.Logger.Warn("apply leader change failed", zap.String("topic", topic), zap.Error(err))
		}
	}
}

// GetNodeID returns this node's ID.
func (n *Node) GetNodeID() string {
	return n.NodeID
}

// GetNodeAddr returns this node's RPC address.
func (n *Node) GetNodeAddr() string {
	return n.NodeRPCAddr
}

func (n *Node) GetOtherNodes() []common.NodeInfo {
	servers := n.raft.GetConfiguration().Configuration().Servers
	nodes := make([]common.NodeInfo, 0)
	for _, srv := range servers {
		if srv.ID == raft.ServerID(n.NodeID) {
			continue
		}
		nodeID := string(srv.ID)
		rpcAddr, err := n.GetRpcAddrForNodeID(nodeID)
		if err != nil {
			rpcAddr = string(srv.Address)
		}
		nodes = append(nodes, common.NodeInfo{NodeID: nodeID, RpcAddr: rpcAddr})
	}
	return nodes
}

func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

func (n *Node) GetTopicLeaderNodeID(topic string) string {
	tm := n.metadataStore.GetTopic(topic)
	if tm == nil {
		return ""
	}
	return tm.LeaderNodeID
}

// ListTopicNames returns all topic names from metadata (for topic manager restore).
func (n *Node) ListTopicNames() []string {
	return n.metadataStore.ListTopicNames()
}

// GetTopicReplicaNodeIDs returns replica node IDs for a topic from metadata.
func (n *Node) GetTopicReplicaNodeIDs(topic string) []string {
	tm := n.metadataStore.GetTopic(topic)
	if tm == nil || tm.Replicas == nil {
		return nil
	}
	ids := make([]string, 0, len(tm.Replicas))
	for id := range tm.Replicas {
		ids = append(ids, id)
	}
	return ids
}

func (n *Node) GetTopicReplicaStates(topic string) map[string]*raftmeta.ReplicaState {
	tm := n.metadataStore.GetTopic(topic)
	if tm == nil || tm.Replicas == nil {
		return nil
	}
	return tm.Replicas
}

// UpdateTopicReplicaLEO updates LEO and IsISR for a topic replica in metadata under store lock.
func (n *Node) UpdateTopicReplicaLEO(topic, replicaNodeID string, leo int64, isr bool) {
	n.metadataStore.UpdateReplicaLEO(topic, replicaNodeID, leo, isr)
}

// GetTopicReplicaState returns LEO and IsISR for a topic replica from metadata (for restore).
func (n *Node) GetTopicReplicaState(topic, replicaNodeID string) (leo uint64, isISR bool) {
	tm := n.metadataStore.GetTopic(topic)
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

func (n *Node) GetTopicReplicaClients(topic string) map[string]*client.RemoteClient {
	tm := n.metadataStore.GetTopic(topic)
	if tm == nil || tm.Replicas == nil {
		return nil
	}
	clients := make(map[string]*client.RemoteClient, len(tm.Replicas))
	for id := range tm.Replicas {
		cl, _ := n.GetRpcClient(id)
		clients[id] = cl
	}
	return clients
}

func (n *Node) GetTopicLeaderClient(topic string) *client.RemoteClient {
	tm := n.metadataStore.GetTopic(topic)
	if tm == nil || tm.LeaderNodeID == "" {
		return nil
	}
	cl, _ := n.GetRpcClient(tm.LeaderNodeID)
	return cl
}

func (n *Node) GetTopicLeaderStreamClient(topic string) *client.RemoteStreamClient {
	tm := n.metadataStore.GetTopic(topic)
	if tm == nil || tm.LeaderNodeID == "" {
		return nil
	}
	cl, _ := n.GetRpcStreamClient(tm.LeaderNodeID)
	return cl
}

// WaitForLeader waits until this node is the Raft leader (for bootstrap).
func (n *Node) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out waiting for leader")
		case <-ticker.C:
			if n.IsLeader() {
				return nil
			}
		}
	}
}

func (n *Node) WaitforRaftReadyWithRetryBackoff(timeout time.Duration, retryCount int) error {
	timeoutc := time.After(timeout)
	backoff := time.Duration(1 * time.Second)
	for i := 0; i < retryCount; i++ {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out waiting for leader")
		default:
			if n.raft.Leader() != "" {
				return nil
			}
			time.Sleep(backoff)
			backoff *= 2
		}
	}
	return fmt.Errorf("timed out waiting for leader")
}

func (n *Node) Shutdown() error {
	n.Logger.Info("node shutting down")
	n.metadataStore.StopPeriodicLog()
	f := n.raft.Shutdown()
	return f.Error()
}
