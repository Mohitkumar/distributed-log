package node

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mohitkumar/mlog/common"
	"github.com/mohitkumar/mlog/config"
	"github.com/mohitkumar/mlog/coordinator"
	"github.com/mohitkumar/mlog/discovery"
	"github.com/mohitkumar/mlog/protocol"
	"github.com/mohitkumar/mlog/raftmeta"
	"go.uber.org/zap"
)

var _ discovery.Handler = (*Node)(nil)

// Node represents a cluster node
type Node struct {
	NodeID        string
	NodeRPCAddr   string
	NodeRaftAddr  string
	Logger        *zap.Logger
	raft          *raft.Raft
	cfg           config.Config
	metadataStore *raftmeta.MetadataStore
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
		NodeID:        config.NodeConfig.ID,
		NodeRPCAddr:   rpcAddr,
		NodeRaftAddr:  config.RaftConfig.Address,
		Logger:        logger,
		raft:          raftNode,
		cfg:           config,
		metadataStore: fsm.MetadataStore,
	}
	n.Logger.Info("node started", zap.String("raft_addr", config.RaftConfig.Address), zap.String("rpc_addr", rpcAddr))
	return n, nil
}

func (n *Node) GetRaftLeaderRpcAddr() (string, error) {
	if n.raft.State() == raft.Leader {
		return n.NodeRPCAddr, nil
	}
	leaderAddr := n.raft.Leader()
	if leaderAddr == "" {
		return "", ErrRaftNoLeader
	}
	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return "", err
	}
	for _, srv := range configFuture.Configuration().Servers {
		if srv.Address == leaderAddr {
			return n.GetRpcAddrForNodeID(string(srv.ID))
		}
	}
	return "", ErrRaftNoLeader
}

func (n *Node) GetRpcAddrForNodeID(nodeID string) (string, error) {
	meta := n.metadataStore.GetNodeMetadata(nodeID)
	if meta == nil {
		return "", ErrNodeNotFound
	}
	return meta.RpcAddr, nil
}

func (n *Node) GetTopicLeaderRpcAddr(topic string) (string, error) {
	tm := n.metadataStore.GetTopic(topic)
	if tm == nil {
		return "", ErrTopicNotFound
	}
	return n.GetRpcAddrForNodeID(tm.LeaderNodeID)
}

func (n *Node) GetClusterNodeIDs() []string {
	nodes := n.metadataStore.Nodes
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.NodeID)
	}
	return ids
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

func (n *Node) ApplyCreateTopicEvent(ev *protocol.MetadataEvent) error {
	if n.raft.State() != raft.Leader {
		n.Logger.Debug("not leader, skipping create topic event", zap.String("topic", ev.CreateTopicEvent.Topic))
		return nil
	}
	if ev.CreateTopicEvent == nil {
		return ErrInvalidEvent(ev)
	}
	n.Logger.Info("applying create topic event", zap.String("topic", ev.CreateTopicEvent.Topic), zap.String("leader_node_id", ev.CreateTopicEvent.LeaderNodeID))
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	f := n.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		topicName := ""
		if ev.CreateTopicEvent != nil {
			topicName = ev.CreateTopicEvent.Topic
		}
		n.Logger.Error("raft apply create topic failed", zap.Error(err), zap.String("topic", topicName))
		return ErrRaftApply(err)
	}
	return nil
}

func (n *Node) ApplyDeleteTopicEvent(ev *protocol.MetadataEvent) error {
	if n.raft.State() != raft.Leader {
		n.Logger.Debug("not leader, skipping delete topic event", zap.String("topic", ev.DeleteTopicEvent.Topic))
		return nil
	}
	if ev.DeleteTopicEvent == nil {
		return ErrInvalidEvent(ev)
	}
	n.Logger.Info("applying delete topic event", zap.String("topic", ev.DeleteTopicEvent.Topic))
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	f := n.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		topicName := ""
		if ev.DeleteTopicEvent != nil {
			topicName = ev.DeleteTopicEvent.Topic
		}
		n.Logger.Error("raft apply delete topic failed", zap.Error(err), zap.String("topic", topicName))
		return ErrRaftApply(err)
	}
	return nil
}

func (n *Node) Join(id, raftAddr, rpcAddr string) error {
	if !n.IsLeader() {
		n.WaitforLeaderWithRetryBackoff(2*time.Second, 5)
	}
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
				// server has already joined
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
	n.ApplyNodeAddEvent(&protocol.MetadataEvent{
		AddNodeEvent: &protocol.AddNodeEvent{
			NodeID:  id,
			Addr:    raftAddr,
			RpcAddr: rpcAddr,
		},
	})

	n.Logger.Info("node joined cluster", zap.String("joined_node_id", id), zap.String("raft_addr", raftAddr), zap.String("rpc_addr", rpcAddr))
	return nil
}

func (n *Node) ApplyNodeAddEvent(ev *protocol.MetadataEvent) error {
	if n.raft.State() != raft.Leader {
		n.Logger.Debug("not leader, skipping node add event", zap.String("node_id", ev.AddNodeEvent.NodeID))
		return nil
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	f := n.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		if ev.AddNodeEvent != nil {
			n.Logger.Error("raft apply node add failed", zap.Error(err), zap.String("add_node_id", ev.AddNodeEvent.NodeID))
		}
		return ErrRaftApply(err)
	}
	return nil
}

func (n *Node) ApplyNodeRemoveEvent(ev *protocol.MetadataEvent) error {
	if n.raft.State() != raft.Leader {
		n.Logger.Debug("not leader, skipping node remove event", zap.String("node_id", ev.RemoveNodeEvent.NodeID))
		return nil
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	f := n.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		if ev.RemoveNodeEvent != nil {
			n.Logger.Error("raft apply node remove failed", zap.Error(err), zap.String("remove_node_id", ev.RemoveNodeEvent.NodeID))
		}
		return ErrRaftApply(err)
	}
	return nil
}

func (n *Node) ApplyIsrUpdateEvent(ev *protocol.MetadataEvent) error {
	if n.raft.State() != raft.Leader {
		n.Logger.Debug("not leader, skipping ISR update event", zap.String("topic", ev.IsrUpdateEvent.Topic))
		return nil
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	f := n.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		// During shutdown or leadership change, apply fails; log at Debug to avoid noise.
		msg := err.Error()
		if strings.Contains(msg, "shutdown") || strings.Contains(msg, "leadership lost") {
			n.Logger.Debug("raft apply ISR update failed (shutdown or leadership change)", zap.Error(err))
		} else {
			n.Logger.Error("raft apply ISR update failed", zap.Error(err))
		}
		return ErrRaftApply(err)
	}
	return nil
}

func (n *Node) Leave(id string) error {
	if !n.IsLeader() {
		n.WaitforLeaderWithRetryBackoff(2*time.Second, 5)
	}
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
	n.ApplyNodeRemoveEvent(&protocol.MetadataEvent{
		RemoveNodeEvent: &protocol.RemoveNodeEvent{
			NodeID: id,
		},
	})
	return nil
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

func (n *Node) WaitforLeaderWithRetryBackoff(timeout time.Duration, retryCount int) error {
	timeoutc := time.After(timeout)
	backoff := time.Duration(1 * time.Second)
	for i := 0; i < retryCount; i++ {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out waiting for leader")
		default:
			if n.IsLeader() {
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
	f := n.raft.Shutdown()
	return f.Error()
}
