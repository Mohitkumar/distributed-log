package node

import (
	"encoding/json"
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

// Node represents a cluster node (Raft + membership). RPC server, topic manager,
// and consumer manager are built in the helper to avoid cyclic dependencies.
type Node struct {
	NodeID        string
	NodeAddr      string
	Logger        *zap.Logger
	raft          *raft.Raft
	cfg           config.Config
	metadataStore *raftmeta.MetadataStore
}

// NewNodeFromConfig creates a Node. If logger is nil, zap.NewNop() is used.
func NewNodeFromConfig(config config.Config, logger *zap.Logger) (*Node, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	fsm, err := coordinator.NewCoordinatorFSM(config.RaftConfig.Dir)
	if err != nil {
		return nil, err
	}
	raftNode, err := coordinator.SetupRaft(fsm, config.RaftConfig.ID, config.RaftConfig.Address, config.RaftConfig.Dir, config.RaftConfig.Boostatrap)
	if err != nil {
		return nil, err
	}
	rpcAddr, err := config.RPCAddr()
	if err != nil {
		return nil, err
	}
	n := &Node{
		NodeID:        config.NodeConfig.ID,
		NodeAddr:      rpcAddr,
		Logger:        logger,
		raft:          raftNode,
		cfg:           config,
		metadataStore: fsm.MetadataStore,
	}
	n.Logger.Info("node started", zap.String("raft_addr", config.RaftConfig.Address), zap.String("rpc_addr", rpcAddr))
	return n, nil
}

// GetLeaderRpcAddr returns the Raft leader's RPC address. If this node is the leader, returns n.NodeAddr.
func (n *Node) GetLeaderRpcAddr() string {
	if n.raft.State() == raft.Leader {
		return n.NodeAddr
	}
	leaderAddr := n.raft.Leader()
	if leaderAddr == "" {
		return ""
	}
	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return ""
	}
	for _, srv := range configFuture.Configuration().Servers {
		if srv.Address == leaderAddr {
			return n.GetRpcAddrForNodeID(string(srv.ID))
		}
	}
	return ""
}

// GetRpcAddrForNodeID returns the RPC address for a node ID from metadata store. Returns "" if node not found.
func (n *Node) GetRpcAddrForNodeID(nodeID string) string {
	meta := n.metadataStore.GetNodeMetadata(nodeID)
	if meta == nil {
		return ""
	}
	return meta.RpcAddr
}

// GetTopicLeaderRpcAddr returns the RPC address of the topic leader from metadata. Returns "" if topic not found or leader has no RPC addr.
func (n *Node) GetTopicLeaderRpcAddr(topic string) string {
	tm := n.metadataStore.GetTopic(topic)
	if tm == nil {
		return ""
	}
	return n.GetRpcAddrForNodeID(tm.LeaderNodeID)
}

// GetClusterNodeIDs returns all node IDs in the Raft configuration.
func (n *Node) GetClusterNodeIDs() []string {
	nodes := n.metadataStore.Nodes
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.NodeID)
	}
	return ids
}

// TopicExists returns true if the topic is in the metadata store (created via Raft).
func (n *Node) TopicExists(topic string) bool {
	return n.metadataStore.GetTopic(topic) != nil
}

// ApplyCreateTopicEvent replicates a CreateTopicEvent through Raft. Call only on the Raft leader.
func (n *Node) ApplyCreateTopicEvent(ev *protocol.MetadataEvent) error {
	if ev.CreateTopicEvent != nil {
		n.Logger.Info("applying create topic event", zap.String("topic", ev.CreateTopicEvent.Topic), zap.String("leader_node_id", ev.CreateTopicEvent.LeaderNodeID))
	}
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

// ApplyDeleteTopicEvent replicates a DeleteTopicEvent through Raft. Call only on the Raft leader (typically by the topic leader after deleting the topic).
func (n *Node) ApplyDeleteTopicEvent(ev *protocol.MetadataEvent) error {
	if ev.DeleteTopicEvent != nil {
		n.Logger.Info("applying delete topic event", zap.String("topic", ev.DeleteTopicEvent.Topic))
	}
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

// Join adds a node to the Raft cluster and records it in metadata. raftAddr is the node's Raft transport address;
// rpcAddr is the node's RPC address (for CreateTopic/Produce forwarding). If rpcAddr is empty, raftAddr is used for both.
func (n *Node) Join(id, raftAddr, rpcAddr string) error {
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
	// Ensure this node (bootstrap leader) is in metadata so GetClusterNodeIDs() and GetRpcAddrForNodeID work.
	if n.metadataStore.GetNodeMetadata(n.NodeID) == nil {
		_ = n.ApplyNodeAddEvent(&protocol.MetadataEvent{
			AddNodeEvent: &protocol.AddNodeEvent{
				NodeID:  n.NodeID,
				Addr:    n.cfg.RaftConfig.Address,
				RpcAddr: n.NodeAddr,
			},
		})
	}
	addFuture := n.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}
	joinRpcAddr := rpcAddr
	if joinRpcAddr == "" {
		joinRpcAddr = raftAddr
	}
	n.ApplyNodeAddEvent(&protocol.MetadataEvent{
		AddNodeEvent: &protocol.AddNodeEvent{
			NodeID:  id,
			Addr:    raftAddr,
			RpcAddr: joinRpcAddr,
		},
	})
	n.Logger.Info("node joined cluster", zap.String("node_id", id), zap.String("raft_addr", raftAddr), zap.String("rpc_addr", joinRpcAddr))
	return nil
}

func (n *Node) ApplyNodeAddEvent(ev *protocol.MetadataEvent) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	f := n.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		if ev.AddNodeEvent != nil {
			n.Logger.Error("raft apply node add failed", zap.Error(err), zap.String("node_id", ev.AddNodeEvent.NodeID))
		}
		return ErrRaftApply(err)
	}
	return nil
}

func (n *Node) ApplyNodeRemoveEvent(ev *protocol.MetadataEvent) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	f := n.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		if ev.RemoveNodeEvent != nil {
			n.Logger.Error("raft apply node remove failed", zap.Error(err), zap.String("node_id", ev.RemoveNodeEvent.NodeID))
		}
		return ErrRaftApply(err)
	}
	return nil
}

func (n *Node) ApplyIsrUpdateEvent(ev *protocol.MetadataEvent) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	f := n.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		n.Logger.Error("raft apply ISR update failed", zap.Error(err))
		return ErrRaftApply(err)
	}
	return nil
}

func (n *Node) Leave(id string) error {
	removeFuture := n.raft.RemoveServer(raft.ServerID(id), 0, 0)
	if err := removeFuture.Error(); err != nil {
		return err
	}
	n.ApplyNodeRemoveEvent(&protocol.MetadataEvent{
		RemoveNodeEvent: &protocol.RemoveNodeEvent{
			NodeID: id,
		},
	})
	n.Logger.Info("node left cluster", zap.String("node_id", id))
	return nil
}

// GetNodeID returns this node's ID.
func (n *Node) GetNodeID() string {
	return n.NodeID
}

// GetNodeAddr returns this node's RPC address.
func (n *Node) GetNodeAddr() string {
	return n.NodeAddr
}

// GetOtherNodes returns other cluster nodes with their RPC addresses (from metadata), not Raft addresses.
// CreateReplica and other RPCs must dial RpcAddr; using Raft srv.Address would send RPC to Raft transport and cause msgpack/EOF errors.
func (n *Node) GetOtherNodes() []common.NodeInfo {
	servers := n.raft.GetConfiguration().Configuration().Servers
	nodes := make([]common.NodeInfo, 0)
	for _, srv := range servers {
		if srv.ID == raft.ServerID(n.NodeID) {
			continue
		}
		nodeID := string(srv.ID)
		rpcAddr := n.GetRpcAddrForNodeID(nodeID)
		if rpcAddr == "" {
			rpcAddr = string(srv.Address) // fallback for tests that haven't set metadata
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

func (n *Node) Shutdown() error {
	n.Logger.Info("node shutting down", zap.String("node_id", n.NodeID))
	f := n.raft.Shutdown()
	return f.Error()
}
