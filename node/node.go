package node

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mohitkumar/mlog/common"
	"github.com/mohitkumar/mlog/config"
	"github.com/mohitkumar/mlog/coordinator"
	"github.com/mohitkumar/mlog/discovery"
	"github.com/mohitkumar/mlog/protocol"
	"github.com/mohitkumar/mlog/raftmeta"
)

var _ discovery.Handler = (*Node)(nil)

// Node represents a cluster node (Raft + membership). RPC server, topic manager,
// and consumer manager are built in the helper to avoid cyclic dependencies.
type Node struct {
	NodeID        string
	NodeAddr      string
	raft          *raft.Raft
	cfg           config.Config
	metadataStore *raftmeta.MetadataStore
}

func NewNodeFromConfig(config config.Config) (*Node, error) {
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
		raft:          raftNode,
		cfg:           config,
		metadataStore: fsm.MetadataStore,
	}
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
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	f := n.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		return fmt.Errorf("raft apply: %w", err)
	}
	return nil
}

// ApplyDeleteTopicEvent replicates a DeleteTopicEvent through Raft. Call only on the Raft leader (typically by the topic leader after deleting the topic).
func (n *Node) ApplyDeleteTopicEvent(ev *protocol.MetadataEvent) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	f := n.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		return fmt.Errorf("raft apply: %w", err)
	}
	return nil
}

func (n *Node) Join(id, addr string) error {
	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)
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
		return err
	}
	n.ApplyNodeAddEvent(&protocol.MetadataEvent{
		AddNodeEvent: &protocol.AddNodeEvent{
			NodeID:  id,
			Addr:    addr,
			RpcAddr: n.NodeAddr,
		},
	})
	return nil
}

func (n *Node) ApplyNodeAddEvent(ev *protocol.MetadataEvent) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	f := n.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		return fmt.Errorf("raft apply: %w", err)
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
		return fmt.Errorf("raft apply: %w", err)
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
		return fmt.Errorf("raft apply: %w", err)
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

func (n *Node) GetOtherNodes() []common.NodeInfo {
	servers := n.raft.GetConfiguration().Configuration().Servers
	nodes := make([]common.NodeInfo, 0)
	for _, srv := range servers {
		if srv.ID == raft.ServerID(n.NodeID) {
			continue
		}
		nodes = append(nodes, common.NodeInfo{NodeID: string(srv.ID), RpcAddr: string(srv.Address)})
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
	f := n.raft.Shutdown()
	return f.Error()
}
