package node

import (
	"fmt"

	"github.com/hashicorp/raft"
	"github.com/mohitkumar/mlog/common"
	"github.com/mohitkumar/mlog/config"
	"github.com/mohitkumar/mlog/consumer"
	"github.com/mohitkumar/mlog/coordinator"
	"github.com/mohitkumar/mlog/discovery"
	"github.com/mohitkumar/mlog/rpc"
	"github.com/mohitkumar/mlog/topic"
	"go.uber.org/zap"
)

var _ discovery.Handler = (*Node)(nil)

// Node represents a physical server running an RPC server.
type Node struct {
	NodeID             string
	NodeAddr           string
	rpcServer          *rpc.RpcServer
	raft               *raft.Raft
	logger             *zap.Logger
	IsLeaderController bool
	cfg                config.Config
}

func NewNodeFromConfig(config config.Config) (*Node, error) {
	fsm, err := coordinator.NewCoordinatorFSM(config.RaftConfig.Dir)
	if err != nil {
		return nil, err
	}
	raft, err := coordinator.SetupRaft(fsm, config.RaftConfig.ID, config.RaftConfig.Address, config.RaftConfig.Dir, config.RaftConfig.Boostatrap)
	if err != nil {
		return nil, err
	}
	n := &Node{
		NodeID:   config.NodeConfig.ID,
		NodeAddr: config.NodeConfig.Address,
		raft:     raft,
		cfg:      config,
	}
	topicMgr, err := topic.NewTopicManager(config.NodeConfig.DataDir, config.NodeConfig.ID, config.NodeConfig.Address, n.GetOtherNodes)
	if err != nil {
		return nil, fmt.Errorf("create topic manager: %w", err)
	}

	consumerMgr, err := consumer.NewConsumerManager(config.NodeConfig.DataDir)
	if err != nil {
		return nil, fmt.Errorf("create consumer manager: %w", err)
	}
	rpcServer := rpc.NewRpcServer(config.NodeConfig.Address, topicMgr, consumerMgr)
	if err != nil {
		return nil, err
	}

	n.rpcServer = rpcServer
	return n, nil
}

func (n *Node) Start() error {
	return n.rpcServer.Start()
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
	return nil
}

func (n *Node) Leave(id string) error {
	removeFuture := n.raft.RemoveServer(raft.ServerID(id), 0, 0)
	if err := removeFuture.Error(); err != nil {
		return err
	}
	return nil
}

func (n *Node) GetOtherNodes() []common.NodeInfo {
	servers := n.raft.GetConfiguration().Configuration().Servers
	nodes := make([]common.NodeInfo, 0, len(servers)-1)
	for _, srv := range servers {
		if srv.ID == raft.ServerID(n.NodeID) {
			continue
		}
		nodes = append(nodes, common.NodeInfo{NodeID: string(srv.ID), RpcAddr: string(srv.Address)})
	}
	return nodes
}

func (n *Node) Shutdown() error {
	f := n.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}
	return n.rpcServer.Stop()
}
