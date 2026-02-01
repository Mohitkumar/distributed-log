package node

import (
	"github.com/mohitkumar/mlog/rpc"
)

// Node represents a physical server running an RPC server.
type Node struct {
	NodeID    string
	NodeAddr  string
	rpcServer *rpc.RpcServer
}

func NewNode(nodeID string, nodeAddr string, rpcServer *rpc.RpcServer) (*Node, error) {
	return &Node{
		NodeID:    nodeID,
		NodeAddr:  nodeAddr,
		rpcServer: rpcServer,
	}, nil
}

func (n *Node) Start() error {
	return n.rpcServer.Start()
}
