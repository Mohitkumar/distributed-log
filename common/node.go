package common

import (
	"sync"

	"github.com/mohitkumar/mlog/client"
)

// Node holds this process's identity (addresses) and RPC clients to other nodes.
// Cluster and Raft responsibility stays in Coordinator; Node is only addresses + client connections.
type Node struct {
	mu                sync.RWMutex
	NodeID            string
	RPCAddr           string
	remoteClient      *client.RemoteClient
	replicationClient *client.RemoteClient // dedicated connection for replication; multiplexed for all topics
}

func NewNode(nodeID, nodeRPCAddr string) *Node {
	return &Node{
		NodeID:  nodeID,
		RPCAddr: nodeRPCAddr,
	}
}

func (n *Node) GetNodeID() string {
	return n.NodeID
}

func (n *Node) GetNodeAddr() string {
	return n.RPCAddr
}

func (n *Node) GetRpcClient() (*client.RemoteClient, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.remoteClient == nil {
		remoteClient, err := client.NewRemoteClient(n.RPCAddr)
		if err != nil {
			return nil, err
		}
		n.remoteClient = remoteClient
	}
	return n.remoteClient, nil
}

func (n *Node) GetReplicationClient() (*client.RemoteClient, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.replicationClient == nil {
		replClient, err := client.NewRemoteClient(n.RPCAddr)
		if err != nil {
			return nil, err
		}
		n.replicationClient = replClient
	}
	return n.replicationClient, nil
}
