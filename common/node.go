package common

import (
	"sync"

	"github.com/mohitkumar/mlog/client"
)

// Node holds this process's identity (addresses) and RPC clients to other nodes.
// Cluster and Raft responsibility stays in Coordinator; Node is only addresses + client connections.
type Node struct {
	mu           sync.RWMutex
	NodeID       string
	RPCAddr      string
	remoteClient *client.RemoteClient
	streamClient *client.RemoteStreamClient
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

func (n *Node) GetRpcStreamClient() (*client.RemoteStreamClient, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.streamClient == nil {
		streamClient, err := client.NewRemoteStreamClient(n.RPCAddr)
		if err != nil {
			return nil, err
		}
		n.streamClient = streamClient
	}
	return n.streamClient, nil
}
