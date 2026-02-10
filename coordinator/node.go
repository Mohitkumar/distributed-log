package coordinator

import (
	"github.com/mohitkumar/mlog/client"
)

// Node holds this process's identity (addresses) and RPC clients to other nodes.
// Cluster and Raft responsibility stays in Coordinator; Node is only addresses + client connections.
type Node struct {
	NodeID            string
	NodeRPCAddr       string
	NodeRaftAddr      string
	nodeClients       map[string]*client.RemoteClient
	nodeStreamClients map[string]*client.RemoteStreamClient
	coord             *Coordinator
}

func newNode(coord *Coordinator, nodeID, nodeRPCAddr, nodeRaftAddr string) *Node {
	return &Node{
		NodeID:            nodeID,
		NodeRPCAddr:       nodeRPCAddr,
		NodeRaftAddr:      nodeRaftAddr,
		nodeClients:       make(map[string]*client.RemoteClient),
		nodeStreamClients: make(map[string]*client.RemoteStreamClient),
		coord:             coord,
	}
}

func (n *Node) GetNodeID() string {
	return n.NodeID
}

func (n *Node) GetNodeAddr() string {
	return n.NodeRPCAddr
}

func (n *Node) GetRpcClient(nodeID string) (*client.RemoteClient, error) {
	cl, ok := n.nodeClients[nodeID]
	if !ok {
		return nil, ErrNodeNotFound
	}
	return cl, nil
}

func (n *Node) GetRpcStreamClient(nodeID string) (*client.RemoteStreamClient, error) {
	cl, ok := n.nodeStreamClients[nodeID]
	if !ok {
		return nil, ErrNodeNotFound
	}
	return cl, nil
}

func (n *Node) GetTopicLeaderClient(topic string) *client.RemoteClient {
	tm := n.coord.metadataStore.GetTopic(topic)
	if tm == nil || tm.LeaderNodeID == "" {
		return nil
	}
	cl, _ := n.GetRpcClient(tm.LeaderNodeID)
	return cl
}

func (n *Node) GetTopicLeaderStreamClient(topic string) *client.RemoteStreamClient {
	tm := n.coord.metadataStore.GetTopic(topic)
	if tm == nil || tm.LeaderNodeID == "" {
		return nil
	}
	cl, _ := n.GetRpcStreamClient(tm.LeaderNodeID)
	return cl
}

func (n *Node) GetTopicReplicaClients(topic string) map[string]*client.RemoteClient {
	tm := n.coord.metadataStore.GetTopic(topic)
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

// addClient stores a client for the given node ID (used by Join).
func (n *Node) addClient(nodeID string, rpc *client.RemoteClient, stream *client.RemoteStreamClient) {
	if rpc != nil {
		n.nodeClients[nodeID] = rpc
	}
	if stream != nil {
		n.nodeStreamClients[nodeID] = stream
	}
}

// removeClient closes and removes the client for the given node ID (used by Leave).
func (n *Node) removeClient(nodeID string) {
	if cl, ok := n.nodeClients[nodeID]; ok {
		cl.Close()
		delete(n.nodeClients, nodeID)
	}
	if streamCl, ok := n.nodeStreamClients[nodeID]; ok {
		streamCl.Close()
		delete(n.nodeStreamClients, nodeID)
	}
}
