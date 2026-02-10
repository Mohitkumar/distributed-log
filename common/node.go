package common

// Node holds identity (node ID and RPC address) for a cluster node.
// Used by TopicManager and tests; RPC clients are obtained via TopicCoordinator.GetRpcClient(nodeID).
type Node struct {
	NodeID  string
	RPCAddr string
}
