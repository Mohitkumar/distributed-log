package node

import (
	"testing"

	"github.com/mohitkumar/mlog/consumer"
	"github.com/mohitkumar/mlog/rpc"
	"github.com/mohitkumar/mlog/topic"
)

// TestNewNode verifies that a Node can be created with an RPC server.
func TestNewNode(t *testing.T) {
	addr := "127.0.0.1:0"
	baseDir := t.TempDir()
	topicMgr, err := topic.NewTopicManager(baseDir, "node-1", addr, nil)
	if err != nil {
		t.Fatalf("NewTopicManager: %v", err)
	}
	consumerMgr, err := consumer.NewConsumerManager(baseDir)
	if err != nil {
		t.Fatalf("NewConsumerManager: %v", err)
	}
	srv := rpc.NewRpcServer(addr, topicMgr, consumerMgr)

	n, err := NewNode("node-1", addr, srv)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if n.NodeID != "node-1" || n.NodeAddr != addr {
		t.Fatalf("Node fields: got NodeID=%q NodeAddr=%q", n.NodeID, n.NodeAddr)
	}
}
