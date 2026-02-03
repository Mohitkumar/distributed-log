package node

import (
	"path/filepath"
	"testing"

	"github.com/mohitkumar/mlog/config"
)

// TestNewNodeFromConfig verifies that a Node can be created from config (Raft only; no RPC server).
func TestNewNodeFromConfig(t *testing.T) {
	baseDir := t.TempDir()
	cfg := config.Config{
		BindAddr: "127.0.0.1:9092",
		NodeConfig: config.NodeConfig{
			ID:      "node-1",
			RPCPort: 9092,
			DataDir: baseDir,
		},
		RaftConfig: config.RaftConfig{
			ID:         "node-1",
			Address:    "127.0.0.1:9093",
			Dir:        filepath.Join(baseDir, "raft"),
			Boostatrap: false, // bootstrap requires proper setup; skip for unit test
		},
	}
	n, err := NewNodeFromConfig(cfg)
	if err != nil {
		t.Fatalf("NewNodeFromConfig: %v", err)
	}
	defer n.Shutdown()
	if n.NodeID != "node-1" || n.NodeAddr == "" {
		t.Fatalf("Node fields: got NodeID=%q NodeAddr=%q", n.NodeID, n.NodeAddr)
	}
}
