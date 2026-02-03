package topic

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mohitkumar/mlog/config"
	"github.com/mohitkumar/mlog/node"
)

// testNode creates a Node for use in topic tests. Caller must call n.Shutdown() when done.
func testNode(t *testing.T, dir string) *node.Node {
	t.Helper()
	cfg := config.Config{
		BindAddr: "127.0.0.1:19092",
		NodeConfig: config.NodeConfig{
			ID:      "node-1",
			RPCPort: 19092,
			DataDir: dir,
		},
		RaftConfig: config.RaftConfig{
			ID:         "node-1",
			Address:    "127.0.0.1:19093",
			Dir:        filepath.Join(dir, "raft"),
			Boostatrap: false,
		},
	}
	n, err := node.NewNodeFromConfig(cfg)
	if err != nil {
		t.Fatalf("NewNodeFromConfig: %v", err)
	}
	return n
}

// TestTopicManager_CreateTopicLeaderOnly verifies that creating a topic
// with zero replicas sets up a leader and a backing log that can be written/read.
func TestTopicManager_CreateTopicLeaderOnly(t *testing.T) {
	dir := t.TempDir()
	n := testNode(t, dir)
	defer n.Shutdown()

	tm, err := NewTopicManager(dir, n)
	if err != nil {
		t.Fatalf("NewTopicManager error = %v", err)
	}

	const topicName = "test-topic"

	if err := tm.CreateTopic(topicName, 0); err != nil {
		t.Fatalf("CreateTopic error = %v", err)
	}

	leader, err := tm.GetLeader(topicName)
	if err != nil {
		t.Fatalf("GetLeader error = %v", err)
	}
	if leader == nil || leader.Log == nil {
		t.Fatalf("expected non-nil leader and log")
	}

	// Append a record and read it back.
	wantVal := []byte("hello-replication")
	off, err := leader.Log.Append(wantVal)
	if err != nil {
		t.Fatalf("Append error = %v", err)
	}
	if off != 0 {
		t.Fatalf("expected first offset 0, got %d", off)
	}

	got, err := leader.Log.Read(off)
	if err != nil {
		t.Fatalf("Read error = %v", err)
	}
	// Segment returns [offset 8 bytes][value]
	const offWidth = 8
	if len(got) < offWidth {
		t.Fatalf("Read returned short data")
	}
	if string(got[offWidth:]) != string(wantVal) {
		t.Fatalf("Read value = %q, want %q", got[offWidth:], wantVal)
	}
}

// TestTopicManager_DeleteTopic verifies that DeleteTopic removes the topic,
// closes and deletes the leader log directory.
func TestTopicManager_DeleteTopic(t *testing.T) {
	dir := t.TempDir()
	n := testNode(t, dir)
	defer n.Shutdown()

	tm, err := NewTopicManager(dir, n)
	if err != nil {
		t.Fatalf("NewTopicManager error = %v", err)
	}

	const topicName = "delete-topic"

	if err := tm.CreateTopic(topicName, 0); err != nil {
		t.Fatalf("CreateTopic error = %v", err)
	}

	// Ensure log directory exists.
	logDir := filepath.Join(dir, topicName)
	if _, err := os.Stat(logDir); err != nil {
		t.Fatalf("expected log dir %s to exist, got error: %v", logDir, err)
	}

	if err := tm.DeleteTopic(topicName); err != nil {
		t.Fatalf("DeleteTopic error = %v", err)
	}

	// Leader should no longer be found.
	if _, err := tm.GetLeader(topicName); err == nil {
		t.Fatalf("expected GetLeader to fail after DeleteTopic")
	}

	// Log directory should be removed.
	if _, err := os.Stat(logDir); !os.IsNotExist(err) {
		t.Fatalf("expected log dir %s to be removed, got err = %v", logDir, err)
	}
}
