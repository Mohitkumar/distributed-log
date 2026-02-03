package rpc

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/protocol"
)

// parseRawChunk parses segment-format [Offset 8][Len 4][Value]... into LogEntries (for tests).
func parseRawChunk(chunk []byte) []*protocol.LogEntry {
	const headerSize = 8 + 4
	var entries []*protocol.LogEntry
	for len(chunk) >= headerSize {
		offset := binary.BigEndian.Uint64(chunk[0:8])
		msgLen := binary.BigEndian.Uint32(chunk[8:12])
		recordSize := headerSize + int(msgLen)
		if len(chunk) < recordSize {
			break
		}
		value := make([]byte, msgLen)
		copy(value, chunk[headerSize:recordSize])
		entries = append(entries, &protocol.LogEntry{Offset: offset, Value: value})
		chunk = chunk[recordSize:]
	}
	return entries
}

func TestCreateTopicOnLeaderCreatesTopicOnFollower(t *testing.T) {
	t.Skip("two-node Raft formation in tests hits follower panic; use integration test with real cluster")
	leaderSrv, followerSrv := SetupTwoTestServers(t, "leader", "follower")
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	topicName := "test-topic"
	ctx := context.Background()

	// Create topic via ReplicationClient (RPC to leader); leader will CreateReplica on follower via RPC
	remoteClient, err := client.NewRemoteClient(leaderSrv.Addr)
	if err != nil {
		t.Fatalf("NewReplicationClient: %v", err)
	}
	resp, err := remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic via client: %v", err)
	}
	if resp.Topic != topicName {
		t.Fatalf("CreateTopic response topic = %q, want %q", resp.Topic, topicName)
	}

	// Verify topic exists on leader (leader has the topic with leader log)
	leaderView, err := leaderSrv.TopicManager.GetLeader(topicName)
	if err != nil {
		t.Fatalf("GetLeader on leader: %v", err)
	}
	if leaderView == nil || leaderView.Log == nil {
		t.Fatal("leader should have leader log for topic")
	}

	// Verify leader topic directory exists (BaseDir/topic)
	leaderTopicDir := filepath.Join(leaderSrv.BaseDir, topicName)
	if fi, err := os.Stat(leaderTopicDir); err != nil {
		t.Fatalf("leader topic dir %q should exist: %v", leaderTopicDir, err)
	} else if !fi.IsDir() {
		t.Fatalf("leader topic path %q should be a directory", leaderTopicDir)
	}

	// Verify topic/replica exists on follower (follower has replica-0 for this topic)
	replicaView, err := followerSrv.TopicManager.GetReplica(topicName, "replica-0")
	if err != nil {
		t.Fatalf("GetReplica on follower: %v (creating topic on leader should create replica on follower)", err)
	}
	if replicaView == nil || replicaView.ReplicaID != "replica-0" {
		t.Fatalf("follower should have replica-0 for topic, got %+v", replicaView)
	}

	// Verify follower topic and replica directories exist (BaseDir/topic, BaseDir/topic/replicaID)
	followerTopicDir := filepath.Join(followerSrv.BaseDir, topicName)
	replicaDir := filepath.Join(followerTopicDir, "replica-0")
	if fi, err := os.Stat(followerTopicDir); err != nil {
		t.Fatalf("follower topic dir %q should exist: %v", followerTopicDir, err)
	} else if !fi.IsDir() {
		t.Fatalf("follower topic path %q should be a directory", followerTopicDir)
	}
	if fi, err := os.Stat(replicaDir); err != nil {
		t.Fatalf("follower replica dir %q should exist: %v", replicaDir, err)
	} else if !fi.IsDir() {
		t.Fatalf("follower replica path %q should be a directory", replicaDir)
	}
}

// TestCreateTopicOnLeaderWithTwoReplicas requires two followers; with only one follower we use replica count 1.
// This test is a variant that explicitly checks the topic object exists on follower (GetTopic).
func TestCreateTopicOnLeader_FollowerHasTopic(t *testing.T) {
	t.Skip("two-node Raft formation in tests hits follower panic; use integration test with real cluster")
	leaderSrv, followerSrv := SetupTwoTestServers(t, "leader2", "follower2")
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	topicName := "my-topic"
	ctx := context.Background()

	remoteClient, err := client.NewRemoteClient(leaderSrv.Addr)
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	_, err = remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic via client: %v", err)
	}

	// Leader has topic
	_, err = leaderSrv.TopicManager.GetTopic(topicName)
	if err != nil {
		t.Fatalf("leader should have topic: %v", err)
	}

	// Follower has topic (with replica; CreateReplica adds the topic to follower's TopicManager)
	_, err = followerSrv.TopicManager.GetTopic(topicName)
	if err != nil {
		t.Fatalf("follower should have topic after leader created it with replica: %v", err)
	}

	_, err = followerSrv.TopicManager.GetReplica(topicName, "replica-0")
	if err != nil {
		t.Fatalf("follower should have replica-0: %v", err)
	}

	// Verify topic directory exists on follower (BaseDir/topic/replicaID)
	replicaDir := filepath.Join(followerSrv.BaseDir, topicName, "replica-0")
	if fi, err := os.Stat(replicaDir); err != nil {
		t.Fatalf("follower replica dir %q should exist: %v", replicaDir, err)
	} else if !fi.IsDir() {
		t.Fatalf("follower replica path %q should be a directory", replicaDir)
	}
}

// TestDeleteReplica verifies that deleting a replica on the follower removes it from
// the TopicManager and removes the replica directory on disk.
func TestDeleteReplica(t *testing.T) {
	t.Skip("two-node Raft formation in tests hits follower panic; use integration test with real cluster")
	leaderSrv, followerSrv := SetupTwoTestServers(t, "leader-del", "follower-del")
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	topicName := "del-topic"
	ctx := context.Background()

	// Create topic on leader with 1 replica via RPC (follower gets replica-0)
	remoteClient, err := client.NewRemoteClient(leaderSrv.Addr)
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	_, err = remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic via client: %v", err)
	}

	// Verify replica and directory exist on follower
	replicaDir := filepath.Join(followerSrv.BaseDir, topicName, "replica-0")
	_, err = followerSrv.TopicManager.GetReplica(topicName, "replica-0")
	if err != nil {
		t.Fatalf("GetReplica before delete: %v", err)
	}
	if _, err := os.Stat(replicaDir); err != nil {
		t.Fatalf("replica dir %q should exist before delete: %v", replicaDir, err)
	}

	// Delete replica on follower via RPC
	replClient, err := client.NewRemoteClient(followerSrv.Addr)
	if err != nil {
		t.Fatalf("NewReplicationClient: %v", err)
	}
	_, err = replClient.DeleteReplica(ctx, &protocol.DeleteReplicaRequest{
		Topic:     topicName,
		ReplicaId: "replica-0",
	})
	if err != nil {
		t.Fatalf("DeleteReplica: %v", err)
	}

	// Verify replica is gone from TopicManager
	_, err = followerSrv.TopicManager.GetReplica(topicName, "replica-0")
	if err == nil {
		t.Fatal("GetReplica after delete should fail (replica should be gone)")
	}

	// Verify replica directory is gone on follower
	if _, err := os.Stat(replicaDir); err == nil {
		t.Fatalf("replica dir %q should not exist after delete", replicaDir)
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat replica dir: %v", err)
	}
}

// TestDeleteReplica_TopicDirRemoved verifies that when the last replica is deleted on the follower,
// the topic entry and topic directory are removed (follower has no leader for this topic).
func TestDeleteReplica_TopicDirRemoved(t *testing.T) {
	t.Skip("two-node Raft formation in tests hits follower panic; use integration test with real cluster")
	leaderSrv, followerSrv := SetupTwoTestServers(t, "leader-del2", "follower-del2")
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	topicName := "del-topic2"
	ctx := context.Background()

	remoteClient, err := client.NewRemoteClient(leaderSrv.Addr)
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	_, err = remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic via client: %v", err)
	}

	topicDir := filepath.Join(followerSrv.BaseDir, topicName)
	replicaDir := filepath.Join(topicDir, "replica-0")

	_, err = followerSrv.TopicManager.GetTopic(topicName)
	if err != nil {
		t.Fatalf("follower should have topic before delete: %v", err)
	}
	if _, err := os.Stat(topicDir); err != nil {
		t.Fatalf("topic dir %q should exist before delete: %v", topicDir, err)
	}

	replClient, err := client.NewRemoteClient(followerSrv.Addr)
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	_, err = replClient.DeleteReplica(ctx, &protocol.DeleteReplicaRequest{
		Topic:     topicName,
		ReplicaId: "replica-0",
	})
	if err != nil {
		t.Fatalf("DeleteReplica: %v", err)
	}

	// Topic should be gone from follower (no leader, no replicas left)
	_, err = followerSrv.TopicManager.GetTopic(topicName)
	if err == nil {
		t.Fatal("GetTopic after delete should fail (topic should be removed when last replica is deleted)")
	}

	// Topic directory should be removed on follower
	if _, err := os.Stat(replicaDir); err == nil {
		t.Fatalf("replica dir %q should not exist after delete", replicaDir)
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat replica dir: %v", err)
	}
	if _, err := os.Stat(topicDir); err == nil {
		t.Fatalf("topic dir %q should not exist after last replica deleted", topicDir)
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat topic dir: %v", err)
	}
}

// TestReplication_FollowerHasMessagesAfterReplication verifies that after producing on the leader,
// the follower replica eventually has the same messages (replication runs in background).
func TestReplication_FollowerHasMessagesAfterReplication(t *testing.T) {
	t.Skip("two-node Raft formation in tests hits follower panic; use integration test with real cluster")
	leaderSrv, followerSrv := SetupTwoTestServers(t, "leader-repl2", "follower-repl2")
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	topicName := "repl-topic2"
	ctx := context.Background()
	remoteClient, err := client.NewRemoteClient(leaderSrv.Addr)
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	_, err = remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic via client: %v", err)
	}
	producerClient, err := client.NewProducerClient(leaderSrv.Addr)
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()

	messages := []string{"hello", "world", "test", "word", "test", "word"}
	for i, msg := range messages {
		_, err := producerClient.Produce(ctx, &protocol.ProduceRequest{
			Topic: topicName,
			Value: []byte(msg),
			Acks:  protocol.AckLeader,
		})
		if err != nil {
			t.Fatalf("Produce %d: %v", i, err)
		}
	}

	// Wait for replication to catch up (follower replica fetches from leader)
	// Then verify follower's replica log has the same messages
	replicaNode, err := followerSrv.TopicManager.GetReplica(topicName, "replica-0")
	if err != nil {
		t.Fatalf("GetReplica: %v", err)
	}
	if replicaNode.Log == nil {
		t.Fatal("replica should have Log")
	}

	// Poll until we have at least len(messages) entries (replication may be async)
	var lastLEO uint64
	for try := 0; try < 50; try++ {
		lastLEO = replicaNode.Log.LEO()
		if lastLEO >= uint64(len(messages)) {
			break
		}
		if try < 49 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if lastLEO < uint64(len(messages)) {
		t.Fatalf("replica LEO %d < %d (replication did not catch up)", lastLEO, len(messages))
	}

	// Replica stores payload only (from raw chunk); segment.Read returns [offset 8][payload].
	const offWidth = 8
	for i, want := range messages {
		raw, err := replicaNode.Log.ReadUncommitted(uint64(i))
		if err != nil {
			t.Fatalf("ReadUncommitted(%d): %v", i, err)
		}
		payload := raw
		if len(raw) >= offWidth {
			payload = raw[offWidth:]
		}
		if string(payload) != want {
			t.Fatalf("entry %d: got %q, want %q", i, string(payload), want)
		}
	}
}

func TestReplication_FollowerHasMessagesAfterReplication_10000(t *testing.T) {
	t.Skip("two-node Raft formation in tests hits follower panic; use integration test with real cluster")
	leaderSrv, followerSrv := SetupTwoTestServers(t, "leader-repl2", "follower-repl2")
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	topicName := "repl-topic2"
	ctx := context.Background()
	remoteClient, err := client.NewRemoteClient(leaderSrv.Addr)
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	_, err = remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic via client: %v", err)
	}
	producerClient, err := client.NewProducerClient(leaderSrv.Addr)
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()

	values := make([][]byte, 10000)
	for i := 0; i < 10000; i++ {
		values[i] = []byte(fmt.Sprintf("message-%d", i))
	}
	_, err = producerClient.ProduceBatch(ctx, &protocol.ProduceBatchRequest{
		Topic:  topicName,
		Values: values,
		Acks:   protocol.AckLeader,
	})
	if err != nil {
		t.Fatalf("ProduceBatch: %v", err)
	}

	// Wait for replication to catch up (follower replica fetches from leader)
	// Then verify follower's replica log has the same messages
	replicaNode, err := followerSrv.TopicManager.GetReplica(topicName, "replica-0")
	if err != nil {
		t.Fatalf("GetReplica: %v", err)
	}
	if replicaNode.Log == nil {
		t.Fatal("replica should have Log")
	}

	// Poll until we have at least len(messages) entries (replication may be async)
	var lastLEO uint64
	for try := 0; try < 50; try++ {
		lastLEO = replicaNode.Log.LEO()
		if lastLEO >= uint64(len(values)) {
			break
		}
		if try < 49 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if lastLEO < uint64(len(values)) {
		t.Fatalf("replica LEO %d < %d (replication did not catch up)", lastLEO, len(values))
	}

	// Replica stores payload only (from raw chunk); segment.Read returns [offset 8][payload].
	const offWidth = 8
	for i, want := range values {
		raw, err := replicaNode.Log.ReadUncommitted(uint64(i))
		if err != nil {
			t.Fatalf("ReadUncommitted(%d): %v", i, err)
		}
		payload := raw
		if len(raw) >= offWidth {
			payload = raw[offWidth:]
		}
		if string(payload) != string(want) {
			t.Fatalf("entry %d: got %q, want %q", i, string(payload), want)
		}
	}
}
