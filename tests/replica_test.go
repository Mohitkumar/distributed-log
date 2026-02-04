package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/protocol"
)

func TestCreateTopicOnLeaderCreatesTopicOnFollower(t *testing.T) {
	server1, server2 := SetupTwoTestServers(t, "server1", "server2")
	defer server1.Cleanup()
	defer server2.Cleanup()

	topicName := "test-topic"
	ctx := context.Background()

	// Create topic via ReplicationClient (RPC to leader); leader will CreateReplica on follower via RPC
	remoteClient, err := client.NewRemoteClient(server1.Addr)
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
	leaderView, err := server1.TopicManager.GetLeader(topicName)
	if err != nil {
		t.Fatalf("GetLeader on leader: %v", err)
	}
	if leaderView == nil || leaderView.Log == nil {
		t.Fatal("leader should have leader log for topic")
	}

	// Verify leader topic directory exists (BaseDir/topic)
	leaderTopicDir := filepath.Join(server1.BaseDir, topicName)
	if fi, err := os.Stat(leaderTopicDir); err != nil {
		t.Fatalf("leader topic dir %q should exist: %v", leaderTopicDir, err)
	} else if !fi.IsDir() {
		t.Fatalf("leader topic path %q should be a directory", leaderTopicDir)
	}

	// Verify topic/replica exists on follower (follower has replica for this topic)
	replicaTopic, err := server2.TopicManager.GetTopic(topicName)
	if err != nil {
		t.Fatalf("GetTopic on follower: %v (creating topic on leader should create replica on follower)", err)
	}
	if replicaTopic == nil || replicaTopic.Log == nil {
		t.Fatalf("follower should have topic with replica log, got %+v", replicaTopic)
	}

	// Verify follower topic directory exists (BaseDir/topic)
	followerTopicDir := filepath.Join(server2.BaseDir, topicName)
	if fi, err := os.Stat(followerTopicDir); err != nil {
		t.Fatalf("follower topic dir %q should exist: %v", followerTopicDir, err)
	} else if !fi.IsDir() {
		t.Fatalf("follower topic path %q should be a directory", followerTopicDir)
	}
}

// TestCreateTopicOnLeaderWithTwoReplicas requires two followers; with only one follower we use replica count 1.
// This test is a variant that explicitly checks the topic object exists on follower (GetTopic).
func TestCreateTopicOnLeader_FollowerHasTopic(t *testing.T) {
	server1, server2 := SetupTwoTestServers(t, "leader2", "follower2")
	defer server1.Cleanup()
	defer server2.Cleanup()

	topicName := "my-topic"
	ctx := context.Background()

	remoteClient, err := client.NewRemoteClient(server1.Addr)
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
	_, err = server1.TopicManager.GetTopic(topicName)
	if err != nil {
		t.Fatalf("leader should have topic: %v", err)
	}

	// Follower has topic (with replica; CreateReplica adds the topic to follower's TopicManager)
	_, err = server2.TopicManager.GetTopic(topicName)
	if err != nil {
		t.Fatalf("follower should have topic after leader created it with replica: %v", err)
	}

	// Verify topic directory exists on follower (BaseDir/topic/replicaID)
	replicaDir := filepath.Join(server2.BaseDir, topicName)
	if fi, err := os.Stat(replicaDir); err != nil {
		t.Fatalf("follower replica dir %q should exist: %v", replicaDir, err)
	} else if !fi.IsDir() {
		t.Fatalf("follower replica path %q should be a directory", replicaDir)
	}
}

// TestDeleteReplica verifies that deleting a replica on the follower removes it from
// the TopicManager and removes the replica directory on disk.
func TestDeleteReplica(t *testing.T) {
	server1, server2 := SetupTwoTestServers(t, "leader-del", "follower-del")
	defer server1.Cleanup()
	defer server2.Cleanup()

	topicName := "del-topic"
	ctx := context.Background()

	// Create topic on leader with 1 replica via RPC (follower gets replica-0)
	remoteClient, err := client.NewRemoteClient(server1.Addr)
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
	replicaDir := filepath.Join(server2.BaseDir, topicName)
	_, err = server2.TopicManager.GetTopic(topicName)
	if err != nil {
		t.Fatalf("GetTopic before delete: %v", err)
	}
	if _, err := os.Stat(replicaDir); err != nil {
		t.Fatalf("replica dir %q should exist before delete: %v", replicaDir, err)
	}

	// Delete replica on follower via RPC
	replClient, err := client.NewRemoteClient(server2.Addr)
	if err != nil {
		t.Fatalf("NewReplicationClient: %v", err)
	}
	_, err = replClient.DeleteReplica(ctx, &protocol.DeleteReplicaRequest{
		Topic: topicName,
	})
	if err != nil {
		t.Fatalf("DeleteReplica: %v", err)
	}

	// Verify replica is gone from TopicManager
	_, err = server2.TopicManager.GetTopic(topicName)
	if err == nil {
		t.Fatal("GetTopic after delete should fail (replica should be gone)")
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
	server1, server2 := SetupTwoTestServers(t, "leader-del2", "follower-del2")
	defer server1.Cleanup()
	defer server2.Cleanup()

	topicName := "del-topic2"
	ctx := context.Background()

	remoteClient, err := client.NewRemoteClient(server1.Addr)
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

	topicDir := filepath.Join(server2.BaseDir, topicName)
	replicaDir := filepath.Join(topicDir, "replica-0")

	_, err = server2.TopicManager.GetTopic(topicName)
	if err != nil {
		t.Fatalf("follower should have topic before delete: %v", err)
	}
	if _, err := os.Stat(topicDir); err != nil {
		t.Fatalf("topic dir %q should exist before delete: %v", topicDir, err)
	}

	replClient, err := client.NewRemoteClient(server2.Addr)
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	_, err = replClient.DeleteReplica(ctx, &protocol.DeleteReplicaRequest{
		Topic: topicName,
	})
	if err != nil {
		t.Fatalf("DeleteReplica: %v", err)
	}

	// Topic should be gone from follower (no leader, no replicas left)
	_, err = server2.TopicManager.GetTopic(topicName)
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
	server1, server2 := SetupTwoTestServers(t, "leader-repl2", "follower-repl2")
	defer server1.Cleanup()
	defer server2.Cleanup()

	topicName := "repl-topic2"
	ctx := context.Background()
	remoteClient, err := client.NewRemoteClient(server1.Addr)
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
	producerClient, err := client.NewProducerClient(server1.Addr)
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
	replicaTopic, err := server2.TopicManager.GetTopic(topicName)
	if err != nil {
		t.Fatalf("GetTopic (replica): %v", err)
	}
	if replicaTopic.Log == nil {
		t.Fatal("replica should have Log")
	}

	// Poll until we have at least len(messages) entries (replication may be async)
	var lastLEO uint64
	for try := 0; try < 50; try++ {
		lastLEO = replicaTopic.Log.LEO()
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
		raw, err := replicaTopic.Log.ReadUncommitted(uint64(i))
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
	server1, server2 := SetupTwoTestServers(t, "leader-repl2", "follower-repl2")
	defer server1.Cleanup()
	defer server2.Cleanup()

	topicName := "repl-topic2"
	ctx := context.Background()
	remoteClient, err := client.NewRemoteClient(server1.Addr)
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
	producerClient, err := client.NewProducerClient(server1.Addr)
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
	replicaTopic, err := server2.TopicManager.GetTopic(topicName)
	if err != nil {
		t.Fatalf("GetTopic (replica): %v", err)
	}
	if replicaTopic.Log == nil {
		t.Fatal("replica should have Log")
	}

	// Poll until we have at least len(values) entries (replication may be async)
	var lastLEO uint64
	for try := 0; try < 50; try++ {
		lastLEO = replicaTopic.Log.LEO()
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
		raw, err := replicaTopic.Log.ReadUncommitted(uint64(i))
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
