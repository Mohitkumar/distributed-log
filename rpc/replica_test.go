package rpc

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/broker"
	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/protocol"
)

func TestCreateTopicOnLeaderCreatesTopicOnFollower(t *testing.T) {
	leaderSrv, followerSrv := SetupTwoTestServers(t, "leader", "follower")
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	topicName := "test-topic"

	// Create topic on leader with 1 replica (leader will CreateReplica on follower via RPC)
	if err := leaderSrv.TopicManager.CreateTopic(topicName, 1); err != nil {
		t.Fatalf("CreateTopic on leader: %v", err)
	}

	// Verify topic exists on leader (leader has the topic with a leader node)
	leaderNode, err := leaderSrv.TopicManager.GetLeader(topicName)
	if err != nil {
		t.Fatalf("GetLeader on leader: %v", err)
	}
	if leaderNode == nil || !leaderNode.IsLeader {
		t.Fatal("leader should have leader node for topic")
	}

	// Verify topic/replica exists on follower (follower has replica-0 for this topic)
	replicaNode, err := followerSrv.TopicManager.GetReplica(topicName, "replica-0")
	if err != nil {
		t.Fatalf("GetReplica on follower: %v (creating topic on leader should create replica on follower)", err)
	}
	if replicaNode == nil || replicaNode.ReplicaID != "replica-0" {
		t.Fatalf("follower should have replica-0 for topic, got %+v", replicaNode)
	}

	// Verify topic directory exists on follower (BaseDir/topic/replicaID)
	replicaDir := filepath.Join(followerSrv.BaseDir, topicName, "replica-0")
	if fi, err := os.Stat(replicaDir); err != nil {
		t.Fatalf("follower replica dir %q should exist: %v", replicaDir, err)
	} else if !fi.IsDir() {
		t.Fatalf("follower replica path %q should be a directory", replicaDir)
	}
}

// TestCreateTopicOnLeaderWithTwoReplicas requires two followers; with only one follower we use replica count 1.
// This test is a variant that explicitly checks the topic object exists on follower (GetTopic).
func TestCreateTopicOnLeader_FollowerHasTopic(t *testing.T) {
	leaderSrv, followerSrv := SetupTwoTestServers(t, "leader2", "follower2")
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	topicName := "my-topic"

	if err := leaderSrv.TopicManager.CreateTopic(topicName, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Leader has topic
	_, err := leaderSrv.TopicManager.GetTopic(topicName)
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
	leaderSrv, followerSrv := SetupTwoTestServers(t, "leader-del", "follower-del")
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	topicName := "del-topic"

	// Create topic on leader with 1 replica (follower gets replica-0)
	if err := leaderSrv.TopicManager.CreateTopic(topicName, 1); err != nil {
		t.Fatalf("CreateTopic on leader: %v", err)
	}

	// Verify replica and directory exist on follower
	replicaDir := filepath.Join(followerSrv.BaseDir, topicName, "replica-0")
	_, err := followerSrv.TopicManager.GetReplica(topicName, "replica-0")
	if err != nil {
		t.Fatalf("GetReplica before delete: %v", err)
	}
	if _, err := os.Stat(replicaDir); err != nil {
		t.Fatalf("replica dir %q should exist before delete: %v", replicaDir, err)
	}

	// Delete replica on follower via RPC
	ctx := context.Background()
	replClient := client.NewReplicationClient(followerSrv.Broker)
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
	leaderSrv, followerSrv := SetupTwoTestServers(t, "leader-del2", "follower-del2")
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	topicName := "del-topic2"

	if err := leaderSrv.TopicManager.CreateTopic(topicName, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	topicDir := filepath.Join(followerSrv.BaseDir, topicName)
	replicaDir := filepath.Join(topicDir, "replica-0")

	_, err := followerSrv.TopicManager.GetTopic(topicName)
	if err != nil {
		t.Fatalf("follower should have topic before delete: %v", err)
	}
	if _, err := os.Stat(topicDir); err != nil {
		t.Fatalf("topic dir %q should exist before delete: %v", topicDir, err)
	}

	ctx := context.Background()
	replClient := client.NewReplicationClient(followerSrv.Broker)
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

// TestReplication_ReadMessagesViaReplicationClient verifies that after the leader produces messages,
// we can read them back using the replication client (Replicate RPC) and verify content.
func TestReplication_ReadMessagesViaReplicationClient(t *testing.T) {
	leaderSrv, followerSrv := SetupTwoTestServers(t, "leader-repl", "follower-repl")
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	topicName := "repl-topic"
	if err := leaderSrv.TopicManager.CreateTopic(topicName, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ctx := context.Background()

	// Produce messages on leader
	producerClient, err := client.NewProducerClient(leaderSrv.Addr)
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()

	messages := []string{"msg-0", "msg-1", "msg-2"}
	for i, msg := range messages {
		resp, err := producerClient.Produce(ctx, &protocol.ProduceRequest{
			Topic: topicName,
			Value: []byte(msg),
			Acks:  protocol.AckLeader,
		})
		if err != nil {
			t.Fatalf("Produce %d: %v", i, err)
		}
		if resp.Offset != uint64(i) {
			t.Fatalf("expected offset %d, got %d", i, resp.Offset)
		}
	}

	// Read messages via replication client (use a dedicated broker so we don't share connection with follower's replica)
	leaderBrokerForClient := broker.NewBroker("test-leader", leaderSrv.Addr)
	replClient := client.NewReplicationClient(leaderBrokerForClient)
	defer leaderBrokerForClient.Close()
	stream, err := replClient.ReplicateStream(ctx, &protocol.ReplicateRequest{
		Topic:     topicName,
		Offset:    0,
		BatchSize: 10,
	})
	if err != nil {
		t.Fatalf("ReplicateStream: %v", err)
	}

	var allEntries []*protocol.LogEntry
	for {
		resp, err := stream.Recv()
		if err != nil {
			if len(allEntries) == 0 {
				t.Fatalf("Recv failed before any entries: %v", err)
			}
			break
		}
		if len(resp.Entries) == 0 {
			break
		}
		allEntries = append(allEntries, resp.Entries...)
	}

	if len(allEntries) != len(messages) {
		t.Fatalf("expected %d entries, got %d", len(messages), len(allEntries))
	}

	// Segment format: [offset 8 bytes][value]; Replicate returns raw ReadUncommitted bytes
	const offWidth = 8
	for i, entry := range allEntries {
		if entry.Offset != uint64(i) {
			t.Fatalf("entry %d: expected offset %d, got %d", i, i, entry.Offset)
		}
		payload := entry.Value
		if len(payload) >= offWidth {
			payload = payload[offWidth:]
		}
		if string(payload) != messages[i] {
			t.Fatalf("entry %d: expected value %q, got %q", i, messages[i], string(payload))
		}
	}
}

// TestReplication_FollowerHasMessagesAfterReplication verifies that after producing on the leader,
// the follower replica eventually has the same messages (replication runs in background).
func TestReplication_FollowerHasMessagesAfterReplication(t *testing.T) {
	leaderSrv, followerSrv := SetupTwoTestServers(t, "leader-repl2", "follower-repl2")
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	topicName := "repl-topic2"
	if err := leaderSrv.TopicManager.CreateTopic(topicName, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ctx := context.Background()
	producerClient, err := client.NewProducerClient(leaderSrv.Addr)
	if err != nil {
		t.Fatalf("NewProducerClient: %v", err)
	}
	defer producerClient.Close()

	messages := []string{"hello", "world"}
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

	// Replica appends entry.Value from leader; leader's Value is segment format [8-byte offset][payload].
	// Replica's segment then stores that as record value, so Read returns [8-byte seg prefix][leader value].
	const offWidth = 8
	for i, want := range messages {
		raw, err := replicaNode.Log.ReadUncommitted(uint64(i))
		if err != nil {
			t.Fatalf("ReadUncommitted(%d): %v", i, err)
		}
		if len(raw) < offWidth {
			t.Fatalf("entry %d: short read (len=%d)", i, len(raw))
		}
		payload := raw[offWidth:] // strip segment's 8-byte offset
		if len(payload) >= offWidth {
			payload = payload[offWidth:] // strip leader's 8-byte offset in value
		}
		if string(payload) != want {
			t.Fatalf("entry %d: got %q, want %q", i, string(payload), want)
		}
	}
}
