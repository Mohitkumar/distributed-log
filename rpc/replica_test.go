package rpc

import (
	"context"
	"os"
	"path/filepath"
	"testing"

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
