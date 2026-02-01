package tests

import (
	"testing"
	"time"

	"github.com/mohitkumar/mlog/rpc"
	"github.com/mohitkumar/mlog/topic"
)

type testServers struct {
	leader            *rpc.TestServer
	follower          *rpc.TestServer
	leaderTopicMgr    *topic.TopicManager
	followerTopicMgr  *topic.TopicManager
	leaderBaseDir     string
	followerBaseDir   string
	cleanup           func()
}

func (ts *testServers) getLeaderAddr() string {
	return ts.leader.Addr
}

func (ts *testServers) getFollowerAddr() string {
	return ts.follower.Addr
}

func setupTestServers(t *testing.T) *testServers {
	t.Helper()

	leader, follower := rpc.SetupTwoTestServers(t, "leader", "follower")
	time.Sleep(300 * time.Millisecond)

	return &testServers{
		leader:           leader,
		follower:         follower,
		leaderTopicMgr:   leader.TopicManager,
		followerTopicMgr: follower.TopicManager,
		leaderBaseDir:    leader.BaseDir,
		followerBaseDir:  follower.BaseDir,
		cleanup: func() {
			leader.Cleanup()
			follower.Cleanup()
		},
	}
}
