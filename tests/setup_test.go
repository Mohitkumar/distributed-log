package tests

import (
	"testing"
	"time"

	"github.com/mohitkumar/mlog/topic"
)

type testServers struct {
	leader           *TestServer
	follower         *TestServer
	leaderTopicMgr   *topic.TopicManager
	followerTopicMgr *topic.TopicManager
	leaderBaseDir    string
	followerBaseDir  string
	cleanup          func()
}

func (ts *testServers) getLeaderAddr() string {
	return ts.leader.Addr
}

func (ts *testServers) getFollowerAddr() string {
	return ts.follower.Addr
}

func setupTestServers(tb testing.TB) *testServers {
	tb.Helper()

	leader, follower := SetupTwoTestServers(tb, "leader", "follower")
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
