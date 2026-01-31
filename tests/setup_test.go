package tests

import (
	"testing"
	"time"

	"github.com/mohitkumar/mlog/broker"
	"github.com/mohitkumar/mlog/node"
	"github.com/mohitkumar/mlog/rpc"
)

type testServers struct {
	leaderBroker     *broker.Broker
	followerBroker   *broker.Broker
	leaderTopicMgr   *node.TopicManager
	followerTopicMgr *node.TopicManager
	leaderBaseDir    string
	followerBaseDir  string
	cleanup          func()
}

func (ts *testServers) getLeaderBroker() *broker.Broker {
	return ts.leaderBroker
}

func (ts *testServers) getFollowerBroker() *broker.Broker {
	return ts.followerBroker
}

func setupTestServers(t *testing.T) *testServers {
	t.Helper()

	leader, follower := rpc.SetupTwoTestServers(t, "leader", "follower")
	time.Sleep(300 * time.Millisecond)

	return &testServers{
		leaderBroker:     leader.Broker,
		followerBroker:   follower.Broker,
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
