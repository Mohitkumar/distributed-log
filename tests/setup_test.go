package tests

import (
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/broker"
	"github.com/mohitkumar/mlog/consumer"
	"github.com/mohitkumar/mlog/node"
	"github.com/mohitkumar/mlog/rpc"
	"github.com/mohitkumar/mlog/transport"
)

type testServers struct {
	leaderBroker     *broker.Broker
	followerBroker   *broker.Broker
	leaderTopicMgr   *node.TopicManager
	followerTopicMgr *node.TopicManager
	leaderBaseDir    string
	followerBaseDir  string
	leaderTr         *transport.Transport
	followerTr       *transport.Transport
	cleanup          func()
}

func (ts *testServers) getLeaderBroker() *broker.Broker {
	return ts.leaderBroker
}

func (ts *testServers) getFollowerBroker() *broker.Broker {
	return ts.followerBroker
}

func setupServer(t *testing.T, bm *broker.BrokerManager, baseDir string, nodeID string, serverName string) (*transport.Transport, *node.TopicManager, error) {
	t.Helper()

	b := bm.GetBroker(nodeID)
	if b == nil {
		return nil, nil, fmt.Errorf("broker %s not found", nodeID)
	}

	topicMgr, err := node.NewTopicManager(baseDir, bm, b)
	if err != nil {
		return nil, nil, fmt.Errorf("create topic manager: %w", err)
	}

	consumerMgr, err := consumer.NewConsumerManager(baseDir)
	if err != nil {
		return nil, nil, fmt.Errorf("create consumer manager: %w", err)
	}

	tr := transport.NewTransport()
	srv := rpc.NewServer(topicMgr, consumerMgr)
	srv.RegisterHandlers(tr)
	ln, err := tr.Listen("127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	go tr.Serve(ln)

	return tr, topicMgr, nil
}

func setupTestServers(t *testing.T) *testServers {
	t.Helper()

	bm := broker.NewBrokerManager()
	leaderBroker := broker.NewBroker("node-1", "127.0.0.1:0")
	followerBroker := broker.NewBroker("node-2", "127.0.0.1:0")
	bm.AddBroker(leaderBroker)
	bm.AddBroker(followerBroker)

	leaderBaseDir := path.Join(t.TempDir(), "leader")
	followerBaseDir := path.Join(t.TempDir(), "follower")

	leaderTr, leaderTopicMgr, err := setupServer(t, bm, leaderBaseDir, "node-1", "leader")
	if err != nil {
		t.Fatalf("setupLeaderServer error: %v", err)
	}
	leaderBroker.Addr = leaderTr.Addr()

	followerTr, followerTopicMgr, err := setupServer(t, bm, followerBaseDir, "node-2", "follower")
	if err != nil {
		_ = leaderTr.Close()
		t.Fatalf("setupFollowerServer error: %v", err)
	}
	followerBroker.Addr = followerTr.Addr()

	time.Sleep(300 * time.Millisecond)

	return &testServers{
		leaderBroker:     leaderBroker,
		followerBroker:   followerBroker,
		leaderTopicMgr:   leaderTopicMgr,
		followerTopicMgr: followerTopicMgr,
		leaderBaseDir:    leaderBaseDir,
		followerBaseDir:  followerBaseDir,
		leaderTr:         leaderTr,
		followerTr:       followerTr,
		cleanup: func() {
			_ = leaderBroker.Close()
			_ = followerBroker.Close()
			_ = leaderTr.Close()
			_ = followerTr.Close()
		},
	}
}
