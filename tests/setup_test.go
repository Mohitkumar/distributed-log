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
	leaderLn         *transport.Listener
	followerLn       *transport.Listener
	cleanup          func()
}

func (ts *testServers) getLeaderConn() (*transport.Conn, error) {
	return ts.leaderBroker.GetConn()
}

func (ts *testServers) getFollowerConn() (*transport.Conn, error) {
	return ts.followerBroker.GetConn()
}

func setupServer(t *testing.T, bm *broker.BrokerManager, baseDir string, nodeID string, serverName string, tr *transport.Transport) (*transport.Listener, *node.TopicManager, error) {
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

	ln, err := tr.Listen("127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}

	srv := rpc.NewServer(topicMgr, consumerMgr)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.ServeTransportConn(conn)
		}
	}()

	return ln, topicMgr, nil
}

func setupTestServers(t *testing.T) *testServers {
	t.Helper()

	tr := transport.NewTransport()
	bm := broker.NewBrokerManager()
	leaderBroker := broker.NewBroker("node-1", "127.0.0.1:0")
	followerBroker := broker.NewBroker("node-2", "127.0.0.1:0")
	bm.AddBroker(leaderBroker)
	bm.AddBroker(followerBroker)

	leaderBaseDir := path.Join(t.TempDir(), "leader")
	followerBaseDir := path.Join(t.TempDir(), "follower")

	leaderLn, leaderTopicMgr, err := setupServer(t, bm, leaderBaseDir, "node-1", "leader", tr)
	if err != nil {
		t.Fatalf("setupLeaderServer error: %v", err)
	}
	leaderBroker.Addr = leaderLn.Addr().String()

	followerLn, followerTopicMgr, err := setupServer(t, bm, followerBaseDir, "node-2", "follower", tr)
	if err != nil {
		_ = leaderLn.Close()
		t.Fatalf("setupFollowerServer error: %v", err)
	}
	followerBroker.Addr = followerLn.Addr().String()

	time.Sleep(300 * time.Millisecond)

	return &testServers{
		leaderBroker:     leaderBroker,
		followerBroker:   followerBroker,
		leaderTopicMgr:   leaderTopicMgr,
		followerTopicMgr: followerTopicMgr,
		leaderBaseDir:    leaderBaseDir,
		followerBaseDir:  followerBaseDir,
		leaderLn:         leaderLn,
		followerLn:       followerLn,
		cleanup: func() {
			_ = leaderBroker.Close()
			_ = followerBroker.Close()
			_ = leaderLn.Close()
			_ = followerLn.Close()
		},
	}
}
