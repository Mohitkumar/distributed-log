package rpc

import (
	consumermgr "github.com/mohitkumar/mlog/consumer"
	"github.com/mohitkumar/mlog/node"
)

// rpcServer holds topic manager and consumer manager for TCP transport RPCs.
type rpcServer struct {
	topicManager    *node.TopicManager
	consumerManager *consumermgr.ConsumerManager
}

func NewServer(topicManager *node.TopicManager, consumerManager *consumermgr.ConsumerManager) *rpcServer {
	return &rpcServer{
		topicManager:    topicManager,
		consumerManager: consumerManager,
	}
}
