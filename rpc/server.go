package rpc

import (
	consumermgr "github.com/mohitkumar/mlog/consumer"
	"github.com/mohitkumar/mlog/node"
)

// grpcServer holds topic manager and consumer manager for TCP transport RPCs.
// Name kept for compatibility; no gRPC is used.
type grpcServer struct {
	topicManager    *node.TopicManager
	consumerManager *consumermgr.ConsumerManager
}

// NewServer returns a server that handles TCP transport RPCs (replication, leader, etc.).
func NewServer(topicManager *node.TopicManager, consumerManager *consumermgr.ConsumerManager) *grpcServer {
	return &grpcServer{
		topicManager:    topicManager,
		consumerManager: consumerManager,
	}
}
