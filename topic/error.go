package topic

import (
	"errors"
	"fmt"
)

// Sentinel errors for topic package. Use errors.Is to check.
var (
	ErrTopicExists        = errors.New("topic already exists")
	ErrTopicNotFound      = errors.New("topic not found")
	ErrNotEnoughNodes     = errors.New("not enough nodes")
	ErrNoNodesInCluster   = errors.New("no nodes in cluster")
	ErrCannotReachLeader  = errors.New("cannot reach Raft leader")
	ErrNoRPCForTopicLeader = errors.New("no RPC address for topic leader node")
	ErrNodeIDRequired     = errors.New("node id is required")
	ErrReplicaNotFound    = errors.New("replica not found for topic")
	ErrThisNodeNotLeader  = errors.New("this node is not leader")
	ErrTopicAlreadyReplica = errors.New("topic already has replica")
	ErrStreamClientNotSet = errors.New("stream client not set for topic")
	ErrReplicationStarted = errors.New("replication already started for topic")
	ErrInvalidAckMode     = errors.New("invalid ack mode")
	ErrValuesEmpty        = errors.New("values cannot be empty")
	ErrTimeoutCatchUp     = errors.New("timeout before all followers caught up")
)

// ErrTopicExistsf returns an error for duplicate topic name.
func ErrTopicExistsf(topic string) error { return fmt.Errorf("topic %s already exists", topic) }

// ErrCreateLog wraps log manager creation failure.
func ErrCreateLog(err error) error { return fmt.Errorf("failed to create log manager: %w", err) }

// ErrNotEnoughNodesf returns error when replica count exceeds available nodes.
func ErrNotEnoughNodesf(need, have int) error {
	return fmt.Errorf("not enough nodes: need %d, have %d", need, have)
}

// ErrCreateReplicationClient wraps client creation failure.
func ErrCreateReplicationClient(addr string, err error) error {
	return fmt.Errorf("failed to create replication client to %s: %w", addr, err)
}

// ErrCreateReplicaOnNode wraps CreateReplica RPC failure.
func ErrCreateReplicaOnNode(nodeID string, err error) error {
	return fmt.Errorf("failed to create replica on node %s: %w", nodeID, err)
}

// ErrCreateTopic wraps topic creation failure.
func ErrCreateTopic(err error) error { return fmt.Errorf("failed to create topic: %w", err) }

// ErrForwardToLeader wraps forward-to-leader failure.
func ErrForwardToLeader(err error) error { return fmt.Errorf("forward to leader: %w", err) }

// ErrNoRPCForTopicLeaderf returns error when topic leader has no RPC address.
func ErrNoRPCForTopicLeaderf(nodeID string) error {
	return fmt.Errorf("no RPC address for topic leader node %s", nodeID)
}

// ErrForwardToTopicLeader wraps forward-to-topic-leader failure.
func ErrForwardToTopicLeader(err error) error { return fmt.Errorf("forward to topic leader: %w", err) }

// ErrApplyCreateTopic wraps ApplyCreateTopicEvent failure.
func ErrApplyCreateTopic(err error) error { return fmt.Errorf("apply create topic event: %w", err) }

// ErrTopicNotFoundf returns error for missing topic.
func ErrTopicNotFoundf(topic string) error { return fmt.Errorf("topic %s not found", topic) }

// ErrDeleteTopic wraps delete failure.
func ErrDeleteTopic(err error) error { return fmt.Errorf("failed to delete topic: %w", err) }

// ErrApplyDeleteTopic wraps ApplyDeleteTopicEvent failure.
func ErrApplyDeleteTopic(err error) error { return fmt.Errorf("apply delete topic event: %w", err) }

// ErrThisNodeNotLeaderf returns error when this node is not the topic leader.
func ErrThisNodeNotLeaderf(topic string) error {
	return fmt.Errorf("topic %s: this node is not leader", topic)
}

// ErrTopicAlreadyReplicaf returns error when topic already has replica.
func ErrTopicAlreadyReplicaf(topic string) error {
	return fmt.Errorf("topic %s already has replica", topic)
}

// ErrCreateLogReplica wraps log manager creation for replica.
func ErrCreateLogReplica(err error) error {
	return fmt.Errorf("failed to create log manager for replica: %w", err)
}

// ErrCreateStreamClient wraps replication stream client creation.
func ErrCreateStreamClient(err error) error {
	return fmt.Errorf("failed to create replication stream client to leader: %w", err)
}

// ErrCreateRPCClient wraps replication RPC client creation.
func ErrCreateRPCClient(err error) error {
	return fmt.Errorf("failed to create replication RPC client to leader: %w", err)
}

// ErrStartReplication wraps StartReplication failure.
func ErrStartReplication(err error) error { return fmt.Errorf("failed to start replication: %w", err) }

// ErrRemoveTopicDir wraps remove directory failure.
func ErrRemoveTopicDir(err error) error {
	return fmt.Errorf("failed to remove topic directory: %w", err)
}

// ErrStreamClientNotSetf returns error when stream client is not set.
func ErrStreamClientNotSetf(topic string) error {
	return fmt.Errorf("stream client not set for topic %s", topic)
}

// ErrReplicationStartedf returns error when replication already started.
func ErrReplicationStartedf(topic string) error {
	return fmt.Errorf("replication already started for topic %s", topic)
}

// ErrWaitFollowersCatchUp wraps wait-for-followers failure.
func ErrWaitFollowersCatchUp(err error) error {
	return fmt.Errorf("failed to wait for all followers to catch up: %w", err)
}

// ErrInvalidAckModef returns error for invalid ack mode.
func ErrInvalidAckModef(mode int32) error { return fmt.Errorf("invalid ack mode: %d", mode) }

// ErrReplicaNotFoundf returns error when replica not found for topic.
func ErrReplicaNotFoundf(nodeID, topic string) error {
	return fmt.Errorf("replica %s not found for topic %s", nodeID, topic)
}
