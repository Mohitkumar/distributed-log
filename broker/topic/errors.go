package topic

import (
	"errors"
	"fmt"
)

var (
	ErrTopicExists         = errors.New("topic already exists")
	ErrTopicNotFound       = errors.New("topic not found")
	ErrCannotReachLeader   = errors.New("cannot reach Raft leader")
	ErrNoRPCForTopicLeader = errors.New("no RPC address for topic leader node")
	ErrNodeIDRequired      = errors.New("node id is required")
	ErrReplicaNotFound     = errors.New("replica not found for topic")
	ErrThisNodeNotLeader   = errors.New("this node is not leader")
	ErrTopicAlreadyReplica = errors.New("topic already has replica")
	ErrStreamClientNotSet  = errors.New("stream client not set for topic")
	ErrReplicationStarted  = errors.New("replication already started for topic")
	ErrInvalidAckMode      = errors.New("invalid ack mode")
	ErrValuesEmpty         = errors.New("values cannot be empty")
	ErrTimeoutCatchUp      = errors.New("timeout before all followers caught up")
)

func ErrTopicExistsf(topic string) error {
	return fmt.Errorf("topic %s already exists: %w", topic, ErrTopicExists)
}

func ErrTopicNotFoundf(topic string) error {
	return fmt.Errorf("topic %s not found: %w", topic, ErrTopicNotFound)
}

func ErrNoRPCForTopicLeaderf(nodeID string) error {
	return fmt.Errorf("no RPC address for topic leader node %s: %w", nodeID, ErrNoRPCForTopicLeader)
}

func ErrThisNodeNotLeaderf(topic string) error {
	return fmt.Errorf("topic %s: this node is not leader: %w", topic, ErrThisNodeNotLeader)
}

func ErrTopicAlreadyReplicaf(topic string) error {
	return fmt.Errorf("topic %s already has replica: %w", topic, ErrTopicAlreadyReplica)
}

func ErrStreamClientNotSetf(topic string) error {
	return fmt.Errorf("stream client not set for topic %s: %w", topic, ErrStreamClientNotSet)
}

func ErrReplicationStartedf(topic string) error {
	return fmt.Errorf("replication already started for topic %s: %w", topic, ErrReplicationStarted)
}

func ErrInvalidAckModef(mode int32) error {
	return fmt.Errorf("invalid ack mode: %d: %w", mode, ErrInvalidAckMode)
}

func ErrReplicaNotFoundf(nodeID, topic string) error {
	return fmt.Errorf("replica %s not found for topic %s: %w", nodeID, topic, ErrReplicaNotFound)
}

func ErrCreateLog(err error) error { return fmt.Errorf("failed to create log manager: %w", err) }
func ErrCreateLogReplica(err error) error {
	return fmt.Errorf("failed to create log manager for replica: %w", err)
}
func ErrCreateReplicationClient(addr string, err error) error {
	return fmt.Errorf("failed to create replication client to %s: %w", addr, err)
}
func ErrGetRpcClient(nodeID string, err error) error {
	return fmt.Errorf("failed to get RPC client for node %s: %w", nodeID, err)
}
func ErrCreateReplicaOnNode(nodeID string, err error) error {
	return fmt.Errorf("failed to create replica on node %s: %w", nodeID, err)
}
func ErrCreateTopic(err error) error          { return fmt.Errorf("failed to create topic: %w", err) }
func ErrForwardToLeader(err error) error      { return fmt.Errorf("forward to leader: %w", err) }
func ErrForwardToTopicLeader(err error) error { return fmt.Errorf("forward to topic leader: %w", err) }
func ErrApplyCreateTopic(err error) error     { return fmt.Errorf("apply create topic event: %w", err) }
func ErrDeleteTopic(err error) error          { return fmt.Errorf("failed to delete topic: %w", err) }
func ErrApplyDeleteTopic(err error) error     { return fmt.Errorf("apply delete topic event: %w", err) }
func ErrCreateStreamClient(err error) error {
	return fmt.Errorf("failed to create replication stream client to leader: %w", err)
}
func ErrCreateRPCClient(err error) error {
	return fmt.Errorf("failed to create replication RPC client to leader: %w", err)
}
func ErrStartReplication(err error) error { return fmt.Errorf("failed to start replication: %w", err) }
func ErrRemoveTopicDir(err error) error {
	return fmt.Errorf("failed to remove topic directory: %w", err)
}
func ErrWaitFollowersCatchUp(err error) error {
	return fmt.Errorf("failed to wait for all followers to catch up: %w", err)
}
