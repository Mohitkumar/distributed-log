// Package errs provides shared errors for the distributed log, grouped by layer (segment, log, topic, raft, protocol, consumer).
// Check errors with errors.Is(err, errs.ErrX). RPC code mapping lives in rpc.CodeFor.
package errs

import (
	"errors"
	"fmt"
)

// Segment errors (offset/index not found, seek/truncate/index sync failures).

var (
	ErrSegmentOffsetNotFound = errors.New("offset not found")
	ErrSegmentIndexNotFound  = errors.New("index not found")
)

func ErrSegmentOffsetOutOfRange(offset, base, next uint64) error {
	return fmt.Errorf("offset %d out of range [%d, %d): %w", offset, base, next, ErrSegmentOffsetNotFound)
}

func ErrSegmentOffsetOutOfRangeSimple(offset uint64) error {
	return fmt.Errorf("offset %d out of range: %w", offset, ErrSegmentOffsetNotFound)
}

func ErrSeekFailed(err error) error  { return fmt.Errorf("failed to seek: %w", err) }
func ErrTruncateFailed(err error) error { return fmt.Errorf("truncate failed: %w", err) }
func ErrIndexSyncFailed(err error) error { return fmt.Errorf("index sync failed: %w", err) }

// Log errors (offset out of range or beyond high watermark).

var (
	ErrLogOffsetOutOfRange = errors.New("log: offset out of range")
	ErrLogOffsetBeyondHW  = errors.New("log: offset beyond high watermark")
)

func ErrLogOffsetOutOfRangef(offset uint64) error {
	return fmt.Errorf("offset %d out of range: %w", offset, ErrLogOffsetOutOfRange)
}

func ErrLogOffsetBeyondHWf(offset, hw uint64) error {
	return fmt.Errorf("offset %d is beyond high watermark %d (uncommitted data): %w", offset, hw, ErrLogOffsetBeyondHW)
}

// Topic manager errors (topic exists/not found, leader/replica, ack mode, etc.).

var (
	ErrTopicExists          = errors.New("topic already exists")
	ErrTopicNotFound        = errors.New("topic not found")
	ErrNotEnoughNodes       = errors.New("not enough nodes")
	ErrNoNodesInCluster     = errors.New("no nodes in cluster")
	ErrCannotReachLeader    = errors.New("cannot reach Raft leader")
	ErrNoRPCForTopicLeader  = errors.New("no RPC address for topic leader node")
	ErrNodeIDRequired       = errors.New("node id is required")
	ErrReplicaNotFound      = errors.New("replica not found for topic")
	ErrThisNodeNotLeader    = errors.New("this node is not leader")
	ErrTopicAlreadyReplica  = errors.New("topic already has replica")
	ErrStreamClientNotSet   = errors.New("stream client not set for topic")
	ErrReplicationStarted   = errors.New("replication already started for topic")
	ErrInvalidAckMode       = errors.New("invalid ack mode")
	ErrValuesEmpty          = errors.New("values cannot be empty")
	ErrTimeoutCatchUp       = errors.New("timeout before all followers caught up")
)

func ErrTopicExistsf(topic string) error {
	return fmt.Errorf("topic %s already exists: %w", topic, ErrTopicExists)
}

func ErrTopicNotFoundf(topic string) error {
	return fmt.Errorf("topic %s not found: %w", topic, ErrTopicNotFound)
}

func ErrNotEnoughNodesf(need, have int) error {
	return fmt.Errorf("not enough nodes: need %d, have %d: %w", need, have, ErrNotEnoughNodes)
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

func ErrCreateLog(err error) error       { return fmt.Errorf("failed to create log manager: %w", err) }
func ErrCreateLogReplica(err error) error { return fmt.Errorf("failed to create log manager for replica: %w", err) }
func ErrCreateReplicationClient(addr string, err error) error {
	return fmt.Errorf("failed to create replication client to %s: %w", addr, err)
}
func ErrGetRpcClient(nodeID string, err error) error {
	return fmt.Errorf("failed to get RPC client for node %s: %w", nodeID, err)
}
func ErrCreateReplicaOnNode(nodeID string, err error) error {
	return fmt.Errorf("failed to create replica on node %s: %w", nodeID, err)
}
func ErrCreateTopic(err error) error { return fmt.Errorf("failed to create topic: %w", err) }
func ErrForwardToLeader(err error) error { return fmt.Errorf("forward to leader: %w", err) }
func ErrForwardToTopicLeader(err error) error { return fmt.Errorf("forward to topic leader: %w", err) }
func ErrApplyCreateTopic(err error) error { return fmt.Errorf("apply create topic event: %w", err) }
func ErrDeleteTopic(err error) error { return fmt.Errorf("failed to delete topic: %w", err) }
func ErrApplyDeleteTopic(err error) error { return fmt.Errorf("apply delete topic event: %w", err) }
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

// Raft / Coordinator errors (no leader, node/topic not found, apply/bootstrap failures).

var (
	ErrRaftNoLeader       = errors.New("raft leader not found")
	ErrRaftNodeNotFound   = errors.New("node not found in metadata")
	ErrRaftTopicNotFound  = errors.New("topic not found in metadata")
	ErrRaftNoNodesInCluster = errors.New("no nodes in cluster")
	ErrCoordinatorStopped = errors.New("coordinator stopped")
)

func ErrRaftApply(err error) error { return fmt.Errorf("raft apply: %w", err) }

func ErrInvalidEvent(ev interface{}) error {
	return fmt.Errorf("invalid event: %v", ev)
}

func ErrRaftLogIndex(index uint64) error {
	return fmt.Errorf("raft log index must be >= 1, got %d", index)
}

func ErrLogRecordTooShort(offset uint64) error {
	return fmt.Errorf("log record too short at offset %d", offset)
}

func ErrNewRaft(err error) error { return fmt.Errorf("failed to create new raft: %w", err) }

func ErrBootstrapCluster(err error) error { return fmt.Errorf("failed to bootstrap cluster: %w", err) }

// Protocol / transport errors (frame size, CRC, unknown message type).

var (
	ErrFrameTooLarge        = errors.New("protocol: frame exceeds max size")
	ErrReplicationBatchCRC  = errors.New("protocol: replication batch CRC mismatch")
)

func ErrUnknownMessageType(mType int) error {
	return fmt.Errorf("protocol: unknown message type: %d", mType)
}

// Consumer errors (offset not found for consumer id/topic).

var ErrConsumerOffsetNotFound = errors.New("consumer: offset not found for id/topic")

func ErrOffsetNotFoundForID(id, topic string) error {
	return fmt.Errorf("offset not found for id: %s and topic: %s: %w", id, topic, ErrConsumerOffsetNotFound)
}
