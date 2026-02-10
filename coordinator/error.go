package coordinator

import (
	"errors"
	"fmt"

	"github.com/mohitkumar/mlog/protocol"
)

var (
	ErrRaftNoLeader     = errors.New("raft leader not found")
	ErrNodeNotFound     = errors.New("node not found in metadata")
	ErrTopicNotFound    = errors.New("topic not found in metadata")
	ErrNoNodesInCluster = errors.New("no nodes in cluster")
)

// ErrRaftApply wraps Raft Apply failure.
func ErrRaftApply(err error) error { return fmt.Errorf("raft apply: %w", err) }

func ErrInvalidEvent(ev *protocol.MetadataEvent) error { return fmt.Errorf("invalid event: %v", ev) }

// ErrRaftLogIndex returns error when Raft log index is invalid.
func ErrRaftLogIndex(index uint64) error {
	return fmt.Errorf("raft log index must be >= 1, got %d", index)
}

// ErrLogRecordTooShort returns error when log record is too short.
func ErrLogRecordTooShort(offset uint64) error {
	return fmt.Errorf("log record too short at offset %d", offset)
}

// ErrNewRaft wraps failed to create Raft.
func ErrNewRaft(err error) error { return fmt.Errorf("failed to create new raft: %s", err) }

// ErrBootstrapCluster wraps failed to bootstrap cluster.
func ErrBootstrapCluster(err error) error { return fmt.Errorf("failed to bootstrap cluster: %s", err) }
