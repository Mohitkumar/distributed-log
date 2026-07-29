package raft

import (
	"errors"
	"fmt"
)

var (
	ErrRaftNoLeader         = errors.New("raft leader not found")
	ErrRaftNodeNotFound     = errors.New("node not found in metadata")
	ErrRaftTopicNotFound    = errors.New("topic not found in metadata")
	ErrRaftNoNodesInCluster = errors.New("no nodes in cluster")
	ErrCoordinatorStopped   = errors.New("coordinator stopped")
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
