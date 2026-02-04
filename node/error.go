package node

import (
	"errors"
	"fmt"
)

var (
	ErrRaftNoLeader  = errors.New("raft leader not found")
	ErrNodeNotFound  = errors.New("node not found in metadata")
	ErrTopicNotFound = errors.New("topic not found in metadata")
)

// ErrRaftApply wraps Raft Apply failure.
func ErrRaftApply(err error) error { return fmt.Errorf("raft apply: %w", err) }
