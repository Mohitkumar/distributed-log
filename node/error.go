package node

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
