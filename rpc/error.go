package rpc

import (
	"errors"
	"fmt"
)

// Sentinel errors for rpc package. Use errors.Is to check.
var (
	ErrTopicNameRequired   = errors.New("topic name is required")
	ErrReplicaCountInvalid = errors.New("replica count cannot be negative")
	ErrTopicRequired       = errors.New("topic is required")
	ErrLeaderAddrRequired  = errors.New("leader_addr is required")
	ErrValuesRequired      = errors.New("values are required")
	ErrNotTopicLeader      = errors.New("this node is not the topic leader; produce to the topic leader")
)

// ErrTopicNotFound wraps a topic-manager error for topic not found.
func ErrTopicNotFound(topic string, err error) error {
	return fmt.Errorf("topic %s not found: %w", topic, err)
}

// ErrReadOffset wraps read offset failure.
func ErrReadOffset(off uint64, err error) error {
	return fmt.Errorf("failed to read offset %d: %w", off, err)
}

// ErrCommitOffset wraps commit offset failure.
func ErrCommitOffset(err error) error { return fmt.Errorf("commit offset failed: %w", err) }

// ErrRecoverOffsets wraps recover offsets failure.
func ErrRecoverOffsets(err error) error { return fmt.Errorf("recover offsets failed: %w", err) }
