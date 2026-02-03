package node

import (
	"fmt"
)

// ErrRaftApply wraps Raft Apply failure.
func ErrRaftApply(err error) error { return fmt.Errorf("raft apply: %w", err) }
