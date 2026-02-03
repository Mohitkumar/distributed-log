package protocol

import (
	"errors"
	"fmt"
)

// Sentinel errors for protocol package.
var (
	ErrFrameTooLarge         = errors.New("protocol: frame exceeds max size")
	ErrReplicationBatchCRC   = errors.New("protocol: replication batch CRC mismatch")
)

// ErrUnknownMessageType returns error for unknown message type.
func ErrUnknownMessageType(mType MessageType) error {
	return fmt.Errorf("protocol: unknown message type: %d", mType)
}
