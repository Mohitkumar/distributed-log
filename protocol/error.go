package protocol

import (
	"errors"
	"fmt"
)

// Sentinel errors for protocol package (no dependency on errs to avoid import cycle).
var (
	ErrFrameTooLarge       = errors.New("protocol: frame exceeds max size")
	ErrReplicationBatchCRC = errors.New("protocol: replication batch CRC mismatch")
)

func ErrUnknownMessageType(mType MessageType) error {
	return fmt.Errorf("protocol: unknown message type: %d", mType)
}
