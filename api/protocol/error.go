package protocol

import (
	"errors"
	"fmt"
)

// Frame and message-type errors for the protocol layer.
var (
	ErrFrameTooLarge       = errors.New("protocol: frame exceeds max size")
	ErrReplicationBatchCRC = errors.New("protocol: replication batch CRC mismatch")
)

func ErrUnknownMessageType(mType MessageType) error {
	return fmt.Errorf("protocol: can not encode type: %d", mType)
}
