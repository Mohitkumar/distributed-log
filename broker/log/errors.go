package log

import (
	"errors"
	"fmt"
)

var (
	ErrLogOffsetOutOfRange = errors.New("log: offset out of range")
	ErrLogOffsetBeyondHW   = errors.New("log: offset beyond high watermark")
)

func ErrLogOffsetOutOfRangef(offset uint64) error {
	return fmt.Errorf("offset %d out of range: %w", offset, ErrLogOffsetOutOfRange)
}

func ErrLogOffsetBeyondHWf(offset, hw uint64) error {
	return fmt.Errorf("offset %d is beyond high watermark %d (uncommitted data): %w", offset, hw, ErrLogOffsetBeyondHW)
}
