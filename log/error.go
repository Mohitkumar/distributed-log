package log

import (
	"fmt"
)

// ErrOffsetOutOfRange returns error when offset is out of range.
func ErrOffsetOutOfRange(offset uint64) error {
	return fmt.Errorf("offset %d out of range", offset)
}

// ErrOffsetBeyondHW returns error when offset is beyond high watermark.
func ErrOffsetBeyondHW(offset, hw uint64) error {
	return fmt.Errorf("offset %d is beyond high watermark %d (uncommitted data)", offset, hw)
}
