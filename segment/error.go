package segment

import (
	"errors"
	"fmt"
)

// Sentinel errors for segment package.
var (
	ErrOffsetNotFound = errors.New("offset not found")
	ErrIndexNotFound  = errors.New("index not found")
)

// ErrOffsetOutOfRange returns error when offset is out of segment range.
func ErrOffsetOutOfRange(offset, base, next uint64) error {
	return fmt.Errorf("offset %d out of range [%d, %d)", offset, base, next)
}

// ErrOffsetOutOfRangeSimple returns error for offset out of range (single offset).
func ErrOffsetOutOfRangeSimple(offset uint64) error {
	return fmt.Errorf("offset %d out of range", offset)
}

// ErrSeekFailed wraps seek failure.
func ErrSeekFailed(err error) error { return fmt.Errorf("failed to seek: %w", err) }

// ErrTruncateFailed wraps truncate failure.
func ErrTruncateFailed(err error) error { return fmt.Errorf("truncate failed: %w", err) }

// ErrIndexSyncFailed wraps index sync failure.
func ErrIndexSyncFailed(err error) error { return fmt.Errorf("index sync failed: %w", err) }
