package segment

import (
	"errors"
	"fmt"
)

var (
	ErrSegmentOffsetNotFound = errors.New("offset not found")
	ErrSegmentIndexNotFound  = errors.New("index not found")
)

func ErrSegmentOffsetOutOfRange(offset, base, next uint64) error {
	return fmt.Errorf("offset %d out of range [%d, %d): %w", offset, base, next, ErrSegmentOffsetNotFound)
}

func ErrSegmentOffsetOutOfRangeSimple(offset uint64) error {
	return fmt.Errorf("offset %d out of range: %w", offset, ErrSegmentOffsetNotFound)
}

func ErrSeekFailed(err error) error      { return fmt.Errorf("failed to seek: %w", err) }
func ErrTruncateFailed(err error) error  { return fmt.Errorf("truncate failed: %w", err) }
func ErrIndexSyncFailed(err error) error { return fmt.Errorf("index sync failed: %w", err) }
