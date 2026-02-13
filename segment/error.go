package segment

import "github.com/mohitkumar/mlog/errs"

// Re-export segment errors from errs so code that uses segment.Err* or errors.Is(err, segment.ErrX) still works.
var (
	ErrOffsetNotFound = errs.ErrSegmentOffsetNotFound
	ErrIndexNotFound  = errs.ErrSegmentIndexNotFound
)

func ErrOffsetOutOfRange(offset, base, next uint64) error {
	return errs.ErrSegmentOffsetOutOfRange(offset, base, next)
}
func ErrOffsetOutOfRangeSimple(offset uint64) error {
	return errs.ErrSegmentOffsetOutOfRangeSimple(offset)
}
func ErrSeekFailed(err error) error       { return errs.ErrSeekFailed(err) }
func ErrTruncateFailed(err error) error   { return errs.ErrTruncateFailed(err) }
func ErrIndexSyncFailed(err error) error  { return errs.ErrIndexSyncFailed(err) }
