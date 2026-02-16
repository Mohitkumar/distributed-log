package consumer

import "github.com/mohitkumar/mlog/errs"

// ErrOffsetNotFoundForID returns an error when no stored offset exists for the given consumer id and topic.
func ErrOffsetNotFoundForID(id, topic string) error { return errs.ErrOffsetNotFoundForID(id, topic) }
