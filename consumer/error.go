package consumer

import "github.com/mohitkumar/mlog/errs"

func ErrOffsetNotFoundForID(id, topic string) error { return errs.ErrOffsetNotFoundForID(id, topic) }
