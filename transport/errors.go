package transport

import "errors"

var (
	ErrFrameTooLarge = errors.New("transport: frame exceeds max size")
	ErrClosed        = errors.New("transport: connection closed")
)
