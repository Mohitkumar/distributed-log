package rpc

import (
	"errors"

	"github.com/mohitkumar/mlog/errs"
	"github.com/mohitkumar/mlog/protocol"
)

// Err returns an RPCError with the given code and message; the transport sends it to the client.
func Err(code int32, message string) error {
	return &protocol.RPCError{Code: code, Message: message}
}

// CodeFor returns the protocol RPC code for the given error.
func CodeFor(err error) int32 {
	if err == nil {
		return 0
	}
	switch {
	case errors.Is(err, errs.ErrTopicNotFound):
		return protocol.CodeTopicNotFound
	case errors.Is(err, errs.ErrTopicExists):
		return protocol.CodeTopicExists
	case errors.Is(err, errs.ErrNotEnoughNodes):
		return protocol.CodeNotEnoughNodes
	case errors.Is(err, errs.ErrCannotReachLeader):
		return protocol.CodeCannotReachLeader
	case errors.Is(err, errs.ErrThisNodeNotLeader):
		return protocol.CodeNotTopicLeader
	case errors.Is(err, errs.ErrInvalidAckMode):
		return protocol.CodeInvalidAckMode
	case errors.Is(err, errs.ErrTimeoutCatchUp):
		return protocol.CodeTimeoutCatchUp
	case errors.Is(err, errs.ErrValuesEmpty):
		return protocol.CodeValuesRequired
	case errors.Is(err, errs.ErrLogOffsetOutOfRange), errors.Is(err, errs.ErrSegmentOffsetNotFound):
		return protocol.CodeReadOffset
	case errors.Is(err, errs.ErrRaftNoLeader), errors.Is(err, errs.ErrRaftNodeNotFound):
		return protocol.CodeRaftLeaderUnavailable
	default:
		return protocol.CodeUnknown
	}
}

// FromError converts an error to an RPCError with the appropriate code for the client.
func FromError(err error) error {
	if err == nil {
		return nil
	}
	return &protocol.RPCError{Code: CodeFor(err), Message: err.Error()}
}
