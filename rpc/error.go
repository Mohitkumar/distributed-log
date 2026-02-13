package rpc

import (
	"errors"

	"github.com/mohitkumar/mlog/errs"
	"github.com/mohitkumar/mlog/protocol"
)

// Err returns a *protocol.RPCError so the transport sends Code and Message to the client.
func Err(code int32, message string) error {
	return &protocol.RPCError{Code: code, Message: message}
}

// CodeFor returns the protocol RPC code for the given error. Use when mapping errors to RPC responses.
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

// Retriable returns true if the error is transient and the caller can retry (e.g. after re-resolving leader).
func Retriable(err error) bool {
	if err == nil {
		return false
	}
	switch CodeFor(err) {
	case protocol.CodeNotTopicLeader, protocol.CodeTopicNotFound, protocol.CodeRaftLeaderUnavailable, protocol.CodeCannotReachLeader:
		return true
	default:
		return false
	}
}

// FromError maps known errors to a *protocol.RPCError with the appropriate code.
// Use for errors returned by topicManager or coordinator that should be propagated to the client with a code.
func FromError(err error) error {
	if err == nil {
		return nil
	}
	return &protocol.RPCError{Code: CodeFor(err), Message: err.Error()}
}
