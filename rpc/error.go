package rpc

import (
	"errors"

	"github.com/mohitkumar/mlog/protocol"
	"github.com/mohitkumar/mlog/topic"
)

// Err returns a *protocol.RPCError so the transport sends Code and Message to the client.
func Err(code int32, message string) error {
	return &protocol.RPCError{Code: code, Message: message}
}

// FromError maps known errors (from topic, coordinator, etc.) to a *protocol.RPCError with the appropriate code.
// Use for errors returned by topicManager or coordinator that should be propagated to the client with a code.
func FromError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, topic.ErrTopicNotFound):
		return &protocol.RPCError{Code: protocol.CodeTopicNotFound, Message: err.Error()}
	case errors.Is(err, topic.ErrTopicExists):
		return &protocol.RPCError{Code: protocol.CodeTopicExists, Message: err.Error()}
	case errors.Is(err, topic.ErrNotEnoughNodes):
		return &protocol.RPCError{Code: protocol.CodeNotEnoughNodes, Message: err.Error()}
	case errors.Is(err, topic.ErrCannotReachLeader):
		return &protocol.RPCError{Code: protocol.CodeCannotReachLeader, Message: err.Error()}
	case errors.Is(err, topic.ErrInvalidAckMode):
		return &protocol.RPCError{Code: protocol.CodeInvalidAckMode, Message: err.Error()}
	case errors.Is(err, topic.ErrTimeoutCatchUp):
		return &protocol.RPCError{Code: protocol.CodeTimeoutCatchUp, Message: err.Error()}
	default:
		return &protocol.RPCError{Code: protocol.CodeUnknown, Message: err.Error()}
	}
}
