package rpc

import (
	"errors"

	"github.com/mohitkumar/mlog/api/protocol"
	"github.com/mohitkumar/mlog/broker/cluster"
	raft "github.com/mohitkumar/mlog/broker/cluster/raft"
	"github.com/mohitkumar/mlog/broker/log"
	"github.com/mohitkumar/mlog/broker/segment"
	"github.com/mohitkumar/mlog/broker/topic"
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
	case errors.Is(err, topic.ErrTopicNotFound):
		return protocol.CodeTopicNotFound
	case errors.Is(err, topic.ErrTopicExists):
		return protocol.CodeTopicExists
	case errors.Is(err, cluster.ErrNotEnoughNodes):
		return protocol.CodeNotEnoughNodes
	case errors.Is(err, topic.ErrCannotReachLeader):
		return protocol.CodeCannotReachLeader
	case errors.Is(err, topic.ErrThisNodeNotLeader):
		return protocol.CodeNotTopicLeader
	case errors.Is(err, topic.ErrInvalidAckMode):
		return protocol.CodeInvalidAckMode
	case errors.Is(err, topic.ErrTimeoutCatchUp):
		return protocol.CodeTimeoutCatchUp
	case errors.Is(err, topic.ErrValuesEmpty):
		return protocol.CodeValuesRequired
	case errors.Is(err, log.ErrLogOffsetOutOfRange), errors.Is(err, segment.ErrSegmentOffsetNotFound):
		return protocol.CodeReadOffset
	case errors.Is(err, raft.ErrRaftNoLeader), errors.Is(err, raft.ErrRaftNodeNotFound):
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
