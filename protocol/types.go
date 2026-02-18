package protocol

import (
	"errors"
	"io"
	"net"
	"syscall"
)

// LogEntry is a log record with offset and value.
type LogEntry struct {
	Offset uint64
	Value  []byte
}

// AckMode for producer acks.
type AckMode int32

const (
	AckNone   AckMode = 0
	AckLeader AckMode = 1
	AckAll    AckMode = 2
)

// Create/delete topic request and response types.
type CreateTopicRequest struct {
	Topic                  string
	ReplicaCount           uint32
	DesignatedLeaderNodeID string // when set, only this node should create the topic (used when Raft leader forwards to topic leader)
}
type CreateTopicResponse struct {
	Topic          string
	ReplicaNodeIds []string // actual replica set (topic leader + replicas) filled by topic leader
}
type DeleteTopicRequest struct {
	Topic string
}
type DeleteTopicResponse struct {
	Topic string
}

// ListTopics: any node can answer (metadata is replicated via Raft).
type ListTopicsRequest struct{}

type ReplicaInfo struct {
	NodeID string
	IsISR  bool
	LEO    int64
}

type TopicInfo struct {
	Name          string
	LeaderNodeID  string
	LeaderEpoch   int64
	Replicas      []ReplicaInfo
}

type ListTopicsResponse struct {
	Topics []TopicInfo
}

// RPC error codes. Clients can switch on Code to handle specific errors (e.g. retry on NOT_TOPIC_LEADER).
const (
	CodeUnknown int32 = iota
	CodeTopicRequired
	CodeTopicNameRequired
	CodeTopicNotFound
	CodeNotTopicLeader
	CodeValuesRequired
	CodeReadOffset
	CodeCommitOffset
	CodeRecoverOffsets
	CodeReplicaCountInvalid
	CodeLeaderAddrRequired
	CodeRaftLeaderUnavailable
	CodeTopicExists
	CodeNotEnoughNodes
	CodeCannotReachLeader
	CodeInvalidAckMode
	CodeTimeoutCatchUp
)

// RPCErrorResponse is sent by the server when a handler returns an error.
// Code allows the client to handle specific errors (e.g. find new leader on CodeNotTopicLeader).
type RPCErrorResponse struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
}

// RPCError is returned by the transport client when the server sends an RPCErrorResponse.
// Clients can check the code to decide action, e.g. find new leader on CodeNotTopicLeader:
//
//	var rpcErr *protocol.RPCError
//	if errors.As(err, &rpcErr) {
//	    switch rpcErr.Code {
//	    case protocol.CodeNotTopicLeader, protocol.CodeTopicNotFound:
//	        // re-resolve leader and retry
//	    case protocol.CodeReadOffset:
//	        // no more data (e.g. replica caught up)
//	    }
//	}
type RPCError struct {
	Code    int32
	Message string
}

func (e *RPCError) Error() string { return e.Message }

// ShouldReconnect returns true if the error indicates the client should invalidate
// the cached connection and reconnect (e.g. leader changed, topic not found, or network failure).
// Use after RPC failures to decide whether to call InvalidateConsumerClient/InvalidateRpcClient
// and retry on the next attempt.
func ShouldReconnect(err error) bool {
	if err == nil {
		return false
	}
	// Server returned a structured RPC error code: reconnect on leader/topic/raft changes.
	var rpcErr *RPCError
	if errors.As(err, &rpcErr) {
		switch rpcErr.Code {
		case CodeNotTopicLeader, CodeTopicNotFound, CodeRaftLeaderUnavailable:
			return true
		default:
			return false
		}
	}
	// EOF means the connection was closed by the peer.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	// Syscall-level connection errors: reset, broken pipe, refused.
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	// net.Error covers timeouts and other transient network failures.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	// net.ErrClosed: use of closed network connection.
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	return false
}

type ReplicateRequest struct {
	Topic         string
	Offset        uint64 // replica LEO; leader streams from this offset
	BatchSize     uint32
	ReplicaNodeID string // leader caches reader per (topic, replica)
}
type ReplicateResponse struct {
	Topic       string
	RawChunk    []byte // raw segment-format records: [Offset 8][Len 4][Value]...
	EndOfStream bool   // when true, leader has sent all data for this round; replica reports LEO via Raft event
	LeaderLEO   int64  // when EndOfStream, leader's LEO so replica can compute ISR and send IsrUpdateEvent
}

// Producer request/response types.
type ProduceRequest struct {
	Topic string
	Value []byte
	Acks  AckMode
}
type ProduceResponse struct {
	Offset uint64
}
type ProduceBatchRequest struct {
	Topic  string
	Values [][]byte
	Acks   AckMode
}
type ProduceBatchResponse struct {
	BaseOffset uint64
	LastOffset uint64
	Count      uint32
}

// FindLeader: clients discover the topic leader RPC address; any node can answer using local metadata.
type FindTopicLeaderRequest struct {
	Topic string
}

type FindTopicLeaderResponse struct {
	LeaderAddr string
}

// ApplyIsrUpdateEventRequest is sent to the Raft leader to apply an ISR update for a replica.
type ApplyIsrUpdateEventRequest struct {
	Topic         string
	ReplicaNodeID string
	Isr           bool
}
type ApplyIsrUpdateEventResponse struct{}

// GetRaftLeader: clients discover the Raft (metadata) leader; create-topic and metadata ops go to this address.
type FindRaftLeaderRequest struct{}

type FindRaftLeaderResponse struct {
	RaftLeaderAddr string
}

// Consumer request/response types.
type FetchRequest struct {
	Topic         string
	Id            string
	Offset        uint64
	ReplicaNodeID string // when set, leader uses ReadUncommitted and stores this offset as replica LEO
}
type FetchResponse struct {
	Entry *LogEntry
}

// FetchBatchRequest fetches up to MaxCount records starting at Offset.
// When ReplicaNodeID is set, leader uses ReadUncommitted and records replica LEO after the batch.
type FetchBatchRequest struct {
	Topic         string
	Id            string
	Offset        uint64
	MaxCount      uint32
	ReplicaNodeID string
}
type FetchBatchResponse struct {
	Entries []*LogEntry
}
type CommitOffsetRequest struct {
	Topic  string
	Id     string
	Offset uint64
}
type CommitOffsetResponse struct {
	Success bool
}
type FetchOffsetRequest struct {
	Topic string
	Id    string
}
type FetchOffsetResponse struct {
	Offset uint64
}
