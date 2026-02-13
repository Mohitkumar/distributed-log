package protocol

// LogEntry is a log record with offset and value (replaces api/common.LogEntry).
type LogEntry struct {
	Offset uint64
	Value  []byte
}

// AckMode for producer acks (replaces api/producer.AckMode).
type AckMode int32

const (
	AckNone   AckMode = 0
	AckLeader AckMode = 1
	AckAll    AckMode = 2
)

// Leader types (replace api/leader).
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

// RPCErrorResponse is sent by the server when a handler returns an error, so the client gets the error message instead of EOF.
type RPCErrorResponse struct {
	Message string `json:"message"`
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

// Producer types (replace api/producer).
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

// FindLeader types are used by clients to discover the topic leader RPC address.
// Any node can answer this using its local metadata (kept in sync via Raft).
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

// GetRaftLeader types are used by clients to discover the Raft (metadata) leader RPC address.
// Any node can answer; create-topic and other metadata ops should be sent to the Raft leader.
type FindRaftLeaderRequest struct{}

type FindRaftLeaderResponse struct {
	RaftLeaderAddr string
}

// Consumer types (replace api/consumer).
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
