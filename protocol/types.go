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

// ApplyDeleteTopicEventRequest is sent to the Raft leader to apply DeleteTopicEvent to the log.
type ApplyDeleteTopicEventRequest struct {
	Topic string
}
type ApplyDeleteTopicEventResponse struct{}

// ApplyIsrUpdateEventRequest is sent to the Raft leader to apply IsrUpdateEvent to the log.
type ApplyIsrUpdateEventRequest struct {
	Topic         string
	ReplicaNodeID string
	Isr           bool
	Leo           int64
}
type ApplyIsrUpdateEventResponse struct{}

// RPCErrorResponse is sent by the server when a handler returns an error, so the client gets the error message instead of EOF.
type RPCErrorResponse struct {
	Message string `json:"message"`
}

type RecordLEORequest struct {
	NodeID string
	Topic  string
	Leo    int64
}
type RecordLEOResponse struct{}
type ReplicateRequest struct {
	Topic         string
	Offset        uint64 // replica LEO; leader streams from this offset
	BatchSize     uint32
	ReplicaNodeID string // leader caches reader per (topic, replica)
}
type ReplicateResponse struct {
	Topic       string
	RawChunk    []byte // raw segment-format records: [Offset 8][Len 4][Value]...
	EndOfStream bool   // when true, leader has sent all data for this round; replica can send RecordLEO and next ReplicateRequest
}

// Replication types (replace api/replication).
type CreateReplicaRequest struct {
	Topic    string
	LeaderId string
}
type CreateReplicaResponse struct {
	Topic string
}
type DeleteReplicaRequest struct {
	Topic string
}
type DeleteReplicaResponse struct{}

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

// Consumer types (replace api/consumer).
type FetchRequest struct {
	Topic  string
	Id     string
	Offset uint64
}
type FetchResponse struct {
	Entry *LogEntry
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
