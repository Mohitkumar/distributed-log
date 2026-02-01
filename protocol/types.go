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
	Topic        string
	ReplicaCount uint32
}
type CreateTopicResponse struct {
	Topic string
}
type DeleteTopicRequest struct {
	Topic string
}
type DeleteTopicResponse struct {
	Topic string
}
type RecordLEORequest struct {
	Topic     string
	ReplicaId string
	Leo       int64
}
type RecordLEOResponse struct{}
type ReplicateRequest struct {
	Topic     string
	Offset    uint64
	BatchSize uint32
}
type ReplicateResponse struct {
	RawChunk    []byte // raw segment-format records: [Offset 8][Len 4][Value]...
	EndOfStream bool   // when true, leader has sent all data for this round; replica can send RecordLEO and next ReplicateRequest
}

// Replication types (replace api/replication).
type CreateReplicaRequest struct {
	Topic      string
	ReplicaId  string
	LeaderAddr string
}
type CreateReplicaResponse struct {
	ReplicaId string
}
type DeleteReplicaRequest struct {
	Topic     string
	ReplicaId string
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
