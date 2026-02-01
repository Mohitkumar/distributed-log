package protocol

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

var ErrReplicationBatchCRC = errors.New("protocol: replication batch CRC mismatch")

// Replication batch format (Kafka-style). Leader sends batches of records to replicas.
// Batch header: baseOffset, batchLength, leaderEpoch, crc, attributes, lastOffsetDelta.
// Records: for each [offset 8][size 4][value size bytes].

const (
	// LeaderEpoch is hardcoded until Raft is implemented; then we use the actual epoch.
	LeaderEpoch        = 0
	CompressionNone    = 0
	replicationBatchHeaderSize = 8 + 4 + 4 + 4 + 1 + 4 // 25 bytes
	replicationRecordHeaderSize = 8 + 4                // offset + size
)

var replicationByteOrder = binary.BigEndian

// ReplicationRecord is a single record in a replication batch (offset + value).
type ReplicationRecord struct {
	Offset int64
	Value  []byte
}

// EncodeReplicationBatch encodes a batch of records into the wire format.
// Header: baseOffset (8), batchLength (4), leaderEpoch (4), crc (4), attributes (1), lastOffsetDelta (4).
// Body: for each record [offset 8][size 4][value].
func EncodeReplicationBatch(records []ReplicationRecord) ([]byte, error) {
	if len(records) == 0 {
		return nil, nil
	}
	baseOffset := records[0].Offset
	lastOffset := records[len(records)-1].Offset
	lastOffsetDelta := int32(lastOffset - baseOffset)

	var recordBuf []byte
	for _, r := range records {
		recordBuf = append(recordBuf, encodeRecord(r)...)
	}
	batchLength := int32(len(recordBuf))
	crc := crc32.ChecksumIEEE(recordBuf)

	// Header: baseOffset, batchLength, leaderEpoch, crc, attributes, lastOffsetDelta
	buf := make([]byte, replicationBatchHeaderSize+len(recordBuf))
	off := 0
	replicationByteOrder.PutUint64(buf[off:off+8], uint64(baseOffset))
	off += 8
	replicationByteOrder.PutUint32(buf[off:off+4], uint32(batchLength))
	off += 4
	replicationByteOrder.PutUint32(buf[off:off+4], uint32(LeaderEpoch))
	off += 4
	replicationByteOrder.PutUint32(buf[off:off+4], crc)
	off += 4
	buf[off] = byte(CompressionNone)
	off++
	replicationByteOrder.PutUint32(buf[off:off+4], uint32(lastOffsetDelta))
	off += 4
	copy(buf[off:], recordBuf)
	return buf, nil
}

func encodeRecord(r ReplicationRecord) []byte {
	buf := make([]byte, replicationRecordHeaderSize+len(r.Value))
	replicationByteOrder.PutUint64(buf[0:8], uint64(r.Offset))
	replicationByteOrder.PutUint32(buf[8:12], uint32(len(r.Value)))
	copy(buf[12:], r.Value)
	return buf
}

// DecodeReplicationBatch decodes a batch payload (header + records) and returns the records.
// Caller applies each record to the local log (e.g. Append(record.Value)).
func DecodeReplicationBatch(data []byte) ([]ReplicationRecord, error) {
	if len(data) < replicationBatchHeaderSize {
		return nil, io.ErrUnexpectedEOF
	}
	baseOffset := int64(replicationByteOrder.Uint64(data[0:8]))
	batchLength := int32(replicationByteOrder.Uint32(data[8:12]))
	_ = replicationByteOrder.Uint32(data[12:16]) // leaderEpoch
	crcStored := replicationByteOrder.Uint32(data[16:20])
	// attributes at 20, lastOffsetDelta at 21:25
	recordBuf := data[replicationBatchHeaderSize:]
	if int32(len(recordBuf)) != batchLength {
		return nil, io.ErrUnexpectedEOF
	}
	if crc32.ChecksumIEEE(recordBuf) != crcStored {
		return nil, ErrReplicationBatchCRC
	}
	var records []ReplicationRecord
	p := recordBuf
	for len(p) >= replicationRecordHeaderSize {
		offset := int64(replicationByteOrder.Uint64(p[0:8]))
		size := int32(replicationByteOrder.Uint32(p[8:12]))
		if size < 0 || int(replicationRecordHeaderSize)+int(size) > len(p) {
			break
		}
		value := make([]byte, size)
		copy(value, p[12:12+size])
		records = append(records, ReplicationRecord{Offset: offset, Value: value})
		p = p[12+size:]
	}
	// Sanity: baseOffset should match first record
	if len(records) > 0 && records[0].Offset != baseOffset {
		// still return records; offset is informational
	}
	return records, nil
}

// ReadRecordFromStream reads one record in segment format [offset 8][len 4][value] from r.
// Used by the leader to read from Log.ReaderFrom() and build replication batches.
func ReadRecordFromStream(r io.Reader) (offset uint64, value []byte, err error) {
	header := make([]byte, 8+4)
	if _, err = io.ReadFull(r, header); err != nil {
		return 0, nil, err
	}
	offset = replicationByteOrder.Uint64(header[0:8])
	size := replicationByteOrder.Uint32(header[8:12])
	value = make([]byte, size)
	if _, err = io.ReadFull(r, value); err != nil {
		return 0, nil, err
	}
	return offset, value, nil
}
