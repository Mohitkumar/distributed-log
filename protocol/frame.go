package protocol

import (
	"encoding/binary"
	"errors"
)

// Message types for transport RPC (4-byte big-endian).
const (
	MsgCreateReplica     uint32 = 1
	MsgCreateReplicaResp uint32 = 2
	MsgDeleteReplica     uint32 = 3
	MsgDeleteReplicaResp uint32 = 4
	MsgReplicateStream   uint32 = 5
	MsgReplicateResp     uint32 = 6
	MsgRecordLEO         uint32 = 7
	MsgRecordLEOResp     uint32 = 8
	MsgProduce           uint32 = 9
	MsgProduceResp       uint32 = 10
	MsgProduceBatch      uint32 = 11
	MsgProduceBatchResp  uint32 = 12
	MsgFetch             uint32 = 13
	MsgFetchResp         uint32 = 14
	MsgFetchStream       uint32 = 15
	MsgFetchStreamResp   uint32 = 16
	MsgCommitOffset      uint32 = 17
	MsgCommitOffsetResp  uint32 = 18
	MsgFetchOffset       uint32 = 19
	MsgFetchOffsetResp   uint32 = 20
	MsgCreateTopic       uint32 = 21
	MsgCreateTopicResp   uint32 = 22
	MsgDeleteTopic       uint32 = 23
	MsgDeleteTopicResp   uint32 = 24
)

var frameByteOrder = binary.BigEndian

// EncodeRequest encodes message type + body for sending over transport.
func EncodeRequest(msgType uint32, body []byte) []byte {
	buf := make([]byte, 4+len(body))
	frameByteOrder.PutUint32(buf[0:4], msgType)
	copy(buf[4:], body)
	return buf
}

// DecodeRequest splits frame payload into message type and body.
func DecodeRequest(payload []byte) (msgType uint32, body []byte, err error) {
	if len(payload) < 4 {
		return 0, nil, errors.New("protocol: payload too short")
	}
	return frameByteOrder.Uint32(payload[0:4]), payload[4:], nil
}

// EncodeResponse encodes message type + body for response.
func EncodeResponse(msgType uint32, body []byte) []byte {
	return EncodeRequest(msgType, body)
}
