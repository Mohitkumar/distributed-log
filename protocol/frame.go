package protocol

import (
	"encoding/binary"
)

type MessageType uint16

var byteOrder = binary.BigEndian

const messageTypeSize = 2

const messageSizeSize = 4

// frameHeaderSize is the length prefix size (2 bytes for message type, 4 bytes for message size)
const frameHeaderSize = messageTypeSize + messageSizeSize

// MaxFrameSize is the maximum allowed frame payload size (4MB) to avoid abuse.
const MaxFrameSize = 4 * 1024 * 1024

const (
	MsgReplicateStream MessageType = iota
	MsgReplicateResp
	MsgProduce
	MsgProduceResp
	MsgProduceBatch
	MsgProduceBatchResp
	MsgFetch
	MsgFetchResp
	MsgFetchStream
	MsgFetchStreamResp
	MsgCommitOffset
	MsgCommitOffsetResp
	MsgFetchOffset
	MsgFetchOffsetResp
	MsgCreateTopic
	MsgCreateTopicResp
	MsgDeleteTopic
	MsgDeleteTopicResp
	MsgApplyIsrUpdateEvent
	MsgApplyIsrUpdateEventResp
	MsgRPCError
	MsgFindLeader
	MsgFindLeaderResp
	MsgGetRaftLeader
	MsgGetRaftLeaderResp
)
