package protocol

import (
	"encoding/json"
	"io"
)

type Codec struct{}

func (c *Codec) Encode(w io.Writer, msg any) error {
	var mType MessageType
	var payload []byte
	var err error
	switch v := msg.(type) {
	case ReplicateRequest, *ReplicateRequest:
		mType = MsgReplicateStream
		payload, err = json.Marshal(v)
	case ReplicateResponse, *ReplicateResponse:
		mType = MsgReplicateResp
		payload, err = json.Marshal(v)
	case ProduceRequest, *ProduceRequest:
		mType = MsgProduce
		payload, err = json.Marshal(v)
	case ProduceResponse, *ProduceResponse:
		mType = MsgProduceResp
		payload, err = json.Marshal(v)
	case ProduceBatchRequest, *ProduceBatchRequest:
		mType = MsgProduceBatch
		payload, err = json.Marshal(v)
	case ProduceBatchResponse, *ProduceBatchResponse:
		mType = MsgProduceBatchResp
		payload, err = json.Marshal(v)
	case FetchRequest, *FetchRequest:
		mType = MsgFetch
		payload, err = json.Marshal(v)
	case FetchResponse, *FetchResponse:
		mType = MsgFetchResp
		payload, err = json.Marshal(v)
	case FetchBatchRequest, *FetchBatchRequest:
		mType = MsgFetchBatch
		payload, err = json.Marshal(v)
	case FetchBatchResponse, *FetchBatchResponse:
		mType = MsgFetchBatchResp
		payload, err = json.Marshal(v)
	case CommitOffsetRequest, *CommitOffsetRequest:
		mType = MsgCommitOffset
		payload, err = json.Marshal(v)
	case CommitOffsetResponse, *CommitOffsetResponse:
		mType = MsgCommitOffsetResp
		payload, err = json.Marshal(v)
	case FetchOffsetRequest, *FetchOffsetRequest:
		mType = MsgFetchOffset
		payload, err = json.Marshal(v)
	case FetchOffsetResponse, *FetchOffsetResponse:
		mType = MsgFetchOffsetResp
		payload, err = json.Marshal(v)
	case CreateTopicRequest, *CreateTopicRequest:
		mType = MsgCreateTopic
		payload, err = json.Marshal(v)
	case CreateTopicResponse, *CreateTopicResponse:
		mType = MsgCreateTopicResp
		payload, err = json.Marshal(v)
	case DeleteTopicRequest, *DeleteTopicRequest:
		mType = MsgDeleteTopic
		payload, err = json.Marshal(v)
	case DeleteTopicResponse, *DeleteTopicResponse:
		mType = MsgDeleteTopicResp
		payload, err = json.Marshal(v)
	case FindTopicLeaderRequest, *FindTopicLeaderRequest:
		mType = MsgFindTopicLeader
		payload, err = json.Marshal(v)
	case FindTopicLeaderResponse, *FindTopicLeaderResponse:
		mType = MsgFindTopicLeaderResp
		payload, err = json.Marshal(v)
	case FindRaftLeaderRequest, *FindRaftLeaderRequest:
		mType = MsgFindRaftLeader
		payload, err = json.Marshal(v)
	case FindRaftLeaderResponse, *FindRaftLeaderResponse:
		mType = MsgFindRaftLeaderResp
		payload, err = json.Marshal(v)
	case ApplyIsrUpdateEventRequest, *ApplyIsrUpdateEventRequest:
		mType = MsgApplyIsrUpdateEvent
		payload, err = json.Marshal(v)
	case ApplyIsrUpdateEventResponse, *ApplyIsrUpdateEventResponse:
		mType = MsgApplyIsrUpdateEventResp
		payload, err = json.Marshal(v)
	case RPCErrorResponse, *RPCErrorResponse:
		mType = MsgRPCError
		payload, err = json.Marshal(v)
	default:
		return ErrUnknownMessageType(mType)
	}
	if err != nil {
		return err
	}
	return c.encodeFrame(w, mType, payload)
}

func (c *Codec) Decode(r io.Reader) (MessageType, any, error) {
	mType, payload, err := c.decodeFrame(r)
	if err != nil {
		return 0, nil, err
	}
	switch mType {
	case MsgReplicateStream:
		var msg ReplicateRequest
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgReplicateResp:
		var msg ReplicateResponse
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgProduce:
		var msg ProduceRequest
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgProduceResp:
		var msg ProduceResponse
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgProduceBatch:
		var msg ProduceBatchRequest
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgProduceBatchResp:
		var msg ProduceBatchResponse
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgFetch:
		var msg FetchRequest
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgFetchResp:
		var msg FetchResponse
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgFetchBatch:
		var msg FetchBatchRequest
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgFetchBatchResp:
		var msg FetchBatchResponse
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgCommitOffset:
		var msg CommitOffsetRequest
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgCommitOffsetResp:
		var msg CommitOffsetResponse
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgFetchOffset:
		var msg FetchOffsetRequest
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgFetchOffsetResp:
		var msg FetchOffsetResponse
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgCreateTopic:
		var msg CreateTopicRequest
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgCreateTopicResp:
		var msg CreateTopicResponse
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgDeleteTopic:
		var msg DeleteTopicRequest
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgDeleteTopicResp:
		var msg DeleteTopicResponse
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgApplyIsrUpdateEvent:
		var msg ApplyIsrUpdateEventRequest
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgApplyIsrUpdateEventResp:
		var msg ApplyIsrUpdateEventResponse
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgFindTopicLeader:
		var msg FindTopicLeaderRequest
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgFindTopicLeaderResp:
		var msg FindTopicLeaderResponse
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgFindRaftLeader:
		var msg FindRaftLeaderRequest
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgFindRaftLeaderResp:
		var msg FindRaftLeaderResponse
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	case MsgRPCError:
		var msg RPCErrorResponse
		err = json.Unmarshal(payload, &msg)
		return mType, msg, err
	default:
		return 0, nil, ErrUnknownMessageType(mType)
	}
}

func (c *Codec) encodeFrame(w io.Writer, mType MessageType, payload []byte) error {
	length := uint32(len(payload))
	if length > MaxFrameSize {
		return ErrFrameTooLarge
	}
	header := make([]byte, frameHeaderSize)
	byteOrder.PutUint16(header, uint16(mType))
	byteOrder.PutUint32(header[messageTypeSize:], length)
	if _, err := w.Write(header); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	return nil
}

// DecodeFrame reads a length-prefixed frame from r and returns the payload.
func (c *Codec) decodeFrame(r io.Reader) (mType MessageType, payload []byte, err error) {
	header := make([]byte, frameHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, nil, err
	}
	mType = MessageType(byteOrder.Uint16(header))
	length := byteOrder.Uint32(header[messageTypeSize:])
	if length > MaxFrameSize {
		return 0, nil, ErrFrameTooLarge
	}
	payload = make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return mType, payload, nil
}
