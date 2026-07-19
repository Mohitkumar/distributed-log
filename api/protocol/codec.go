package protocol

import (
	"io"

	"github.com/mohitkumar/mlog/api/protocol/pb"
	"google.golang.org/protobuf/proto"
)

type Codec struct{}

func (c *Codec) Encode(w io.Writer, msg any) error {
	var mType MessageType
	var payload []byte
	var err error
	switch v := msg.(type) {
	case ReplicateRequest:
		mType = MsgReplicateStream
		payload, err = proto.Marshal(&pb.ReplicateRequest{Topic: v.Topic, Offset: v.Offset, BatchSize: v.BatchSize, ReplicaNodeId: v.ReplicaNodeID})
	case *ReplicateRequest:
		mType = MsgReplicateStream
		payload, err = proto.Marshal(&pb.ReplicateRequest{Topic: v.Topic, Offset: v.Offset, BatchSize: v.BatchSize, ReplicaNodeId: v.ReplicaNodeID})
	case ReplicateResponse:
		mType = MsgReplicateResp
		payload, err = proto.Marshal(&pb.ReplicateResponse{Topic: v.Topic, RawChunk: v.RawChunk, EndOfStream: v.EndOfStream, LeaderLeo: v.LeaderLEO})
	case *ReplicateResponse:
		mType = MsgReplicateResp
		payload, err = proto.Marshal(&pb.ReplicateResponse{Topic: v.Topic, RawChunk: v.RawChunk, EndOfStream: v.EndOfStream, LeaderLeo: v.LeaderLEO})
	case ProduceRequest:
		mType = MsgProduce
		payload, err = proto.Marshal(&pb.ProduceRequest{Topic: v.Topic, Value: v.Value, Acks: int32(v.Acks)})
	case *ProduceRequest:
		mType = MsgProduce
		payload, err = proto.Marshal(&pb.ProduceRequest{Topic: v.Topic, Value: v.Value, Acks: int32(v.Acks)})
	case ProduceResponse:
		mType = MsgProduceResp
		payload, err = proto.Marshal(&pb.ProduceResponse{Offset: v.Offset})
	case *ProduceResponse:
		mType = MsgProduceResp
		payload, err = proto.Marshal(&pb.ProduceResponse{Offset: v.Offset})
	case ProduceBatchRequest:
		mType = MsgProduceBatch
		payload, err = proto.Marshal(&pb.ProduceBatchRequest{Topic: v.Topic, Values: v.Values, Acks: int32(v.Acks)})
	case *ProduceBatchRequest:
		mType = MsgProduceBatch
		payload, err = proto.Marshal(&pb.ProduceBatchRequest{Topic: v.Topic, Values: v.Values, Acks: int32(v.Acks)})
	case ProduceBatchResponse:
		mType = MsgProduceBatchResp
		payload, err = proto.Marshal(&pb.ProduceBatchResponse{BaseOffset: v.BaseOffset, LastOffset: v.LastOffset, Count: v.Count})
	case *ProduceBatchResponse:
		mType = MsgProduceBatchResp
		payload, err = proto.Marshal(&pb.ProduceBatchResponse{BaseOffset: v.BaseOffset, LastOffset: v.LastOffset, Count: v.Count})
	case FetchRequest:
		mType = MsgFetch
		payload, err = proto.Marshal(&pb.FetchRequest{Topic: v.Topic, Id: v.Id, Offset: v.Offset, ReplicaNodeId: v.ReplicaNodeID})
	case *FetchRequest:
		mType = MsgFetch
		payload, err = proto.Marshal(&pb.FetchRequest{Topic: v.Topic, Id: v.Id, Offset: v.Offset, ReplicaNodeId: v.ReplicaNodeID})
	case FetchResponse:
		mType = MsgFetchResp
		payload, err = proto.Marshal(&pb.FetchResponse{Entry: logEntryToPB(v.Entry)})
	case *FetchResponse:
		mType = MsgFetchResp
		payload, err = proto.Marshal(&pb.FetchResponse{Entry: logEntryToPB(v.Entry)})
	case FetchBatchRequest:
		mType = MsgFetchBatch
		payload, err = proto.Marshal(&pb.FetchBatchRequest{Topic: v.Topic, Id: v.Id, Offset: v.Offset, MaxCount: v.MaxCount, ReplicaNodeId: v.ReplicaNodeID})
	case *FetchBatchRequest:
		mType = MsgFetchBatch
		payload, err = proto.Marshal(&pb.FetchBatchRequest{Topic: v.Topic, Id: v.Id, Offset: v.Offset, MaxCount: v.MaxCount, ReplicaNodeId: v.ReplicaNodeID})
	case FetchBatchResponse:
		mType = MsgFetchBatchResp
		payload, err = proto.Marshal(&pb.FetchBatchResponse{Entries: logEntriesToPB(v.Entries)})
	case *FetchBatchResponse:
		mType = MsgFetchBatchResp
		payload, err = proto.Marshal(&pb.FetchBatchResponse{Entries: logEntriesToPB(v.Entries)})
	case CommitOffsetRequest:
		mType = MsgCommitOffset
		payload, err = proto.Marshal(&pb.CommitOffsetRequest{Topic: v.Topic, Id: v.Id, Offset: v.Offset})
	case *CommitOffsetRequest:
		mType = MsgCommitOffset
		payload, err = proto.Marshal(&pb.CommitOffsetRequest{Topic: v.Topic, Id: v.Id, Offset: v.Offset})
	case CommitOffsetResponse:
		mType = MsgCommitOffsetResp
		payload, err = proto.Marshal(&pb.CommitOffsetResponse{Success: v.Success})
	case *CommitOffsetResponse:
		mType = MsgCommitOffsetResp
		payload, err = proto.Marshal(&pb.CommitOffsetResponse{Success: v.Success})
	case FetchOffsetRequest:
		mType = MsgFetchOffset
		payload, err = proto.Marshal(&pb.FetchOffsetRequest{Topic: v.Topic, Id: v.Id})
	case *FetchOffsetRequest:
		mType = MsgFetchOffset
		payload, err = proto.Marshal(&pb.FetchOffsetRequest{Topic: v.Topic, Id: v.Id})
	case FetchOffsetResponse:
		mType = MsgFetchOffsetResp
		payload, err = proto.Marshal(&pb.FetchOffsetResponse{Offset: v.Offset})
	case *FetchOffsetResponse:
		mType = MsgFetchOffsetResp
		payload, err = proto.Marshal(&pb.FetchOffsetResponse{Offset: v.Offset})
	case CreateTopicRequest:
		mType = MsgCreateTopic
		payload, err = proto.Marshal(&pb.CreateTopicRequest{Topic: v.Topic, ReplicaCount: v.ReplicaCount, DesignatedLeaderNodeId: v.DesignatedLeaderNodeID})
	case *CreateTopicRequest:
		mType = MsgCreateTopic
		payload, err = proto.Marshal(&pb.CreateTopicRequest{Topic: v.Topic, ReplicaCount: v.ReplicaCount, DesignatedLeaderNodeId: v.DesignatedLeaderNodeID})
	case CreateTopicResponse:
		mType = MsgCreateTopicResp
		payload, err = proto.Marshal(&pb.CreateTopicResponse{Topic: v.Topic, ReplicaNodeIds: v.ReplicaNodeIds})
	case *CreateTopicResponse:
		mType = MsgCreateTopicResp
		payload, err = proto.Marshal(&pb.CreateTopicResponse{Topic: v.Topic, ReplicaNodeIds: v.ReplicaNodeIds})
	case DeleteTopicRequest:
		mType = MsgDeleteTopic
		payload, err = proto.Marshal(&pb.DeleteTopicRequest{Topic: v.Topic})
	case *DeleteTopicRequest:
		mType = MsgDeleteTopic
		payload, err = proto.Marshal(&pb.DeleteTopicRequest{Topic: v.Topic})
	case DeleteTopicResponse:
		mType = MsgDeleteTopicResp
		payload, err = proto.Marshal(&pb.DeleteTopicResponse{Topic: v.Topic})
	case *DeleteTopicResponse:
		mType = MsgDeleteTopicResp
		payload, err = proto.Marshal(&pb.DeleteTopicResponse{Topic: v.Topic})
	case FindTopicLeaderRequest:
		mType = MsgFindTopicLeader
		payload, err = proto.Marshal(&pb.FindTopicLeaderRequest{Topic: v.Topic})
	case *FindTopicLeaderRequest:
		mType = MsgFindTopicLeader
		payload, err = proto.Marshal(&pb.FindTopicLeaderRequest{Topic: v.Topic})
	case FindTopicLeaderResponse:
		mType = MsgFindTopicLeaderResp
		payload, err = proto.Marshal(&pb.FindTopicLeaderResponse{LeaderAddr: v.LeaderAddr})
	case *FindTopicLeaderResponse:
		mType = MsgFindTopicLeaderResp
		payload, err = proto.Marshal(&pb.FindTopicLeaderResponse{LeaderAddr: v.LeaderAddr})
	case FindRaftLeaderRequest:
		mType = MsgFindRaftLeader
		payload, err = proto.Marshal(&pb.FindRaftLeaderRequest{})
	case *FindRaftLeaderRequest:
		mType = MsgFindRaftLeader
		payload, err = proto.Marshal(&pb.FindRaftLeaderRequest{})
	case FindRaftLeaderResponse:
		mType = MsgFindRaftLeaderResp
		payload, err = proto.Marshal(&pb.FindRaftLeaderResponse{RaftLeaderAddr: v.RaftLeaderAddr})
	case *FindRaftLeaderResponse:
		mType = MsgFindRaftLeaderResp
		payload, err = proto.Marshal(&pb.FindRaftLeaderResponse{RaftLeaderAddr: v.RaftLeaderAddr})
	case ListTopicsRequest:
		mType = MsgListTopics
		payload, err = proto.Marshal(&pb.ListTopicsRequest{})
	case *ListTopicsRequest:
		mType = MsgListTopics
		payload, err = proto.Marshal(&pb.ListTopicsRequest{})
	case ListTopicsResponse:
		mType = MsgListTopicsResp
		payload, err = proto.Marshal(&pb.ListTopicsResponse{Topics: topicInfosToPB(v.Topics)})
	case *ListTopicsResponse:
		mType = MsgListTopicsResp
		payload, err = proto.Marshal(&pb.ListTopicsResponse{Topics: topicInfosToPB(v.Topics)})
	case ApplyIsrUpdateEventRequest:
		mType = MsgApplyIsrUpdateEvent
		payload, err = proto.Marshal(&pb.ApplyIsrUpdateEventRequest{Topic: v.Topic, ReplicaNodeId: v.ReplicaNodeID, Isr: v.Isr})
	case *ApplyIsrUpdateEventRequest:
		mType = MsgApplyIsrUpdateEvent
		payload, err = proto.Marshal(&pb.ApplyIsrUpdateEventRequest{Topic: v.Topic, ReplicaNodeId: v.ReplicaNodeID, Isr: v.Isr})
	case ApplyIsrUpdateEventResponse:
		mType = MsgApplyIsrUpdateEventResp
		payload, err = proto.Marshal(&pb.ApplyIsrUpdateEventResponse{})
	case *ApplyIsrUpdateEventResponse:
		mType = MsgApplyIsrUpdateEventResp
		payload, err = proto.Marshal(&pb.ApplyIsrUpdateEventResponse{})
	case RPCErrorResponse:
		mType = MsgRPCError
		payload, err = proto.Marshal(&pb.RPCErrorResponse{Code: v.Code, Message: v.Message})
	case *RPCErrorResponse:
		mType = MsgRPCError
		payload, err = proto.Marshal(&pb.RPCErrorResponse{Code: v.Code, Message: v.Message})
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
		var m pb.ReplicateRequest
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, ReplicateRequest{Topic: m.Topic, Offset: m.Offset, BatchSize: m.BatchSize, ReplicaNodeID: m.ReplicaNodeId}, nil
	case MsgReplicateResp:
		var m pb.ReplicateResponse
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, ReplicateResponse{Topic: m.Topic, RawChunk: m.RawChunk, EndOfStream: m.EndOfStream, LeaderLEO: m.LeaderLeo}, nil
	case MsgProduce:
		var m pb.ProduceRequest
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, ProduceRequest{Topic: m.Topic, Value: m.Value, Acks: AckMode(m.Acks)}, nil
	case MsgProduceResp:
		var m pb.ProduceResponse
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, ProduceResponse{Offset: m.Offset}, nil
	case MsgProduceBatch:
		var m pb.ProduceBatchRequest
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, ProduceBatchRequest{Topic: m.Topic, Values: m.Values, Acks: AckMode(m.Acks)}, nil
	case MsgProduceBatchResp:
		var m pb.ProduceBatchResponse
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, ProduceBatchResponse{BaseOffset: m.BaseOffset, LastOffset: m.LastOffset, Count: m.Count}, nil
	case MsgFetch:
		var m pb.FetchRequest
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, FetchRequest{Topic: m.Topic, Id: m.Id, Offset: m.Offset, ReplicaNodeID: m.ReplicaNodeId}, nil
	case MsgFetchResp:
		var m pb.FetchResponse
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, FetchResponse{Entry: logEntryFromPB(m.Entry)}, nil
	case MsgFetchBatch:
		var m pb.FetchBatchRequest
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, FetchBatchRequest{Topic: m.Topic, Id: m.Id, Offset: m.Offset, MaxCount: m.MaxCount, ReplicaNodeID: m.ReplicaNodeId}, nil
	case MsgFetchBatchResp:
		var m pb.FetchBatchResponse
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, FetchBatchResponse{Entries: logEntriesFromPB(m.Entries)}, nil
	case MsgCommitOffset:
		var m pb.CommitOffsetRequest
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, CommitOffsetRequest{Topic: m.Topic, Id: m.Id, Offset: m.Offset}, nil
	case MsgCommitOffsetResp:
		var m pb.CommitOffsetResponse
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, CommitOffsetResponse{Success: m.Success}, nil
	case MsgFetchOffset:
		var m pb.FetchOffsetRequest
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, FetchOffsetRequest{Topic: m.Topic, Id: m.Id}, nil
	case MsgFetchOffsetResp:
		var m pb.FetchOffsetResponse
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, FetchOffsetResponse{Offset: m.Offset}, nil
	case MsgCreateTopic:
		var m pb.CreateTopicRequest
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, CreateTopicRequest{Topic: m.Topic, ReplicaCount: m.ReplicaCount, DesignatedLeaderNodeID: m.DesignatedLeaderNodeId}, nil
	case MsgCreateTopicResp:
		var m pb.CreateTopicResponse
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, CreateTopicResponse{Topic: m.Topic, ReplicaNodeIds: m.ReplicaNodeIds}, nil
	case MsgDeleteTopic:
		var m pb.DeleteTopicRequest
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, DeleteTopicRequest{Topic: m.Topic}, nil
	case MsgDeleteTopicResp:
		var m pb.DeleteTopicResponse
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, DeleteTopicResponse{Topic: m.Topic}, nil
	case MsgApplyIsrUpdateEvent:
		var m pb.ApplyIsrUpdateEventRequest
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, ApplyIsrUpdateEventRequest{Topic: m.Topic, ReplicaNodeID: m.ReplicaNodeId, Isr: m.Isr}, nil
	case MsgApplyIsrUpdateEventResp:
		var m pb.ApplyIsrUpdateEventResponse
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, ApplyIsrUpdateEventResponse{}, nil
	case MsgFindTopicLeader:
		var m pb.FindTopicLeaderRequest
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, FindTopicLeaderRequest{Topic: m.Topic}, nil
	case MsgFindTopicLeaderResp:
		var m pb.FindTopicLeaderResponse
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, FindTopicLeaderResponse{LeaderAddr: m.LeaderAddr}, nil
	case MsgFindRaftLeader:
		var m pb.FindRaftLeaderRequest
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, FindRaftLeaderRequest{}, nil
	case MsgFindRaftLeaderResp:
		var m pb.FindRaftLeaderResponse
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, FindRaftLeaderResponse{RaftLeaderAddr: m.RaftLeaderAddr}, nil
	case MsgListTopics:
		var m pb.ListTopicsRequest
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, ListTopicsRequest{}, nil
	case MsgListTopicsResp:
		var m pb.ListTopicsResponse
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, ListTopicsResponse{Topics: topicInfosFromPB(m.Topics)}, nil
	case MsgRPCError:
		var m pb.RPCErrorResponse
		if err := proto.Unmarshal(payload, &m); err != nil {
			return mType, nil, err
		}
		return mType, RPCErrorResponse{Code: m.Code, Message: m.Message}, nil
	default:
		return 0, nil, ErrUnknownMessageType(mType)
	}
}

func (c *Codec) encodeFrame(w io.Writer, mType MessageType, payload []byte) error {
	length := uint32(len(payload))
	if length > MaxFrameSize {
		return ErrFrameTooLarge
	}
	// Stack-allocated header avoids a heap allocation per frame.
	var header [frameHeaderSize]byte
	byteOrder.PutUint16(header[:], uint16(mType))
	byteOrder.PutUint32(header[messageTypeSize:], length)
	// Single write: combine header + payload to avoid two syscalls.
	buf := make([]byte, frameHeaderSize+len(payload))
	copy(buf, header[:])
	copy(buf[frameHeaderSize:], payload)
	_, err := w.Write(buf)
	return err
}

// decodeFrame reads a length-prefixed frame from r and returns the payload.
func (c *Codec) decodeFrame(r io.Reader) (mType MessageType, payload []byte, err error) {
	// Stack-allocated header avoids a heap allocation per frame.
	var header [frameHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, nil, err
	}
	mType = MessageType(byteOrder.Uint16(header[:]))
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
