package rpc

import (
	"context"
	"io"
	"time"

	"github.com/mohitkumar/mlog/protocol"
)

func (s *RpcServer) CreateTopic(ctx context.Context, req *protocol.CreateTopicRequest) (*protocol.CreateTopicResponse, error) {
	if req.Topic == "" {
		return nil, ErrTopicNameRequired
	}
	if req.ReplicaCount < 1 {
		return nil, ErrReplicaCountInvalid
	}
	return s.topicManager.CreateTopicWithForwarding(ctx, req)
}

func (s *RpcServer) DeleteTopic(ctx context.Context, req *protocol.DeleteTopicRequest) (*protocol.DeleteTopicResponse, error) {
	if req.Topic == "" {
		return nil, ErrTopicNameRequired
	}
	return s.topicManager.DeleteTopicWithForwarding(ctx, req)
}

// ApplyDeleteTopicEvent applies DeleteTopicEvent to the Raft log. Call on the Raft leader (e.g. from topic leader via RPC).
func (s *RpcServer) ApplyDeleteTopicEvent(ctx context.Context, req *protocol.ApplyDeleteTopicEventRequest) (*protocol.ApplyDeleteTopicEventResponse, error) {
	s.topicManager.ApplyDeleteTopicEvent(req.Topic)
	return &protocol.ApplyDeleteTopicEventResponse{}, nil
}

// ApplyIsrUpdateEvent applies IsrUpdateEvent to the Raft log. Call on the Raft leader (e.g. from replica/leader via RPC).
func (s *RpcServer) ApplyIsrUpdateEvent(ctx context.Context, req *protocol.ApplyIsrUpdateEventRequest) (*protocol.ApplyIsrUpdateEventResponse, error) {
	s.topicManager.ApplyIsrUpdateEvent(req.Topic, req.ReplicaNodeID, req.Isr, req.Leo)
	return &protocol.ApplyIsrUpdateEventResponse{}, nil
}

// RecordLEO records the Log End Offset (LEO) of a replica
func (s *RpcServer) RecordLEO(ctx context.Context, req *protocol.RecordLEORequest) (*protocol.RecordLEOResponse, error) {
	err := s.topicManager.RecordLEORemote(req.NodeID, req.Topic, uint64(req.Leo), time.Now())
	if err != nil {
		return nil, ErrTopicNotFound(req.Topic, err)
	}
	return &protocol.RecordLEOResponse{}, nil
}

func (s *RpcServer) handleReplicate(req *protocol.ReplicateRequest) (any, error) {
	leaderLog, err := s.topicManager.GetLeader(req.Topic)
	if err != nil {
		if topicObj, e := s.topicManager.GetTopic(req.Topic); e == nil && topicObj != nil && topicObj.Log != nil {
			leaderLog = topicObj.Log
			err = nil
		}
	}
	reader, err := leaderLog.ReaderFrom(req.Offset)

	if err != nil {
		return nil, err
	}

	var batch []protocol.ReplicationRecord

	var hitEOF bool
	for len(batch) < int(req.BatchSize) {
		offset, value, err := protocol.ReadRecordFromStream(reader)
		if err == io.EOF {
			hitEOF = true
			break
		}
		if err != nil {
			return nil, err
		}

		batch = append(batch, protocol.ReplicationRecord{Offset: int64(offset), Value: value})
	}
	// EndOfStream when we reached end of log (EOF) or log was empty
	endOfStream := hitEOF || len(batch) == 0
	var rawChunk []byte
	if len(batch) > 0 {
		var encErr error
		rawChunk, encErr = protocol.EncodeReplicationBatch(batch)
		if encErr != nil {
			return nil, encErr
		}
	}
	return protocol.ReplicateResponse{Topic: req.Topic, RawChunk: rawChunk, EndOfStream: endOfStream}, nil
}
