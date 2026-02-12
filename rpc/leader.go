package rpc

import (
	"context"
	"io"

	"github.com/mohitkumar/mlog/protocol"
)

func (s *RpcServer) CreateTopic(ctx context.Context, req *protocol.CreateTopicRequest) (*protocol.CreateTopicResponse, error) {
	if req.Topic == "" {
		return nil, ErrTopicNameRequired
	}
	return s.topicManager.CreateTopic(ctx, req)
}

func (s *RpcServer) DeleteTopic(ctx context.Context, req *protocol.DeleteTopicRequest) (*protocol.DeleteTopicResponse, error) {
	if req.Topic == "" {
		return nil, ErrTopicNameRequired
	}
	return s.topicManager.DeleteTopic(ctx, req)
}

// ApplyIsrUpdateEvent applies IsrUpdateEvent to the Raft log. Call on the Raft leader (e.g. from replica/leader via RPC).
func (s *RpcServer) ApplyIsrUpdateEvent(ctx context.Context, req *protocol.ApplyIsrUpdateEventRequest) (*protocol.ApplyIsrUpdateEventResponse, error) {
	if err := s.topicManager.ApplyIsrUpdateEvent(req.Topic, req.ReplicaNodeID, req.Isr, req.Leo); err != nil {
		return nil, err
	}
	return &protocol.ApplyIsrUpdateEventResponse{}, nil
}

func (s *RpcServer) handleReplicate(req *protocol.ReplicateRequest) (any, error) {
	leaderLog, err := s.topicManager.GetLeader(req.Topic)
	if err != nil {
		if topicObj, e := s.topicManager.GetTopic(req.Topic); e == nil && topicObj != nil && topicObj.Log != nil {
			leaderLog = topicObj.Log
			err = nil
		}
	}
	if leaderLog.LEO() <= req.Offset {
		return protocol.ReplicateResponse{Topic: req.Topic, RawChunk: nil, EndOfStream: true, LeaderLEO: int64(leaderLog.LEO())}, nil
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
	leaderLEO := int64(0)
	if endOfStream && leaderLog != nil {
		leaderLEO = int64(leaderLog.LEO())
	}
	return protocol.ReplicateResponse{Topic: req.Topic, RawChunk: rawChunk, EndOfStream: endOfStream, LeaderLEO: leaderLEO}, nil
}
