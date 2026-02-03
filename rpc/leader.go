package rpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/mohitkumar/mlog/protocol"
)

// CreateTopic creates a new topic. Forwarding to Raft leader and topic leader is handled by TopicManager.
func (s *RpcServer) CreateTopic(ctx context.Context, req *protocol.CreateTopicRequest) (*protocol.CreateTopicResponse, error) {
	if req.Topic == "" {
		return nil, fmt.Errorf("topic name is required")
	}
	if req.ReplicaCount < 1 {
		return nil, fmt.Errorf("replica count must be at least 1")
	}
	return s.topicManager.CreateTopicWithForwarding(ctx, req)
}

// DeleteTopic deletes a topic. Forwarding to the topic leader is handled by TopicManager.
func (s *RpcServer) DeleteTopic(ctx context.Context, req *protocol.DeleteTopicRequest) (*protocol.DeleteTopicResponse, error) {
	if req.Topic == "" {
		return nil, fmt.Errorf("topic name is required")
	}
	return s.topicManager.DeleteTopicWithForwarding(ctx, req)
}

// RecordLEO records the Log End Offset (LEO) of a replica
func (s *RpcServer) RecordLEO(ctx context.Context, req *protocol.RecordLEORequest) (*protocol.RecordLEOResponse, error) {
	if req.ReplicaId == "" {
		return nil, fmt.Errorf("replica_id is required")
	}

	topicObj, err := s.topicManager.GetTopic(req.Topic)
	if err != nil {
		return nil, fmt.Errorf("topic %s not found: %w", req.Topic, err)
	}

	err = topicObj.RecordLEORemote(req.ReplicaId, uint64(req.Leo), time.Now())
	if err != nil {
		return nil, fmt.Errorf("failed to record LEO for replica %s in topic %s: %w", req.ReplicaId, req.Topic, err)
	}

	return &protocol.RecordLEOResponse{}, nil
}

// handleReplicateStream runs on a persistent connection: reads records from the leader log
// from req.Offset, batches them (Kafka-style batch: baseOffset, batchLength, leaderEpoch, crc,
// attributes, lastOffsetDelta, then records [offset+size+value]), and sends each batch to the replica
// until caught up, then sends EndOfStream.
func (s *RpcServer) handleReplicateStream(ctx context.Context, msg any, conn net.Conn, codec *protocol.Codec) error {
	req := msg.(protocol.ReplicateRequest)
	leaderView, err := s.topicManager.GetLeader(req.Topic)
	if err != nil {
		return err
	}
	reader, err := leaderView.Log.ReaderFrom(req.Offset)
	if err != nil {
		return err
	}
	batchSize := req.BatchSize
	if batchSize == 0 {
		batchSize = 1000
	}
	var batch []protocol.ReplicationRecord
	for {
		offset, value, err := protocol.ReadRecordFromStream(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		batch = append(batch, protocol.ReplicationRecord{Offset: int64(offset), Value: value})
		if len(batch) >= int(batchSize) {
			payload, err := protocol.EncodeReplicationBatch(batch)
			if err != nil {
				return err
			}
			if err := codec.Encode(conn, &protocol.ReplicateResponse{RawChunk: payload, EndOfStream: false}); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		payload, err := protocol.EncodeReplicationBatch(batch)
		if err != nil {
			return err
		}
		if err := codec.Encode(conn, &protocol.ReplicateResponse{RawChunk: payload, EndOfStream: false}); err != nil {
			return err
		}
	}
	if err := codec.Encode(conn, &protocol.ReplicateResponse{RawChunk: nil, EndOfStream: true}); err != nil {
		return err
	}
	return nil
}
