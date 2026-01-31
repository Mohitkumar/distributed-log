package rpc

import (
	"context"
	"fmt"

	"github.com/mohitkumar/mlog/protocol"
)

// CreateReplica creates a new replica for a topic on this broker
func (s *RpcServer) CreateReplica(ctx context.Context, req *protocol.CreateReplicaRequest) (*protocol.CreateReplicaResponse, error) {
	if req.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}
	if req.ReplicaId == "" {
		return nil, fmt.Errorf("replica_id is required")
	}
	if req.LeaderAddr == "" {
		return nil, fmt.Errorf("leader_addr is required")
	}

	err := s.topicManager.CreateReplicaRemote(req.Topic, req.ReplicaId, req.LeaderAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create replica: %w", err)
	}

	return &protocol.CreateReplicaResponse{
		ReplicaId: req.ReplicaId,
	}, nil
}

// DeleteReplica deletes a replica for a topic
func (s *RpcServer) DeleteReplica(ctx context.Context, req *protocol.DeleteReplicaRequest) (*protocol.DeleteReplicaResponse, error) {
	if req.ReplicaId == "" {
		return nil, fmt.Errorf("replica_id is required")
	}
	err := s.topicManager.DeleteReplicaRemote(req.Topic, req.ReplicaId)
	if err != nil {
		return nil, fmt.Errorf("failed to delete replica: %w", err)
	}

	return &protocol.DeleteReplicaResponse{}, nil
}

// Replicate returns one batch of log entries from the leader for the given topic/offset (handler-style; client may call in a loop for streaming).
func (s *RpcServer) Replicate(ctx context.Context, req *protocol.ReplicateRequest) (*protocol.ReplicateResponse, error) {
	leaderNode, err := s.topicManager.GetLeader(req.Topic)
	if err != nil {
		return nil, err
	}
	batchSize := req.BatchSize
	if batchSize == 0 {
		batchSize = 1000
	}
	currentOffset := req.Offset
	endExclusive := leaderNode.Log.LEO()
	entries := make([]*protocol.LogEntry, 0, batchSize)
	for i := 0; i < int(batchSize); i++ {
		endExclusive = leaderNode.Log.LEO()
		if currentOffset >= endExclusive {
			break
		}
		data, err := leaderNode.Log.ReadUncommitted(currentOffset)
		if err != nil {
			break
		}
		entries = append(entries, &protocol.LogEntry{Offset: currentOffset, Value: data})
		currentOffset++
	}
	var lastOffset uint64
	if len(entries) > 0 {
		lastOffset = currentOffset - 1
	}
	return &protocol.ReplicateResponse{
		LastOffset: lastOffset,
		Entries:    entries,
	}, nil
}
