package rpc

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/mohitkumar/mlog/log"
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

type LeaderReaderCache struct {
	mu        sync.RWMutex
	readerMap map[string]*LeaderCacheEntry
}

func NewLeaderReaderCache() *LeaderReaderCache {
	return &LeaderReaderCache{
		readerMap: make(map[string]*LeaderCacheEntry),
	}
}

type LeaderCacheEntry struct {
	reader     io.Reader
	lastOffset uint64
}

func (c *LeaderReaderCache) Get(topic string, offset uint64, leaderLog *log.LogManager) (io.Reader, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if entry, ok := c.readerMap[topic]; ok {
		if offset != entry.lastOffset {
			reader, err := leaderLog.ReaderFrom(offset)
			if err != nil {
				return nil, err
			}
			entry.reader = reader
			entry.lastOffset = offset
		}
		return entry.reader, nil
	}
	reader, err := leaderLog.ReaderFrom(offset)
	if err != nil {
		return nil, err
	}
	c.readerMap[topic] = &LeaderCacheEntry{
		reader:     reader,
		lastOffset: offset,
	}
	return reader, nil
}

func (c *LeaderReaderCache) UpdateLastOffset(topic string, offset uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.readerMap[topic]; ok {
		entry.lastOffset = offset
	}
}

func (s *RpcServer) handleReplicate(req *protocol.ReplicateRequest) (any, error) {
	leaderLog, err := s.topicManager.GetLeader(req.Topic)
	if err != nil {
		if topicObj, e := s.topicManager.GetTopic(req.Topic); e == nil && topicObj != nil && topicObj.Log != nil {
			leaderLog = topicObj.Log
			err = nil
		}
	}
	if err != nil {
		return nil, err
	}

	reader, err := s.leaderReaderCache.Get(req.Topic, req.Offset, leaderLog)

	if err != nil {
		return nil, err
	}

	var batch []protocol.ReplicationRecord

	var hitEOF bool
	var lastOffset uint64
	for len(batch) < int(req.BatchSize) {
		offset, value, err := protocol.ReadRecordFromStream(reader)
		fmt.Printf("handleReplicate: offset: %+v, value: %+v, err: %+v\n", offset, string(value), err)
		if err == io.EOF {
			hitEOF = true
			break
		}
		if err != nil {
			return nil, err
		}

		lastOffset = offset
		batch = append(batch, protocol.ReplicationRecord{Offset: int64(offset), Value: value})
	}
	s.leaderReaderCache.UpdateLastOffset(req.Topic, lastOffset)
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
