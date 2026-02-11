package rpc

import (
	"context"
	"io"
	"sync"
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

// replicateReaderCache caches a log reader per (topic, replica). nextOffset is the offset of the next record to read.
// If req.Offset == nextOffset we reuse the reader; else we create a new reader at req.Offset.
type replicateReaderCache struct {
	mu      sync.Mutex
	entries map[string]*cachedReplicateReader
}

type cachedReplicateReader struct {
	reader     io.Reader
	nextOffset uint64 // offset of next record to read; updated after each batch
}

// get returns a reader for the given key and request offset. If cached reader's nextOffset matches, reuse it; else create at offset.
// Caller must call setNextOffset(lastReadOffset+1) after reading a batch so the next request can reuse.
func (c *replicateReaderCache) get(key string, offset uint64, create func() (io.Reader, error)) (reader io.Reader, setNextOffset func(uint64), err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[string]*cachedReplicateReader)
	}
	entry := c.entries[key]
	if entry != nil && entry.nextOffset == offset {
		setNext := func(next uint64) {
			c.mu.Lock()
			defer c.mu.Unlock()
			if e := c.entries[key]; e != nil {
				e.nextOffset = next
			}
		}
		return entry.reader, setNext, nil
	}
	reader, err = create()
	if err != nil {
		return nil, nil, err
	}
	c.entries[key] = &cachedReplicateReader{reader: reader, nextOffset: offset}
	setNext := func(next uint64) {
		c.mu.Lock()
		defer c.mu.Unlock()
		if e := c.entries[key]; e != nil {
			e.nextOffset = next
		}
	}
	return reader, setNext, nil
}

// handleReplicate handles one ReplicateRequest: returns one batch (or empty) and EndOfStream.
// Leader caches reader per (topic, replica); if request offset matches cached reader offset we reuse it, else reinit reader at request offset.
func (s *RpcServer) handleReplicate(ctx context.Context, req *protocol.ReplicateRequest) (any, error) {
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
	cacheKey := req.Topic + "\x00" + req.ReplicaNodeID
	reader, setNextOffset, err := s.replicateReaderCache.get(cacheKey, req.Offset, func() (io.Reader, error) {
		return leaderLog.ReaderFrom(req.Offset)
	})
	if err != nil {
		return nil, err
	}
	batchSize := req.BatchSize
	if batchSize == 0 {
		batchSize = 1000
	}
	var batch []protocol.ReplicationRecord
	var lastOffset uint64
	for len(batch) < int(batchSize) {
		offset, value, err := protocol.ReadRecordFromStream(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		lastOffset = offset
		batch = append(batch, protocol.ReplicationRecord{Offset: int64(offset), Value: value})
	}
	if len(batch) > 0 {
		setNextOffset(lastOffset + 1)
	}
	endOfStream := len(batch) == 0
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
