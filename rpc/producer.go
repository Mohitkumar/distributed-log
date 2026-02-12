package rpc

import (
	"context"

	"github.com/mohitkumar/mlog/protocol"
)

func (srv *RpcServer) Produce(ctx context.Context, req *protocol.ProduceRequest) (*protocol.ProduceResponse, error) {
	topicObj, err := srv.topicManager.GetTopic(req.Topic)
	if err != nil {
		return nil, ErrTopicNotFound(req.Topic, err)
	}
	isLeader, _ := srv.topicManager.IsLeader(req.Topic)
	if !isLeader {
		return nil, ErrNotTopicLeader
	}

	offset, err := srv.topicManager.HandleProduce(ctx, topicObj, &protocol.LogEntry{
		Value: req.Value,
	}, req.Acks)
	if err != nil {
		return nil, err
	}
	return &protocol.ProduceResponse{Offset: offset}, err
}

func (srv *RpcServer) ProduceBatch(ctx context.Context, req *protocol.ProduceBatchRequest) (*protocol.ProduceBatchResponse, error) {
	if req.Topic == "" {
		return nil, ErrTopicRequired
	}
	if len(req.Values) == 0 {
		return nil, ErrValuesRequired
	}

	topicObj, err := srv.topicManager.GetTopic(req.Topic)
	if err != nil {
		return nil, ErrTopicNotFound(req.Topic, err)
	}
	isLeader, _ := srv.topicManager.IsLeader(req.Topic)
	if !isLeader {
		return nil, ErrNotTopicLeader
	}

	base, last, err := srv.topicManager.HandleProduceBatch(ctx, topicObj, req.Values, req.Acks)
	if err != nil {
		return nil, err
	}
	return &protocol.ProduceBatchResponse{
		BaseOffset: base,
		LastOffset: last,
		Count:      uint32(len(req.Values)),
	}, nil
}
