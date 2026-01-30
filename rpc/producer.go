package rpc

import (
	"context"
	"fmt"

	"github.com/mohitkumar/mlog/protocol"
)

func (srv *rpcServer) Produce(ctx context.Context, req *protocol.ProduceRequest) (*protocol.ProduceResponse, error) {
	topicObj, err := srv.topicManager.GetTopic(req.Topic)
	if err != nil {
		return nil, fmt.Errorf("topic %s not found: %w", req.Topic, err)
	}

	offset, err := topicObj.HandleProduce(ctx, &protocol.LogEntry{
		Value: req.Value,
	}, req.Acks)
	if err != nil {
		return nil, err
	}
	return &protocol.ProduceResponse{Offset: offset}, err
}

func (srv *rpcServer) ProduceBatch(ctx context.Context, req *protocol.ProduceBatchRequest) (*protocol.ProduceBatchResponse, error) {
	if req.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}
	if len(req.Values) == 0 {
		return nil, fmt.Errorf("values are required")
	}

	topicObj, err := srv.topicManager.GetTopic(req.Topic)
	if err != nil {
		return nil, fmt.Errorf("topic %s not found: %w", req.Topic, err)
	}

	base, last, err := topicObj.HandleProduceBatch(ctx, req.Values, req.Acks)
	if err != nil {
		return nil, err
	}
	return &protocol.ProduceBatchResponse{
		BaseOffset: base,
		LastOffset: last,
		Count:      uint32(len(req.Values)),
	}, nil
}
