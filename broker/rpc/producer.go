package rpc

import (
	"context"
	"fmt"

	"github.com/mohitkumar/mlog/api/protocol"
)

func (srv *RpcServer) Produce(ctx context.Context, req *protocol.ProduceRequest) (*protocol.ProduceResponse, error) {
	if req.Topic == "" {
		return nil, Err(protocol.CodeTopicRequired, "topic is required")
	}
	if len(req.Value) == 0 {
		return nil, Err(protocol.CodeValuesRequired, "value is required")
	}
	l, err := srv.topicManager.GetLog(req.Topic)
	if err != nil {
		return nil, &protocol.RPCError{Code: protocol.CodeTopicNotFound, Message: fmt.Sprintf("topic %s not found: %v", req.Topic, err)}
	}
	isLeader, _ := srv.topicManager.IsLeader(req.Topic)
	if !isLeader {
		return nil, Err(protocol.CodeNotTopicLeader, "this node is not the topic leader; produce to the topic leader")
	}

	offset, err := srv.topicManager.HandleProduce(ctx, req.Topic, l, &protocol.LogEntry{
		Value: req.Value,
	}, req.Acks)
	if err != nil {
		return nil, FromError(err)
	}
	return &protocol.ProduceResponse{Offset: offset}, nil
}

func (srv *RpcServer) ProduceBatch(ctx context.Context, req *protocol.ProduceBatchRequest) (*protocol.ProduceBatchResponse, error) {
	if req.Topic == "" {
		return nil, Err(protocol.CodeTopicRequired, "topic is required")
	}
	if len(req.Values) == 0 {
		return nil, Err(protocol.CodeValuesRequired, "values are required")
	}

	l, err := srv.topicManager.GetLog(req.Topic)
	if err != nil {
		return nil, &protocol.RPCError{Code: protocol.CodeTopicNotFound, Message: fmt.Sprintf("topic %s not found: %v", req.Topic, err)}
	}
	isLeader, _ := srv.topicManager.IsLeader(req.Topic)
	if !isLeader {
		return nil, Err(protocol.CodeNotTopicLeader, "this node is not the topic leader; produce to the topic leader")
	}

	base, last, err := srv.topicManager.HandleProduceBatch(ctx, req.Topic, l, req.Values, req.Acks)
	if err != nil {
		return nil, FromError(err)
	}
	return &protocol.ProduceBatchResponse{
		BaseOffset: base,
		LastOffset: last,
		Count:      uint32(len(req.Values)),
	}, nil
}
