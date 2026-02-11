package rpc

import (
	"context"

	consumermgr "github.com/mohitkumar/mlog/consumer"
	"github.com/mohitkumar/mlog/protocol"
	"github.com/mohitkumar/mlog/topic"
	"github.com/mohitkumar/mlog/transport"
)

// RpcServer holds topic manager and consumer manager for TCP transport RPCs.
type RpcServer struct {
	Addr                 string
	topicManager         *topic.TopicManager
	consumerManager      *consumermgr.ConsumerManager
	transport            *transport.Transport
	replicateReaderCache replicateReaderCache
}

func NewRpcServer(addr string, topicManager *topic.TopicManager, consumerManager *consumermgr.ConsumerManager) *RpcServer {
	srv := &RpcServer{
		Addr:            addr,
		topicManager:    topicManager,
		consumerManager: consumerManager,
		transport:       transport.NewTransport(),
	}
	srv.RegisterHandlers()
	return srv
}

// RegisterHandlers registers all RPC handlers on tr. Used by Start() and by tests that run the transport themselves.
func (s *RpcServer) RegisterHandlers() {
	// Replication / leader
	s.transport.RegisterHandler(protocol.MsgCreateReplica, func(ctx context.Context, req any) (any, error) {
		r := req.(protocol.CreateReplicaRequest)
		return s.CreateReplica(ctx, &r)
	})
	s.transport.RegisterHandler(protocol.MsgDeleteReplica, func(ctx context.Context, req any) (any, error) {
		r := req.(protocol.DeleteReplicaRequest)
		return s.DeleteReplica(ctx, &r)
	})
	s.transport.RegisterHandler(protocol.MsgRecordLEO, func(ctx context.Context, req any) (any, error) {
		r := req.(protocol.RecordLEORequest)
		return s.RecordLEO(ctx, &r)
	})
	s.transport.RegisterHandler(protocol.MsgReplicateStream, func(ctx context.Context, req any) (any, error) {
		r := req.(protocol.ReplicateRequest)
		return s.handleReplicate(ctx, &r)
	})
	// Producer
	s.transport.RegisterHandler(protocol.MsgProduce, func(ctx context.Context, req any) (any, error) {
		r := req.(protocol.ProduceRequest)
		return s.Produce(ctx, &r)
	})
	s.transport.RegisterHandler(protocol.MsgProduceBatch, func(ctx context.Context, req any) (any, error) {
		r := req.(protocol.ProduceBatchRequest)
		return s.ProduceBatch(ctx, &r)
	})
	// Consumer
	s.transport.RegisterHandler(protocol.MsgFetch, func(ctx context.Context, req any) (any, error) {
		r := req.(protocol.FetchRequest)
		return s.Fetch(ctx, &r)
	})
	s.transport.RegisterHandler(protocol.MsgCommitOffset, func(ctx context.Context, req any) (any, error) {
		r := req.(protocol.CommitOffsetRequest)
		return s.CommitOffset(ctx, &r)
	})
	s.transport.RegisterHandler(protocol.MsgFetchOffset, func(ctx context.Context, req any) (any, error) {
		r := req.(protocol.FetchOffsetRequest)
		return s.FetchOffset(ctx, &r)
	})
	// Topic
	s.transport.RegisterHandler(protocol.MsgCreateTopic, func(ctx context.Context, req any) (any, error) {
		r := req.(protocol.CreateTopicRequest)
		return s.CreateTopic(ctx, &r)
	})
	s.transport.RegisterHandler(protocol.MsgDeleteTopic, func(ctx context.Context, req any) (any, error) {
		r := req.(protocol.DeleteTopicRequest)
		return s.DeleteTopic(ctx, &r)
	})
	s.transport.RegisterHandler(protocol.MsgApplyDeleteTopicEvent, func(ctx context.Context, req any) (any, error) {
		r := req.(protocol.ApplyDeleteTopicEventRequest)
		return s.ApplyDeleteTopicEvent(ctx, &r)
	})
	s.transport.RegisterHandler(protocol.MsgApplyIsrUpdateEvent, func(ctx context.Context, req any) (any, error) {
		r := req.(protocol.ApplyIsrUpdateEventRequest)
		return s.ApplyIsrUpdateEvent(ctx, &r)
	})
}

func (s *RpcServer) Start() error {
	ln, err := s.transport.Listen(s.Addr)
	if err != nil {
		return err
	}
	s.Addr = s.transport.Addr()
	go s.transport.Serve(ln)
	return nil
}

func (s *RpcServer) Stop() error {
	return s.transport.Close()
}
