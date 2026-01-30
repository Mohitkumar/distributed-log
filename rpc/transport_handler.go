package rpc

import (
	"context"
	"time"

	"github.com/mohitkumar/mlog/protocol"
	"github.com/mohitkumar/mlog/transport"
)

// ServeTransportConn handles replication and leader RPCs on a single transport connection.
// It runs until the connection is closed or an error occurs.
func (s *rpcServer) ServeTransportConn(conn *transport.Conn) {
	defer conn.Close()
	ctx := context.Background()

	for {
		payload, err := conn.Receive()
		if err != nil {
			return
		}
		msgType, body, err := protocol.DecodeRequest(payload)
		if err != nil {
			return
		}

		switch msgType {
		case protocol.MsgCreateReplica:
			req := &protocol.CreateReplicaRequest{}
			if err := protocol.UnmarshalJSON(body, req); err != nil {
				return
			}
			resp, err := s.CreateReplica(ctx, req)
			if err != nil {
				return
			}
			respBody, _ := protocol.MarshalJSON(resp)
			if conn.Send(protocol.EncodeResponse(protocol.MsgCreateReplicaResp, respBody)) != nil {
				return
			}

		case protocol.MsgDeleteReplica:
			req := &protocol.DeleteReplicaRequest{}
			if err := protocol.UnmarshalJSON(body, req); err != nil {
				return
			}
			resp, err := s.DeleteReplica(ctx, req)
			if err != nil {
				return
			}
			respBody, _ := protocol.MarshalJSON(resp)
			if conn.Send(protocol.EncodeResponse(protocol.MsgDeleteReplicaResp, respBody)) != nil {
				return
			}

		case protocol.MsgRecordLEO:
			req := &protocol.RecordLEORequest{}
			if err := protocol.UnmarshalJSON(body, req); err != nil {
				return
			}
			resp, err := s.RecordLEO(ctx, req)
			if err != nil {
				return
			}
			respBody, _ := protocol.MarshalJSON(resp)
			if conn.Send(protocol.EncodeResponse(protocol.MsgRecordLEOResp, respBody)) != nil {
				return
			}

		case protocol.MsgReplicateStream:
			req := &protocol.ReplicateRequest{}
			if err := protocol.UnmarshalJSON(body, req); err != nil {
				return
			}
			s.serveReplicateStream(conn, req)
			return // stream consumes connection

		case protocol.MsgProduce:
			req := &protocol.ProduceRequest{}
			if err := protocol.UnmarshalJSON(body, req); err != nil {
				return
			}
			resp, err := s.Produce(ctx, req)
			if err != nil {
				return
			}
			respBody, _ := protocol.MarshalJSON(resp)
			if conn.Send(protocol.EncodeResponse(protocol.MsgProduceResp, respBody)) != nil {
				return
			}

		case protocol.MsgProduceBatch:
			req := &protocol.ProduceBatchRequest{}
			if err := protocol.UnmarshalJSON(body, req); err != nil {
				return
			}
			resp, err := s.ProduceBatch(ctx, req)
			if err != nil {
				return
			}
			respBody, _ := protocol.MarshalJSON(resp)
			if conn.Send(protocol.EncodeResponse(protocol.MsgProduceBatchResp, respBody)) != nil {
				return
			}

		case protocol.MsgFetch:
			req := &protocol.FetchRequest{}
			if err := protocol.UnmarshalJSON(body, req); err != nil {
				return
			}
			resp, err := s.Fetch(ctx, req)
			if err != nil {
				return
			}
			respBody, _ := protocol.MarshalJSON(resp)
			if conn.Send(protocol.EncodeResponse(protocol.MsgFetchResp, respBody)) != nil {
				return
			}

		case protocol.MsgFetchStream:
			req := &protocol.FetchRequest{}
			if err := protocol.UnmarshalJSON(body, req); err != nil {
				return
			}
			s.serveFetchStream(conn, req)
			return

		case protocol.MsgCommitOffset:
			req := &protocol.CommitOffsetRequest{}
			if err := protocol.UnmarshalJSON(body, req); err != nil {
				return
			}
			resp, err := s.CommitOffset(ctx, req)
			if err != nil {
				return
			}
			respBody, _ := protocol.MarshalJSON(resp)
			if conn.Send(protocol.EncodeResponse(protocol.MsgCommitOffsetResp, respBody)) != nil {
				return
			}

		case protocol.MsgFetchOffset:
			req := &protocol.FetchOffsetRequest{}
			if err := protocol.UnmarshalJSON(body, req); err != nil {
				return
			}
			resp, err := s.FetchOffset(ctx, req)
			if err != nil {
				return
			}
			respBody, _ := protocol.MarshalJSON(resp)
			if conn.Send(protocol.EncodeResponse(protocol.MsgFetchOffsetResp, respBody)) != nil {
				return
			}

		case protocol.MsgCreateTopic:
			req := &protocol.CreateTopicRequest{}
			if err := protocol.UnmarshalJSON(body, req); err != nil {
				return
			}
			resp, err := s.CreateTopic(ctx, req)
			if err != nil {
				return
			}
			respBody, _ := protocol.MarshalJSON(resp)
			if conn.Send(protocol.EncodeResponse(protocol.MsgCreateTopicResp, respBody)) != nil {
				return
			}

		case protocol.MsgDeleteTopic:
			req := &protocol.DeleteTopicRequest{}
			if err := protocol.UnmarshalJSON(body, req); err != nil {
				return
			}
			resp, err := s.DeleteTopic(ctx, req)
			if err != nil {
				return
			}
			respBody, _ := protocol.MarshalJSON(resp)
			if conn.Send(protocol.EncodeResponse(protocol.MsgDeleteTopicResp, respBody)) != nil {
				return
			}

		default:
			return
		}
	}
}

func (s *rpcServer) serveFetchStream(conn *transport.Conn, req *protocol.FetchRequest) {
	send := func(resp *protocol.FetchResponse) error {
		respBody, _ := protocol.MarshalJSON(resp)
		return conn.Send(protocol.EncodeResponse(protocol.MsgFetchStreamResp, respBody))
	}
	_ = s.FetchStream(req, send)
}

// serveReplicateStream runs the replication stream loop, sending ReplicateResponse frames.
func (s *rpcServer) serveReplicateStream(conn *transport.Conn, req *protocol.ReplicateRequest) {
	leaderNode, err := s.topicManager.GetLeader(req.Topic)
	if err != nil {
		return
	}
	batchSize := req.BatchSize
	if batchSize == 0 {
		batchSize = 1000
	}
	currentOffset := req.Offset
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		endExclusive := leaderNode.Log.LEO()
		if currentOffset < endExclusive {
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
			if len(entries) > 0 {
				resp := &protocol.ReplicateResponse{
					LastOffset: currentOffset - 1,
					Entries:    entries,
				}
				respBody, err := protocol.MarshalJSON(resp)
				if err != nil {
					return
				}
				if err := conn.Send(protocol.EncodeResponse(protocol.MsgReplicateResp, respBody)); err != nil {
					return
				}
			}
			continue
		}
		<-ticker.C
	}
}
