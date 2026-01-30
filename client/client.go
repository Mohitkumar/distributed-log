package client

import (
	"context"
	"fmt"
	"io"

	"github.com/mohitkumar/mlog/protocol"
	"github.com/mohitkumar/mlog/transport"
)

// ReplicationClient performs replication and leader RPCs over a transport connection.
type ReplicationClient struct {
	conn *transport.Conn
}

func NewReplicationClient(conn *transport.Conn) *ReplicationClient {
	return &ReplicationClient{conn: conn}
}

func (c *ReplicationClient) CreateReplica(ctx context.Context, req *protocol.CreateReplicaRequest) (*protocol.CreateReplicaResponse, error) {
	body, err := protocol.MarshalJSON(req)
	if err != nil {
		return nil, err
	}
	if err := c.conn.Send(protocol.EncodeRequest(protocol.MsgCreateReplica, body)); err != nil {
		return nil, err
	}
	respPayload, err := c.conn.Receive()
	if err != nil {
		return nil, err
	}
	msgType, body, err := protocol.DecodeRequest(respPayload)
	if err != nil {
		return nil, err
	}
	if msgType != protocol.MsgCreateReplicaResp {
		return nil, fmt.Errorf("client: unexpected response type %d", msgType)
	}
	resp := &protocol.CreateReplicaResponse{}
	if err := protocol.UnmarshalJSON(body, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *ReplicationClient) DeleteReplica(ctx context.Context, req *protocol.DeleteReplicaRequest) (*protocol.DeleteReplicaResponse, error) {
	body, err := protocol.MarshalJSON(req)
	if err != nil {
		return nil, err
	}
	if err := c.conn.Send(protocol.EncodeRequest(protocol.MsgDeleteReplica, body)); err != nil {
		return nil, err
	}
	respPayload, err := c.conn.Receive()
	if err != nil {
		return nil, err
	}
	msgType, body, err := protocol.DecodeRequest(respPayload)
	if err != nil {
		return nil, err
	}
	if msgType != protocol.MsgDeleteReplicaResp {
		return nil, fmt.Errorf("client: unexpected response type %d", msgType)
	}
	resp := &protocol.DeleteReplicaResponse{}
	if err := protocol.UnmarshalJSON(body, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *ReplicationClient) ReplicateStream(ctx context.Context, req *protocol.ReplicateRequest) (ReplicateStreamReader, error) {
	body, err := protocol.MarshalJSON(req)
	if err != nil {
		return nil, err
	}
	if err := c.conn.Send(protocol.EncodeRequest(protocol.MsgReplicateStream, body)); err != nil {
		return nil, err
	}
	return &replicateStreamReader{conn: c.conn}, nil
}

type ReplicateStreamReader interface {
	Recv() (*protocol.ReplicateResponse, error)
}

type replicateStreamReader struct {
	conn *transport.Conn
}

func (r *replicateStreamReader) Recv() (*protocol.ReplicateResponse, error) {
	respPayload, err := r.conn.Receive()
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, err
	}
	msgType, body, err := protocol.DecodeRequest(respPayload)
	if err != nil {
		return nil, err
	}
	if msgType != protocol.MsgReplicateResp {
		return nil, fmt.Errorf("client: unexpected stream response type %d", msgType)
	}
	resp := &protocol.ReplicateResponse{}
	if err := protocol.UnmarshalJSON(body, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *ReplicationClient) CreateTopic(ctx context.Context, req *protocol.CreateTopicRequest) (*protocol.CreateTopicResponse, error) {
	body, err := protocol.MarshalJSON(req)
	if err != nil {
		return nil, err
	}
	if err := c.conn.Send(protocol.EncodeRequest(protocol.MsgCreateTopic, body)); err != nil {
		return nil, err
	}
	respPayload, err := c.conn.Receive()
	if err != nil {
		return nil, err
	}
	msgType, body, err := protocol.DecodeRequest(respPayload)
	if err != nil {
		return nil, err
	}
	if msgType != protocol.MsgCreateTopicResp {
		return nil, fmt.Errorf("client: unexpected response type %d", msgType)
	}
	resp := &protocol.CreateTopicResponse{}
	if err := protocol.UnmarshalJSON(body, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *ReplicationClient) DeleteTopic(ctx context.Context, req *protocol.DeleteTopicRequest) (*protocol.DeleteTopicResponse, error) {
	body, err := protocol.MarshalJSON(req)
	if err != nil {
		return nil, err
	}
	if err := c.conn.Send(protocol.EncodeRequest(protocol.MsgDeleteTopic, body)); err != nil {
		return nil, err
	}
	respPayload, err := c.conn.Receive()
	if err != nil {
		return nil, err
	}
	msgType, body, err := protocol.DecodeRequest(respPayload)
	if err != nil {
		return nil, err
	}
	if msgType != protocol.MsgDeleteTopicResp {
		return nil, fmt.Errorf("client: unexpected response type %d", msgType)
	}
	resp := &protocol.DeleteTopicResponse{}
	if err := protocol.UnmarshalJSON(body, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *ReplicationClient) RecordLEO(ctx context.Context, req *protocol.RecordLEORequest) (*protocol.RecordLEOResponse, error) {
	body, err := protocol.MarshalJSON(req)
	if err != nil {
		return nil, err
	}
	if err := c.conn.Send(protocol.EncodeRequest(protocol.MsgRecordLEO, body)); err != nil {
		return nil, err
	}
	respPayload, err := c.conn.Receive()
	if err != nil {
		return nil, err
	}
	msgType, body, err := protocol.DecodeRequest(respPayload)
	if err != nil {
		return nil, err
	}
	if msgType != protocol.MsgRecordLEOResp {
		return nil, fmt.Errorf("client: unexpected response type %d", msgType)
	}
	resp := &protocol.RecordLEOResponse{}
	if err := protocol.UnmarshalJSON(body, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// ProducerClient performs produce RPCs over a transport connection.
type ProducerClient struct {
	conn *transport.Conn
}

func NewProducerClient(conn *transport.Conn) *ProducerClient {
	return &ProducerClient{conn: conn}
}

func (c *ProducerClient) Produce(ctx context.Context, req *protocol.ProduceRequest) (*protocol.ProduceResponse, error) {
	body, err := protocol.MarshalJSON(req)
	if err != nil {
		return nil, err
	}
	if err := c.conn.Send(protocol.EncodeRequest(protocol.MsgProduce, body)); err != nil {
		return nil, err
	}
	respPayload, err := c.conn.Receive()
	if err != nil {
		return nil, err
	}
	msgType, body, err := protocol.DecodeRequest(respPayload)
	if err != nil {
		return nil, err
	}
	if msgType != protocol.MsgProduceResp {
		return nil, fmt.Errorf("client: unexpected response type %d", msgType)
	}
	resp := &protocol.ProduceResponse{}
	if err := protocol.UnmarshalJSON(body, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *ProducerClient) ProduceBatch(ctx context.Context, req *protocol.ProduceBatchRequest) (*protocol.ProduceBatchResponse, error) {
	body, err := protocol.MarshalJSON(req)
	if err != nil {
		return nil, err
	}
	if err := c.conn.Send(protocol.EncodeRequest(protocol.MsgProduceBatch, body)); err != nil {
		return nil, err
	}
	respPayload, err := c.conn.Receive()
	if err != nil {
		return nil, err
	}
	msgType, body, err := protocol.DecodeRequest(respPayload)
	if err != nil {
		return nil, err
	}
	if msgType != protocol.MsgProduceBatchResp {
		return nil, fmt.Errorf("client: unexpected response type %d", msgType)
	}
	resp := &protocol.ProduceBatchResponse{}
	if err := protocol.UnmarshalJSON(body, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// ConsumerClient performs consumer RPCs over a transport connection.
type ConsumerClient struct {
	conn *transport.Conn
}

func NewConsumerClient(conn *transport.Conn) *ConsumerClient {
	return &ConsumerClient{conn: conn}
}

func (c *ConsumerClient) Fetch(ctx context.Context, req *protocol.FetchRequest) (*protocol.FetchResponse, error) {
	body, err := protocol.MarshalJSON(req)
	if err != nil {
		return nil, err
	}
	if err := c.conn.Send(protocol.EncodeRequest(protocol.MsgFetch, body)); err != nil {
		return nil, err
	}
	respPayload, err := c.conn.Receive()
	if err != nil {
		return nil, err
	}
	msgType, body, err := protocol.DecodeRequest(respPayload)
	if err != nil {
		return nil, err
	}
	if msgType != protocol.MsgFetchResp {
		return nil, fmt.Errorf("client: unexpected response type %d", msgType)
	}
	resp := &protocol.FetchResponse{}
	if err := protocol.UnmarshalJSON(body, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *ConsumerClient) FetchStream(ctx context.Context, req *protocol.FetchRequest) (FetchStreamReader, error) {
	body, err := protocol.MarshalJSON(req)
	if err != nil {
		return nil, err
	}
	if err := c.conn.Send(protocol.EncodeRequest(protocol.MsgFetchStream, body)); err != nil {
		return nil, err
	}
	return &fetchStreamReader{conn: c.conn}, nil
}

type FetchStreamReader interface {
	Recv() (*protocol.FetchResponse, error)
}

type fetchStreamReader struct {
	conn *transport.Conn
}

func (r *fetchStreamReader) Recv() (*protocol.FetchResponse, error) {
	respPayload, err := r.conn.Receive()
	if err != nil {
		return nil, err
	}
	msgType, body, err := protocol.DecodeRequest(respPayload)
	if err != nil {
		return nil, err
	}
	if msgType != protocol.MsgFetchStreamResp {
		return nil, fmt.Errorf("client: unexpected stream response type %d", msgType)
	}
	resp := &protocol.FetchResponse{}
	if err := protocol.UnmarshalJSON(body, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *ConsumerClient) CommitOffset(ctx context.Context, req *protocol.CommitOffsetRequest) (*protocol.CommitOffsetResponse, error) {
	body, err := protocol.MarshalJSON(req)
	if err != nil {
		return nil, err
	}
	if err := c.conn.Send(protocol.EncodeRequest(protocol.MsgCommitOffset, body)); err != nil {
		return nil, err
	}
	respPayload, err := c.conn.Receive()
	if err != nil {
		return nil, err
	}
	msgType, body, err := protocol.DecodeRequest(respPayload)
	if err != nil {
		return nil, err
	}
	if msgType != protocol.MsgCommitOffsetResp {
		return nil, fmt.Errorf("client: unexpected response type %d", msgType)
	}
	resp := &protocol.CommitOffsetResponse{}
	if err := protocol.UnmarshalJSON(body, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *ConsumerClient) FetchOffset(ctx context.Context, req *protocol.FetchOffsetRequest) (*protocol.FetchOffsetResponse, error) {
	body, err := protocol.MarshalJSON(req)
	if err != nil {
		return nil, err
	}
	if err := c.conn.Send(protocol.EncodeRequest(protocol.MsgFetchOffset, body)); err != nil {
		return nil, err
	}
	respPayload, err := c.conn.Receive()
	if err != nil {
		return nil, err
	}
	msgType, body, err := protocol.DecodeRequest(respPayload)
	if err != nil {
		return nil, err
	}
	if msgType != protocol.MsgFetchOffsetResp {
		return nil, fmt.Errorf("client: unexpected response type %d", msgType)
	}
	resp := &protocol.FetchOffsetResponse{}
	if err := protocol.UnmarshalJSON(body, resp); err != nil {
		return nil, err
	}
	return resp, nil
}
