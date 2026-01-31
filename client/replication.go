package client

import (
	"context"
	"io"

	"github.com/mohitkumar/mlog/broker"
	"github.com/mohitkumar/mlog/protocol"
)

// ReplicationClient performs replication and leader RPCs via a broker (used within the cluster).
type ReplicationClient struct {
	b *broker.Broker
}

// NewReplicationClient returns a client that uses the broker's transport (shared connection).
func NewReplicationClient(b *broker.Broker) *ReplicationClient {
	return &ReplicationClient{b: b}
}

func (c *ReplicationClient) CreateReplica(ctx context.Context, req *protocol.CreateReplicaRequest) (*protocol.CreateReplicaResponse, error) {
	tc, err := c.b.GetClient()
	if err != nil {
		return nil, err
	}
	resp, err := tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.CreateReplicaResponse)
	return &r, nil
}

func (c *ReplicationClient) DeleteReplica(ctx context.Context, req *protocol.DeleteReplicaRequest) (*protocol.DeleteReplicaResponse, error) {
	tc, err := c.b.GetClient()
	if err != nil {
		return nil, err
	}
	resp, err := tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.DeleteReplicaResponse)
	return &r, nil
}

// ReplicateStream returns a reader that fetches batches via repeated Replicate RPCs.
func (c *ReplicationClient) ReplicateStream(ctx context.Context, req *protocol.ReplicateRequest) (ReplicateStreamReader, error) {
	return &replicateStreamReader{b: c.b, req: *req}, nil
}

// ReplicateStreamReader reads ReplicateResponse messages.
type ReplicateStreamReader interface {
	Recv() (*protocol.ReplicateResponse, error)
}

type replicateStreamReader struct {
	b   *broker.Broker
	req protocol.ReplicateRequest
}

func (r *replicateStreamReader) Recv() (*protocol.ReplicateResponse, error) {
	tc, err := r.b.GetClient()
	if err != nil {
		return nil, err
	}
	resp, err := tc.Call(r.req)
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, err
	}
	rep := resp.(protocol.ReplicateResponse)
	out := &rep
	if len(rep.Entries) > 0 {
		r.req.Offset = rep.LastOffset + 1
	}
	return out, nil
}

func (c *ReplicationClient) CreateTopic(ctx context.Context, req *protocol.CreateTopicRequest) (*protocol.CreateTopicResponse, error) {
	tc, err := c.b.GetClient()
	if err != nil {
		return nil, err
	}
	resp, err := tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.CreateTopicResponse)
	return &r, nil
}

func (c *ReplicationClient) DeleteTopic(ctx context.Context, req *protocol.DeleteTopicRequest) (*protocol.DeleteTopicResponse, error) {
	tc, err := c.b.GetClient()
	if err != nil {
		return nil, err
	}
	resp, err := tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.DeleteTopicResponse)
	return &r, nil
}

func (c *ReplicationClient) RecordLEO(ctx context.Context, req *protocol.RecordLEORequest) (*protocol.RecordLEOResponse, error) {
	tc, err := c.b.GetClient()
	if err != nil {
		return nil, err
	}
	resp, err := tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.RecordLEOResponse)
	return &r, nil
}
