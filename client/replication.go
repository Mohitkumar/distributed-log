package client

import (
	"context"

	"github.com/mohitkumar/mlog/protocol"
	"github.com/mohitkumar/mlog/transport"
)

// ReplicationStreamClient is a dedicated client for the replication stream only.
// It sends one ReplicateRequest and then reads ReplicateResponse frames until EndOfStream.
// Use this for replication;
type ReplicationStreamClient struct {
	tc *transport.TransportClient
}

func NewReplicationStreamClient(addr string) (*ReplicationStreamClient, error) {
	tc, err := transport.Dial(addr)
	if err != nil {
		return nil, err
	}
	return &ReplicationStreamClient{tc: tc}, nil
}

// ReplicateStream sends one ReplicateRequest on the connection; the leader then streams raw bytes until EndOfStream.
// Call Recv() in a loop to read each ReplicateResponse (RawChunk + EndOfStream).
func (c *ReplicationStreamClient) ReplicateStream(ctx context.Context, req *protocol.ReplicateRequest) error {
	return c.tc.Write(*req)
}

// Recv reads the next ReplicateResponse from the stream. Call after ReplicateStream; loop until resp.EndOfStream.
func (c *ReplicationStreamClient) Recv() (*protocol.ReplicateResponse, error) {
	resp, err := c.tc.ReadResponse()
	if err != nil {
		return nil, err
	}
	rep := resp.(protocol.ReplicateResponse)
	return &rep, nil
}

// Close closes the underlying transport connection.
func (c *ReplicationStreamClient) Close() error {
	return c.tc.Close()
}

type RemoteClient struct {
	tc *transport.TransportClient
}

func NewRemoteClient(addr string) (*RemoteClient, error) {
	tc, err := transport.Dial(addr)
	if err != nil {
		return nil, err
	}
	return &RemoteClient{tc: tc}, nil
}

func (c *RemoteClient) Close() error {
	return c.tc.Close()
}

func (c *RemoteClient) CreateReplica(ctx context.Context, req *protocol.CreateReplicaRequest) (*protocol.CreateReplicaResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.CreateReplicaResponse)
	return &r, nil
}

func (c *RemoteClient) DeleteReplica(ctx context.Context, req *protocol.DeleteReplicaRequest) (*protocol.DeleteReplicaResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.DeleteReplicaResponse)
	return &r, nil
}

func (c *RemoteClient) CreateTopic(ctx context.Context, req *protocol.CreateTopicRequest) (*protocol.CreateTopicResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.CreateTopicResponse)
	return &r, nil
}

func (c *RemoteClient) DeleteTopic(ctx context.Context, req *protocol.DeleteTopicRequest) (*protocol.DeleteTopicResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.DeleteTopicResponse)
	return &r, nil
}

func (c *RemoteClient) RecordLEO(ctx context.Context, req *protocol.RecordLEORequest) (*protocol.RecordLEOResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.RecordLEOResponse)
	return &r, nil
}

func (c *RemoteClient) ApplyDeleteTopicEvent(ctx context.Context, req *protocol.ApplyDeleteTopicEventRequest) (*protocol.ApplyDeleteTopicEventResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.ApplyDeleteTopicEventResponse)
	return &r, nil
}

func (c *RemoteClient) ApplyIsrUpdateEvent(ctx context.Context, req *protocol.ApplyIsrUpdateEventRequest) (*protocol.ApplyIsrUpdateEventResponse, error) {
	resp, err := c.tc.Call(*req)
	if err != nil {
		return nil, err
	}
	r := resp.(protocol.ApplyIsrUpdateEventResponse)
	return &r, nil
}
