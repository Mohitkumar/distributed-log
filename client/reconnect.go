package client

import "github.com/mohitkumar/mlog/protocol"

// ShouldReconnect reports whether the caller should re-resolve the leader (e.g. FindTopicLeader
// or FindRaftLeader) and create a new ProducerClient or ConsumerClient to the returned address.
// Use after Produce, Fetch, or other RPC calls fail: if true, the connection is stale (leader
// change, topic not found, or network failure) and a new client to the current leader should
// be created and the operation retried.
func ShouldReconnect(err error) bool {
	return protocol.ShouldReconnect(err)
}
