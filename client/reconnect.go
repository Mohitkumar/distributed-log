package client

import "github.com/mohitkumar/mlog/protocol"

// ShouldReconnect reports whether the caller should re-resolve the leader and create a new client.
// Use after Produce, Fetch, or other RPC calls fail; if true, reconnect to the current leader and retry.
func ShouldReconnect(err error) bool {
	return protocol.ShouldReconnect(err)
}
