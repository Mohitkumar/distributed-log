package client

import (
	"context"
	"errors"
	"strings"
)

var errNoConnection = errors.New("could not connect to any address")

// TryAddrs tries each address in addrs in order. For each address it creates a RemoteClient,
// calls fn(client), then closes the client. If fn returns a non-empty result and nil error,
// TryAddrs returns that result. If fn returns an error and ShouldReconnect(err) is true,
// it tries the next address. Otherwise it returns the error.
// Use this for discovery (FindRaftLeader, FindTopicLeader) so that after a node or leader
// failure, the next address in the list can be used.
func TryAddrs(ctx context.Context, addrs []string, fn func(*RemoteClient) (string, error)) (string, error) {
	var lastErr error
	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		c, err := NewRemoteClient(addr)
		if err != nil {
			lastErr = err
			continue
		}
		result, err := fn(c)
		_ = c.Close()
		if err == nil && result != "" {
			return result, nil
		}
		lastErr = err
		if err != nil && ShouldReconnect(err) {
			continue
		}
		return "", err
	}
	if lastErr != nil {
		return "", lastErr
	}
	return "", errNoConnection
}
