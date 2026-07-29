package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var errNoConnection = errors.New("could not connect to any address")

// TryAddrs tries each address in addrs in order. For each address it creates a RemoteClient,
// calls fn(client), then closes the client. If fn returns a non-empty result and nil error,
// TryAddrs returns that result. An empty result with a nil error is treated the same as an
// error (fn's contract is "non-empty result on success") — it moves on to the next address
// rather than returning ("", nil), which would look like success to a caller that only
// checks err. If fn returns a real error and ShouldReconnect(err) is true, it also tries the
// next address; otherwise it returns the error.
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
		if err == nil {
			err = fmt.Errorf("empty result returned from %s", addr)
			lastErr = err
			continue
		}
		lastErr = err
		if ShouldReconnect(err) {
			continue
		}
		return "", err
	}
	if lastErr != nil {
		return "", lastErr
	}
	return "", errNoConnection
}
