package integration

import (
	"os"
	"testing"

	"github.com/mohitkumar/mlog/tests"
)

// TestMain claims ports 15000+ for this package. tests/integration, tests/endtoend, and
// tests/chaos are separate `go test` processes and can run concurrently under
// `go test ./...` — each gets its own distinct base so they never race to bind the same
// 127.0.0.1:<port> (see tests.SetPortBase).
func TestMain(m *testing.M) {
	tests.SetPortBase(15000)
	os.Exit(m.Run())
}
