//go:build chaos

package chaos

import (
	"os"
	"testing"

	"github.com/mohitkumar/mlog/tests"
)

// TestMain claims ports 35000+ for this package — see tests/integration/main_test.go
// for why each subpackage needs a distinct base.
func TestMain(m *testing.M) {
	tests.SetPortBase(35000)
	os.Exit(m.Run())
}
