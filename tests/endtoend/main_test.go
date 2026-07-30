package endtoend

import (
	"os"
	"testing"

	"github.com/mohitkumar/mlog/tests"
)

// TestMain claims ports 25000+ for this package — see tests/integration/main_test.go
// for why each subpackage needs a distinct base.
func TestMain(m *testing.M) {
	tests.SetPortBase(25000)
	os.Exit(m.Run())
}
