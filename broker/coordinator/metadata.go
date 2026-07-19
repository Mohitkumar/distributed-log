package coordinator

import (
	"github.com/mohitkumar/mlog/api/protocol"
)

type MetadataStore interface {
	Apply(ev *protocol.MetadataEvent) error
	Restore(data []byte) error
	Snapshot() ([]byte, error)
}
