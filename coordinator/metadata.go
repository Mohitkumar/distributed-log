package coordinator

import (
	"github.com/mohitkumar/mlog/protocol"
)

type MetadataStore interface {
	Apply(ev *protocol.MetadataEvent) error
	Restore(data []byte) error
}
