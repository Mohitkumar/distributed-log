package coordinator

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/mohitkumar/mlog/protocol"
)

var _ raft.FSM = (*MetadataFSM)(nil)

type MetadataFSM struct {
	mu            sync.RWMutex
	metadataStore MetadataStore
	BaseDir       string
}

func NewCoordinatorFSM(baseDir string, metadataStore MetadataStore) (*MetadataFSM, error) {
	return &MetadataFSM{
		metadataStore: metadataStore,
		BaseDir:       baseDir,
	}, nil
}

func (c *MetadataFSM) Apply(l *raft.Log) interface{} {
	var metadataEvent protocol.MetadataEvent
	if err := json.Unmarshal(l.Data, &metadataEvent); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.metadataStore.Apply(&metadataEvent)
}

// Snapshot serializes the metadata store under the lock so Persist() works on a
// frozen copy and is safe to call without holding the FSM lock.
func (c *MetadataFSM) Snapshot() (raft.FSMSnapshot, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	data, err := json.Marshal(c.metadataStore)
	if err != nil {
		return nil, err
	}
	return &metadataSnapshot{data: data}, nil
}

func (c *MetadataFSM) Restore(r io.ReadCloser) error {
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return c.metadataStore.Restore(data)
}

var _ raft.FSMSnapshot = (*metadataSnapshot)(nil)

// metadataSnapshot holds a frozen byte slice captured at snapshot time.
type metadataSnapshot struct {
	data []byte
}

func (s *metadataSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *metadataSnapshot) Release() {}
