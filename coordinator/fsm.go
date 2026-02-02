package coordinator

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/mohitkumar/mlog/protocol"
	"github.com/mohitkumar/mlog/raftmeta"
)

var _ raft.FSM = (*MetadataFSM)(nil)

type MetadataFSM struct {
	mu            sync.RWMutex
	MetadataStore *raftmeta.MetadataStore
	BaseDir       string
}

func NewCoordinatorFSM(baseDir string) (*MetadataFSM, error) {
	return &MetadataFSM{
		MetadataStore: raftmeta.NewMetadataStore(),
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
	c.MetadataStore.Apply(&metadataEvent)
	return nil
}

func (c *MetadataFSM) Snapshot() (raft.FSMSnapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return &metadataSnapshot{
		metadataStore: c.MetadataStore,
	}, nil
}

func (c *MetadataFSM) Restore(r io.ReadCloser) error {
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	var metadataStore raftmeta.MetadataStore
	if err := json.Unmarshal(data, &metadataStore); err != nil {
		return err
	}
	c.MetadataStore = &metadataStore
	return nil
}

var _ raft.FSMSnapshot = (*metadataSnapshot)(nil)

type metadataSnapshot struct {
	metadataStore *raftmeta.MetadataStore
}

func (c *metadataSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode and write state to the sink
		b, err := json.Marshal(c.metadataStore)
		if err != nil {
			return err
		}
		if _, err := sink.Write(b); err != nil {
			return err
		}
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}
	return err
}

func (c *metadataSnapshot) Release() {}
