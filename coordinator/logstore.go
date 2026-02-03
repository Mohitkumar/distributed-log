package coordinator

import (
	"encoding/json"

	"github.com/hashicorp/raft"
	"github.com/mohitkumar/mlog/log"
	"github.com/mohitkumar/mlog/segment"
)

var _ raft.LogStore = (*logStore)(nil)

type logStore struct {
	*log.Log
}

func NewLogStore(dir string) (*logStore, error) {
	log, err := log.NewLog(dir + "__metadata__.log")
	if err != nil {
		return nil, err
	}
	return &logStore{
		Log: log,
	}, nil
}

// Raft uses 1-based log indices (first entry is index 1). Our log uses 0-based offsets.
// Map: raft_index = our_offset + 1.

func (l *logStore) FirstIndex() (uint64, error) {
	if l.IsEmpty() {
		return 0, nil
	}
	return l.LowestOffset() + 1, nil
}

func (l *logStore) LastIndex() (uint64, error) {
	if l.IsEmpty() {
		return 0, nil
	}
	return l.HighestOffset() + 1, nil
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	if index < 1 {
		return ErrRaftLogIndex(index)
	}
	offset := index - 1
	in, err := l.Read(offset)
	if err != nil {
		return err
	}
	// Segment stores [8 offset][4 len][value]; Segment.Read returns [8 offset][payload]
	// where payload is what we Appended. StoreLog appends segment.Encode(entry) = [8][4][json],
	// so payload is [8][4][json]. Skip 8 (segment header) + 12 (Encode header) = 20 to get the JSON.
	if len(in) < 20 {
		return ErrLogRecordTooShort(offset)
	}
	value := in[20:]
	if err := json.Unmarshal(value, out); err != nil {
		return err
	}
	out.Index = index
	return nil
}

func (l *logStore) StoreLog(log *raft.Log) error {
	data, err := json.Marshal(log)
	if err != nil {
		return err
	}
	entry := &segment.LogEntry{
		Offset: log.Index,
		Value:  data,
	}
	encoded, err := segment.Encode(entry)
	if err != nil {
		return err
	}
	_, err = l.Append(encoded)
	if err != nil {
		return err
	}
	return nil
}

func (l *logStore) StoreLogs(logs []*raft.Log) error {
	for _, log := range logs {
		if err := l.StoreLog(log); err != nil {
			return err
		}
	}
	return nil
}

// DeleteRange deletes Raft log entries [min, max] inclusive.
// Our log uses 0-based offsets (raft_index = our_offset + 1), so we truncate from the start
// to remove our offsets (min-1)..(max-1), i.e. keep our offset >= max.
func (l *logStore) DeleteRange(min, max uint64) error {
	if max < 1 {
		return nil
	}
	return l.Truncate(max - 1)
}
