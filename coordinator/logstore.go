package coordinator

import (
	"encoding/json"
	"path/filepath"

	"github.com/hashicorp/raft"
	"github.com/mohitkumar/mlog/errs"
	"github.com/mohitkumar/mlog/log"
)

var _ raft.LogStore = (*logStore)(nil)

type logStore struct {
	*log.Log
}

func NewLogStore(dir string) (*logStore, error) {
	log, err := log.NewLog(filepath.Join(dir, "__metadata__.log"))
	if err != nil {
		return nil, err
	}
	return &logStore{
		Log: log,
	}, nil
}

// Raft uses 1-based log indices (first entry is index 1). Our log uses 0-based offsets.
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

// Segment format: each record is [offset 8 bytes][payload]. Read returns that whole slice.
const segmentOffsetPrefix = 8

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	if index < 1 {
		return errs.ErrRaftLogIndex(index)
	}
	offset := index - 1
	in, err := l.Read(offset)
	if err != nil {
		return err
	}
	if len(in) < segmentOffsetPrefix {
		return errs.ErrLogRecordTooShort(offset)
	}
	payload := in[segmentOffsetPrefix:]
	if err := json.Unmarshal(payload, out); err != nil {
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
	_, err = l.Append(data)
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

func (l *logStore) DeleteRange(min, max uint64) error {
	if max < 1 {
		return nil
	}
	return l.Truncate(max - 1)
}
