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

func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset(), nil
}

func (l *logStore) LastIndex() (uint64, error) {
	return l.HighestOffset(), nil
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	in, err := l.Read(index)
	if err != nil {
		return err
	}
	entry, err := segment.Decode(in)
	if err != nil {
		return err
	}
	json.Unmarshal(entry.Value, out)
	out.Index = entry.Offset
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

func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Truncate(max)
}
