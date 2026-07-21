package partition

import "github.com/mohitkumar/mlog/broker/log"

type Partition struct {
	ID           uint64          `json:"id"`
	LeaderNodeID string          `json:"leader_id"`
	LeaderEpoch  int64           `json:"leader_epoch"`
	Topic        string          `json:"topic"`
	Log          *log.LogManager `json:"-"`
}
