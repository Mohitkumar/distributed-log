package consumer

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/mohitkumar/mlog/log"
)

type ConsumerManager struct {
	mu          sync.RWMutex
	offsetCache map[string]map[string]uint64
	offsetLog   *log.Log
	recoverOnce sync.Once
	recoverErr  error
}

func NewConsumerManager(baseDir string) (*ConsumerManager, error) {
	offsetLog, err := log.NewLog(filepath.Join(baseDir, "__consumer_offsets__.log"))
	if err != nil {
		return nil, err
	}
	cm := &ConsumerManager{
		offsetCache: make(map[string]map[string]uint64),
		offsetLog:   offsetLog,
	}
	// Recover offsets eagerly at startup instead of per-request.
	if err := cm.Recover(); err != nil {
		return nil, fmt.Errorf("consumer offset recovery: %w", err)
	}
	return cm, nil
}

func (c *ConsumerManager) CommitOffset(id string, topic string, offset uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.offsetCache[id]; !ok {
		c.offsetCache[id] = make(map[string]uint64)
	}
	c.offsetCache[id][topic] = offset
	_, err := c.offsetLog.Append([]byte(fmt.Sprintf("%s,%s,%d", id, topic, offset)))
	if err != nil {
		return err
	}
	return nil
}

func (c *ConsumerManager) GetOffset(id string, topic string) (uint64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	topicMap, ok := c.offsetCache[id]
	if !ok {
		return 0, ErrOffsetNotFoundForID(id, topic)
	}
	return topicMap[topic], nil
}

// Recover replays the offset log into the in-memory cache.
// It is safe to call multiple times; only the first call does work.
func (c *ConsumerManager) Recover() error {
	c.recoverOnce.Do(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		offset := c.offsetLog.LowestOffset()

		highestOffset := c.offsetLog.HighestOffset()
		for ; offset < highestOffset; offset++ {
			data, err := c.offsetLog.Read(offset)
			if err != nil {
				c.recoverErr = err
				return
			}
			parts := strings.Split(string(data), ",")
			if len(parts) != 3 {
				continue
			}
			off, err := strconv.ParseUint(parts[2], 10, 64)
			if err != nil {
				c.recoverErr = err
				return
			}
			if _, ok := c.offsetCache[parts[0]]; !ok {
				c.offsetCache[parts[0]] = make(map[string]uint64)
			}
			c.offsetCache[parts[0]][parts[1]] = off
		}
	})
	return c.recoverErr
}
