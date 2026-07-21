package topic

import (
	"encoding/json"

	"github.com/mohitkumar/mlog/broker/log"
)

// GetLog returns the currently open log for this topic, or nil if not yet opened locally.
func (t *Topic) GetLog() *log.LogManager {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Log
}

// SetLog sets the open log for this topic.
func (t *Topic) SetLog(l *log.LogManager) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Log = l
}

// MarshalJSON locks t.mu so periodic/debug logging of a Topic can't race with a
// concurrent SetLog.
func (t *Topic) MarshalJSON() ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	type topicJSON struct {
		Name string `json:"name"`
	}
	return json.Marshal(topicJSON{Name: t.Name})
}
