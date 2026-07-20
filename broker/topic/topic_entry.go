package topic

import (
	"encoding/json"

	"github.com/mohitkumar/mlog/broker/log"
)

// This file holds Topic's own thread-safe accessors and mutators. Topic.mu guards
// LeaderNodeID, LeaderEpoch, Replicas and Log; TopicManager.mu guards only the
// Topics/Nodes maps themselves (existence, not per-topic state). Any code that
// reads or writes a Topic's fields — inside this package or out — must go through
// these methods rather than touching the fields directly, or the field is left
// unprotected against concurrent Fetch/Apply/produce paths that only take Topic.mu.

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

// LeaderID returns the current leader node ID.
func (t *Topic) LeaderID() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.LeaderNodeID
}

// SetLeader updates the leader and epoch, removes nodeID from Replicas (a leader isn't
// also tracked as a replica), and returns the previous leader node ID.
func (t *Topic) SetLeader(nodeID string, epoch int64) (oldLeaderID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	oldLeaderID = t.LeaderNodeID
	t.LeaderNodeID = nodeID
	t.LeaderEpoch = epoch
	delete(t.Replicas, nodeID)
	return oldLeaderID
}

// ReplicaSnapshot is a point-in-time copy of one replica's state.
type ReplicaSnapshot struct {
	ReplicaNodeID string
	LEO           int64
	IsISR         bool
}

// Snapshot returns a point-in-time copy of the topic's leader/epoch/replica state.
func (t *Topic) Snapshot() (leaderNodeID string, epoch int64, replicas []ReplicaSnapshot) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	replicas = make([]ReplicaSnapshot, 0, len(t.Replicas))
	for _, r := range t.Replicas {
		if r != nil {
			replicas = append(replicas, ReplicaSnapshot{ReplicaNodeID: r.ReplicaNodeID, LEO: r.LEO, IsISR: r.IsISR})
		}
	}
	return t.LeaderNodeID, t.LeaderEpoch, replicas
}

// ReplicaCount returns the number of tracked replicas.
func (t *Topic) ReplicaCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.Replicas)
}

// HasReplica reports whether nodeID is a tracked replica.
func (t *Topic) HasReplica(nodeID string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.Replicas[nodeID]
	return ok
}

// AddReplicaIfAbsent adds nodeID as a replica with the given ISR status if not already tracked.
func (t *Topic) AddReplicaIfAbsent(nodeID string, isr bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.Replicas == nil {
		t.Replicas = make(map[string]*ReplicaState)
	}
	if _, ok := t.Replicas[nodeID]; ok {
		return
	}
	t.Replicas[nodeID] = &ReplicaState{ReplicaNodeID: nodeID, LEO: 0, IsISR: isr}
}

// SetReplicaISR sets nodeID's ISR flag (creating the entry with LEO 0 if absent) and
// advances the high watermark accordingly.
func (t *Topic) SetReplicaISR(nodeID string, isr bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.Replicas == nil {
		t.Replicas = make(map[string]*ReplicaState)
	}
	rs := t.Replicas[nodeID]
	if rs == nil {
		t.Replicas[nodeID] = &ReplicaState{ReplicaNodeID: nodeID, LEO: 0, IsISR: isr}
	} else {
		rs.IsISR = isr
	}
	t.advanceHWLocked()
}

// RecordReplicaFetch updates nodeID's LEO from a Fetch call, recomputes its ISR status against
// lagThreshold, advances the high watermark, and returns the replica's new ISR status.
func (t *Topic) RecordReplicaFetch(nodeID string, leo int64, lagThreshold uint64) (isr bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	leaderLEO := uint64(0)
	if t.Log != nil {
		leaderLEO = t.Log.LEO()
	}
	if t.Replicas == nil {
		t.Replicas = make(map[string]*ReplicaState)
	}
	rs := t.Replicas[nodeID]
	if rs == nil {
		rs = &ReplicaState{ReplicaNodeID: nodeID, LEO: leo}
		t.Replicas[nodeID] = rs
	} else {
		rs.LEO = leo
	}
	if leaderLEO > lagThreshold {
		isr = uint64(leo) >= leaderLEO-lagThreshold
	} else {
		isr = leo >= 0 // all replicas are in-sync for small topics
	}
	rs.IsISR = isr
	t.advanceHWLocked()
	return isr
}

// AdvanceHW recomputes HW = min(local LEO, all ISR replicas' LEO) and applies it.
func (t *Topic) AdvanceHW() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.advanceHWLocked()
}

// advanceHWLocked is AdvanceHW's body. Non-ISR replicas are excluded — a dead/lagging
// replica must not hold back consumer visibility. Caller must hold t.mu.
func (t *Topic) advanceHWLocked() {
	if t.Log == nil {
		return
	}
	minOffset := t.Log.LEO()
	for _, r := range t.Replicas {
		if r != nil && r.IsISR && uint64(r.LEO) < minOffset {
			minOffset = uint64(r.LEO)
		}
	}
	t.Log.SetHighWatermark(minOffset)
}

// MarshalJSON locks t.mu so periodic/debug logging of a Topic (including via the
// enclosing TopicManager's reflection-based json.Marshal) can't race with concurrent
// field mutation from RecordReplicaFetch/SetLeader/etc.
func (t *Topic) MarshalJSON() ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	type topicJSON struct {
		Name                string                   `json:"name"`
		LeaderNodeID        string                   `json:"leader_id"`
		LeaderEpoch         int64                    `json:"leader_epoch"`
		DesiredReplicaCount int                      `json:"desired_replica_count"`
		Replicas            map[string]*ReplicaState `json:"replicas"`
	}
	return json.Marshal(topicJSON{
		Name:                t.Name,
		LeaderNodeID:        t.LeaderNodeID,
		LeaderEpoch:         t.LeaderEpoch,
		DesiredReplicaCount: t.DesiredReplicaCount,
		Replicas:            t.Replicas,
	})
}
