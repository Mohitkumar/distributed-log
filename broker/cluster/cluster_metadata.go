package cluster

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/mohitkumar/mlog/api/protocol/pb"
	raft "github.com/mohitkumar/mlog/broker/cluster/raft"
	"google.golang.org/protobuf/proto"
)

var _ raft.MetadataStore = (*ClusterMetadataStore)(nil)

// ReplicaState is one replica's view within a topic. IsISR is Raft-replicated
// (changed only via Apply, so it's identical on every node). LEO is written
// locally (via RecordReplicaFetch, not through Raft) by whichever node
// currently leads the topic when it serves a Fetch from that replica — it is
// not part of the agreed cluster state, only a convenience cache, and is
// meaningless/stale on nodes that aren't currently leading the topic.
type ReplicaState struct {
	ReplicaNodeID string `json:"replica_id"`
	LEO           int64  `json:"leo"`
	IsISR         bool   `json:"is_isr"`
}

// ReplicaSnapshot is a point-in-time copy of one replica's state.
type ReplicaSnapshot struct {
	ReplicaNodeID string
	LEO           int64
	IsISR         bool
}

// TopicMetadata is the cluster-wide (Raft-replicated) view of one topic: its
// current leader, epoch, and replica set. This is the equivalent of a Kafka
// partition's controller-side registration — every broker holds an identical
// copy, kept in sync purely by applying Raft-committed events. It deliberately
// holds no log handle: the actual on-disk log is a per-broker runtime concern
// owned by topic.TopicManager, not cluster metadata.
type TopicMetadata struct {
	mu                  sync.RWMutex
	Name                string
	LeaderNodeID        string
	LeaderEpoch         int64
	DesiredReplicaCount int // from CreateTopic; used to re-add replicas when nodes rejoin
	Replicas            map[string]*ReplicaState
}

func newTopicMetadata(name, leaderNodeID string, leaderEpoch int64, replicaNodeIds []string) *TopicMetadata {
	t := &TopicMetadata{
		Name:                name,
		LeaderNodeID:        leaderNodeID,
		LeaderEpoch:         leaderEpoch,
		DesiredReplicaCount: len(replicaNodeIds),
		Replicas:            make(map[string]*ReplicaState),
	}
	for _, id := range replicaNodeIds {
		t.Replicas[id] = &ReplicaState{ReplicaNodeID: id, LEO: 0, IsISR: true}
	}
	return t
}

// LeaderID returns the current leader node ID.
func (t *TopicMetadata) LeaderID() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.LeaderNodeID
}

// SetLeader updates the leader and epoch, removes nodeID from Replicas (a
// leader isn't also tracked as a replica), and returns the previous leader.
func (t *TopicMetadata) SetLeader(nodeID string, epoch int64) (oldLeaderID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	oldLeaderID = t.LeaderNodeID
	t.LeaderNodeID = nodeID
	t.LeaderEpoch = epoch
	delete(t.Replicas, nodeID)
	return oldLeaderID
}

// Snapshot returns a point-in-time copy of the topic's leader/epoch/replica state.
func (t *TopicMetadata) Snapshot() (leaderNodeID string, epoch int64, replicas []ReplicaSnapshot) {
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
func (t *TopicMetadata) ReplicaCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.Replicas)
}

// HasReplica reports whether nodeID is a tracked replica.
func (t *TopicMetadata) HasReplica(nodeID string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.Replicas[nodeID]
	return ok
}

// AddReplicaIfAbsent adds nodeID as a replica with the given ISR status if not already tracked.
func (t *TopicMetadata) AddReplicaIfAbsent(nodeID string, isr bool) {
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

// SetReplicaISR sets nodeID's ISR flag (creating the entry with LEO 0 if absent).
// Called only from Apply (Raft-driven), so every node's copy converges identically.
func (t *TopicMetadata) SetReplicaISR(nodeID string, isr bool) {
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
}

// RecordReplicaFetch updates nodeID's LEO from a Fetch call and recomputes its ISR
// status against lagThreshold, returning the new status. This is called directly by
// whichever node currently leads the topic — NOT through Raft/Apply — so it is only
// ever accurate on that node; it exists here (rather than on a purely local struct)
// because ISR membership is derived from it and IS Raft-replicated (the caller sends
// the returned bool through ApplyIsrUpdateEventInternal).
func (t *TopicMetadata) RecordReplicaFetch(nodeID string, leo int64, lagThreshold uint64, leaderLEO uint64) (isr bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
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
	return isr
}

// MinISRLeo returns min(localLEO, all ISR replicas' LEO) — used by the caller to set
// the high watermark. Non-ISR replicas are excluded — a dead/lagging replica must not
// hold back consumer visibility.
func (t *TopicMetadata) MinISRLeo(localLEO uint64) uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	minOffset := localLEO
	for _, r := range t.Replicas {
		if r != nil && r.IsISR && uint64(r.LEO) < minOffset {
			minOffset = uint64(r.LEO)
		}
	}
	return minOffset
}

// MarshalJSON locks t.mu so debug logging can't race with concurrent field mutation.
func (t *TopicMetadata) MarshalJSON() ([]byte, error) {
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

// ClusterMetadataStore is the in-memory materialized view of Raft-replicated topic
// metadata (leader/epoch/replica assignment per topic), kept consistent across every
// node by being the Raft FSM's applied state (implements raft.MetadataStore). This is
// the equivalent of Kafka's MetadataCache/MetadataImage: every broker holds a full
// copy, updated only by applying Raft-committed events; queries here never touch Raft
// directly.
type ClusterMetadataStore struct {
	mu     sync.RWMutex
	Topics map[string]*TopicMetadata
}

func NewClusterMetadataStore() *ClusterMetadataStore {
	return &ClusterMetadataStore{
		Topics: make(map[string]*TopicMetadata),
	}
}

// GetTopic returns the topic metadata, or nil if not found.
func (s *ClusterMetadataStore) GetTopic(name string) *TopicMetadata {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Topics[name]
}

// TopicExists reports whether a topic exists.
func (s *ClusterMetadataStore) TopicExists(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.Topics[name]
	return ok
}

// TopicNames returns all topic names.
func (s *ClusterMetadataStore) TopicNames() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.Topics))
	for name := range s.Topics {
		names = append(names, name)
	}
	return names
}

// NodeIDWithLeastTopics returns whichever of candidateNodeIDs currently leads the
// fewest topics (deterministic tie-break: lexicographically smallest node ID), for
// CreateTopic placement. candidateNodeIDs is the caller's current view of alive
// cluster members (see cluster.Cluster.AliveNodeIDs) — this store only knows about
// topics, not nodes.
func (s *ClusterMetadataStore) NodeIDWithLeastTopics(candidateNodeIDs []string) (string, error) {
	if len(candidateNodeIDs) == 0 {
		return "", raft.ErrRaftNoNodesInCluster
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	countByNode := make(map[string]int, len(candidateNodeIDs))
	for _, id := range candidateNodeIDs {
		countByNode[id] = 0
	}
	for _, t := range s.Topics {
		if t == nil {
			continue
		}
		if leaderID := t.LeaderID(); leaderID != "" {
			if _, ok := countByNode[leaderID]; ok {
				countByNode[leaderID]++
			}
		}
	}
	ids := make([]string, 0, len(countByNode))
	for id := range countByNode {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	bestID := ids[0]
	minCount := countByNode[bestID]
	for _, id := range ids[1:] {
		if c := countByNode[id]; c < minCount {
			minCount = c
			bestID = id
		}
	}
	return bestID, nil
}

// Apply applies a single Raft-committed metadata event to the store.
func (s *ClusterMetadataStore) Apply(ev *raft.MetadataEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch ev.EventType {
	case raft.MetadataEventTypeCreateTopic:
		e, err := raft.DecodeCreateTopicEvent(ev.Data)
		if err != nil {
			return err
		}
		s.createTopicLocked(e.Topic, e.LeaderNodeID, e.LeaderEpoch, e.ReplicaNodeIds)
	case raft.MetadataEventTypeDeleteTopic:
		e, err := raft.DecodeDeleteTopicEvent(ev.Data)
		if err != nil {
			return err
		}
		delete(s.Topics, e.Topic)
	case raft.MetadataEventTypeLeaderChange:
		e, err := raft.DecodeLeaderChangeEvent(ev.Data)
		if err != nil {
			return err
		}
		if t := s.Topics[e.Topic]; t != nil {
			oldLeaderID := t.SetLeader(e.LeaderNodeID, e.LeaderEpoch)
			if oldLeaderID != e.LeaderNodeID && oldLeaderID != "" {
				t.AddReplicaIfAbsent(oldLeaderID, false)
			}
		}
	case raft.MetadataEventTypeIsrUpdate:
		e, err := raft.DecodeIsrUpdateEvent(ev.Data)
		if err != nil {
			return err
		}
		if t := s.Topics[e.Topic]; t != nil {
			t.SetReplicaISR(e.ReplicaNodeID, e.Isr)
		}
	default:
		return fmt.Errorf("unknown or unsupported event type for cluster metadata: %d", ev.EventType)
	}
	return nil
}

// createTopicLocked creates the topic if absent. Caller holds s.mu. Idempotent: guards
// against TOCTOU races between CreateTopic's existence check and Apply().
func (s *ClusterMetadataStore) createTopicLocked(name, leaderNodeID string, leaderEpoch int64, replicaNodeIds []string) {
	if _, exists := s.Topics[name]; exists {
		return
	}
	s.Topics[name] = newTopicMetadata(name, leaderNodeID, leaderEpoch, replicaNodeIds)
}

// Snapshot serializes the store as protobuf, for Raft snapshot persistence.
func (s *ClusterMetadataStore) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return proto.Marshal(snapshotToPB(s.Topics))
}

// Restore replaces the store's contents from a previously-taken Snapshot.
func (s *ClusterMetadataStore) Restore(data []byte) error {
	var m pb.MetadataSnapshot
	if err := proto.Unmarshal(data, &m); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Topics = pbToSnapshot(&m)
	return nil
}

func snapshotToPB(topics map[string]*TopicMetadata) *pb.MetadataSnapshot {
	pbTopics := make(map[string]*pb.TopicState, len(topics))
	for name, t := range topics {
		if t == nil {
			continue
		}
		// Name and DesiredReplicaCount are set once at creation and never mutated
		// afterward, so they're safe to read without t.mu. Leader/epoch/replicas can
		// change concurrently, so those go through Snapshot() which takes t.mu.
		leaderID, epoch, replicaSnaps := t.Snapshot()
		replicas := make(map[string]*pb.ReplicaState, len(replicaSnaps))
		for _, r := range replicaSnaps {
			replicas[r.ReplicaNodeID] = &pb.ReplicaState{ReplicaId: r.ReplicaNodeID, Leo: r.LEO, IsIsr: r.IsISR}
		}
		pbTopics[name] = &pb.TopicState{
			Name:                t.Name,
			LeaderId:            leaderID,
			LeaderEpoch:         epoch,
			DesiredReplicaCount: int32(t.DesiredReplicaCount),
			Replicas:            replicas,
		}
	}
	return &pb.MetadataSnapshot{Topics: pbTopics}
}

func pbToSnapshot(m *pb.MetadataSnapshot) map[string]*TopicMetadata {
	topics := make(map[string]*TopicMetadata, len(m.Topics))
	for name, t := range m.Topics {
		if t == nil {
			continue
		}
		replicas := make(map[string]*ReplicaState, len(t.Replicas))
		for id, r := range t.Replicas {
			if r == nil {
				continue
			}
			replicas[id] = &ReplicaState{ReplicaNodeID: r.ReplicaId, LEO: r.Leo, IsISR: r.IsIsr}
		}
		topics[name] = &TopicMetadata{
			Name:                t.Name,
			LeaderNodeID:        t.LeaderId,
			LeaderEpoch:         t.LeaderEpoch,
			DesiredReplicaCount: int(t.DesiredReplicaCount),
			Replicas:            replicas,
		}
	}
	return topics
}
