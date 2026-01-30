package raftmeta

import (
	"sync"

	"github.com/mohitkumar/mlog/protocol"
)

type PartitionMetadata struct {
	PartitionID string
	LeaderID    string
	LeaderEpoch int64
	ISR         map[string]bool
	HW          int64
}

type MetadataStore struct {
	mu         sync.RWMutex
	partitions map[string]*PartitionMetadata
}

func NewMetadataStore() *MetadataStore {
	return &MetadataStore{
		partitions: make(map[string]*PartitionMetadata),
	}
}

func (ms *MetadataStore) GetPartitionMetadata(PartitionID string) (*PartitionMetadata, bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	partition, exists := ms.partitions[PartitionID]
	return partition, exists
}

func (ms *MetadataStore) Apply(ev *protocol.MetadataEvent) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ev.CreatePartition != nil {
		e := ev.CreatePartition
		replicas := make(map[string]bool)
		for _, r := range e.Replicas {
			replicas[r] = true
		}
		ms.partitions[e.PartitionId] = &PartitionMetadata{
			PartitionID: e.PartitionId,
			ISR:         replicas,
			HW:          -1,
		}
		return
	}
	if ev.LeaderChange != nil {
		e := ev.LeaderChange
		p := ms.partitions[e.PartitionId]
		if p != nil {
			p.LeaderID = e.LeaderId
			p.LeaderEpoch = e.LeaderEpoch
		}
		return
	}
	if ev.IsrUpdate != nil {
		e := ev.IsrUpdate
		p := ms.partitions[e.PartitionId]
		if p != nil {
			newISR := make(map[string]bool)
			for _, r := range e.Isr {
				newISR[r] = true
			}
			p.ISR = newISR
		}
		return
	}
	if ev.HwUpdate != nil {
		e := ev.HwUpdate
		p := ms.partitions[e.PartitionId]
		if p != nil {
			p.HW = e.Offset
		}
	}
}
