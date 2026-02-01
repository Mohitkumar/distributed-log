package protocol

// MetadataEvent is applied to metadata store (replaces api/metadata.MetadataEvent).
// Exactly one of the event fields should be set.
type MetadataEvent struct {
	CreatePartition *CreatePartitionEvent `json:"create_partition,omitempty"`
	LeaderChange    *LeaderChangeEvent    `json:"leader_change,omitempty"`
	IsrUpdate       *IsrUpdateEvent       `json:"isr_update,omitempty"`
	HwUpdate        *HwUpdateEvent        `json:"hw_update,omitempty"`
}

type CreatePartitionEvent struct {
	PartitionId string   `json:"partition_id"`
	Replicas    []string `json:"replicas"`
}

type LeaderChangeEvent struct {
	PartitionId  string `json:"partition_id"`
	LeaderId     string `json:"leader_id"`
	LeaderEpoch  int64  `json:"leader_epoch"`
}

type IsrUpdateEvent struct {
	PartitionId string   `json:"partition_id"`
	Isr         []string `json:"isr"`
}

type HwUpdateEvent struct {
	PartitionId string `json:"partition_id"`
	Offset      int64  `json:"offset"`
}

// Which returns which event type is set (for Apply switch).
func (e *MetadataEvent) Which() interface{} {
	if e.CreatePartition != nil {
		return e.CreatePartition
	}
	if e.LeaderChange != nil {
		return e.LeaderChange
	}
	if e.IsrUpdate != nil {
		return e.IsrUpdate
	}
	if e.HwUpdate != nil {
		return e.HwUpdate
	}
	return nil
}
