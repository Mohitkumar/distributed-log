package protocol

type MetadataEvent struct {
	CreateTopicEvent  *CreateTopicEvent
	DeleteTopicEvent  *DeleteTopicEvent
	LeaderChangeEvent *LeaderChangeEvent
	IsrUpdateEvent    *IsrUpdateEvent
	AddNodeEvent      *AddNodeEvent
	RemoveNodeEvent   *RemoveNodeEvent
	UpdateNodeEvent   *UpdateNodeEvent
}
type CreateTopicEvent struct {
	Topic        string   `json:"topic"`
	ReplicaCount uint32   `json:"replica_count"`
	LeaderID     string   `json:"leader_id"`
	LeaderEpoch  int64    `json:"leader_epoch"`
	Replicas     []string `json:"replicas"`
}

type DeleteTopicEvent struct {
	Topic string `json:"topic"`
}

type LeaderChangeEvent struct {
	Topic       string `json:"topic"`
	LeaderID    string `json:"leader_id"`
	LeaderEpoch int64  `json:"leader_epoch"`
}

type IsrUpdateEvent struct {
	Topic     string   `json:"topic"`
	ReplicaID string   `json:"replica_id"`
	Isr       []string `json:"isr"`
}

type AddNodeEvent struct {
	NodeID  string `json:"node_id"`
	Addr    string `json:"addr"`
	RpcAddr string `json:"rpc_addr"`
}

type RemoveNodeEvent struct {
	NodeID string `json:"node_id"`
}

type UpdateNodeEvent struct {
	NodeID    string `json:"node_id"`
	IsHealthy bool   `json:"is_healthy"`
}

func (e *MetadataEvent) WhichEvent() string {
	switch {
	case e.CreateTopicEvent != nil:
		return "CreateTopicEvent"
	case e.DeleteTopicEvent != nil:
		return "DeleteTopicEvent"
	case e.LeaderChangeEvent != nil:
		return "LeaderChangeEvent"
	case e.IsrUpdateEvent != nil:
		return "IsrUpdateEvent"
	case e.AddNodeEvent != nil:
		return "AddNodeEvent"
	case e.RemoveNodeEvent != nil:
		return "RemoveNodeEvent"
	case e.UpdateNodeEvent != nil:
		return "UpdateNodeEvent"
	}
	return ""
}
