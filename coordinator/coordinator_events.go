package coordinator

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mohitkumar/mlog/errs"
	"github.com/mohitkumar/mlog/protocol"
	"go.uber.org/zap"
)

func (c *Coordinator) ApplyCreateTopicEvent(topic string, replicaCount uint32, leaderNodeID string, replicaNodeIds []string) error {
	if c.raft.State() != raft.Leader {
		c.Logger.Debug("not leader, skipping create topic event", zap.String("topic", topic))
		return nil
	}
	eventData, err := json.Marshal(protocol.CreateTopicEvent{
		Topic:          topic,
		ReplicaCount:   replicaCount,
		LeaderNodeID:   leaderNodeID,
		LeaderEpoch:    1,
		ReplicaNodeIds: replicaNodeIds,
	})
	if err != nil {
		return err
	}
	ev := &protocol.MetadataEvent{
		EventType: protocol.MetadataEventTypeCreateTopic,
		Data:      eventData,
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	c.Logger.Info("applying create topic event", zap.String("topic", topic), zap.String("leader_node_id", leaderNodeID))
	f := c.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		c.Logger.Error("raft apply create topic failed", zap.Error(err), zap.String("topic", topic))
		return errs.ErrRaftApply(err)
	}
	return nil
}

func (c *Coordinator) ApplyDeleteTopicEventInternal(topic string) error {
	if c.raft.State() != raft.Leader {
		c.Logger.Debug("not leader, skipping delete topic event", zap.String("topic", topic))
		return nil
	}
	eventData, err := json.Marshal(protocol.DeleteTopicEvent{Topic: topic})
	if err != nil {
		return err
	}
	ev := &protocol.MetadataEvent{
		EventType: protocol.MetadataEventTypeDeleteTopic,
		Data:      eventData,
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	c.Logger.Info("applying delete topic event", zap.String("topic", topic))
	f := c.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		c.Logger.Error("raft apply delete topic failed", zap.Error(err), zap.String("topic", topic))
		return errs.ErrRaftApply(err)
	}
	return nil
}

func (c *Coordinator) ApplyNodeAddEvent(nodeID, addr, rpcAddr string) error {
	if c.raft.State() != raft.Leader {
		c.Logger.Debug("not leader, skipping node add event", zap.String("node_id", nodeID))
		return nil
	}
	eventData, err := json.Marshal(protocol.AddNodeEvent{NodeID: nodeID, Addr: addr, RpcAddr: rpcAddr})
	if err != nil {
		return err
	}
	ev := &protocol.MetadataEvent{
		EventType: protocol.MetadataEventTypeAddNode,
		Data:      eventData,
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	f := c.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		c.Logger.Error("raft apply node add failed", zap.Error(err), zap.String("add_node_id", nodeID))
		return errs.ErrRaftApply(err)
	}
	return nil
}

func (c *Coordinator) ApplyNodeRemoveEvent(nodeID string) error {
	if c.raft.State() != raft.Leader {
		c.Logger.Debug("not leader, skipping node remove event", zap.String("node_id", nodeID))
		return nil
	}
	eventData, err := json.Marshal(protocol.RemoveNodeEvent{NodeID: nodeID})
	if err != nil {
		return err
	}
	ev := &protocol.MetadataEvent{
		EventType: protocol.MetadataEventTypeRemoveNode,
		Data:      eventData,
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	f := c.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		c.Logger.Error("raft apply node remove failed", zap.Error(err), zap.String("remove_node_id", nodeID))
		return errs.ErrRaftApply(err)
	}
	return nil
}

func (c *Coordinator) ApplyIsrUpdateEventInternal(topic, replicaNodeID string, isr bool) error {
	if c.raft.State() != raft.Leader {
		c.Logger.Debug("not leader, skipping ISR update event", zap.String("topic", topic))
		return nil
	}
	eventData, err := json.Marshal(protocol.IsrUpdateEvent{Topic: topic, ReplicaNodeID: replicaNodeID, Isr: isr})
	if err != nil {
		return err
	}
	ev := &protocol.MetadataEvent{
		EventType: protocol.MetadataEventTypeIsrUpdate,
		Data:      eventData,
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	f := c.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		msg := err.Error()
		if strings.Contains(msg, "shutdown") || strings.Contains(msg, "leadership lost") {
			c.Logger.Debug("raft apply ISR update failed (shutdown or leadership change)", zap.Error(err))
		} else {
			c.Logger.Error("raft apply ISR update failed", zap.Error(err))
		}
		return errs.ErrRaftApply(err)
	}
	return nil
}

func (c *Coordinator) ApplyLeaderChangeEvent(topic, leaderNodeID string, leaderEpoch int64) error {
	if c.raft.State() != raft.Leader {
		c.Logger.Debug("not leader, skipping leader change event", zap.String("topic", topic))
		return nil
	}
	eventData, err := json.Marshal(protocol.LeaderChangeEvent{
		Topic:        topic,
		LeaderNodeID: leaderNodeID,
		LeaderEpoch:  leaderEpoch,
	})
	if err != nil {
		return err
	}
	ev := &protocol.MetadataEvent{
		EventType: protocol.MetadataEventTypeLeaderChange,
		Data:      eventData,
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	c.Logger.Info("applying leader change event", zap.String("topic", topic), zap.String("new_leader_node_id", leaderNodeID), zap.Int64("leader_epoch", leaderEpoch))
	f := c.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		c.Logger.Error("raft apply leader change failed", zap.Error(err), zap.String("topic", topic))
		return errs.ErrRaftApply(err)
	}
	return nil
}
