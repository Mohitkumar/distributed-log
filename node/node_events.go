package node

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mohitkumar/mlog/protocol"
	"go.uber.org/zap"
)

func (n *Node) ApplyCreateTopicEvent(topic string, replicaCount uint32, leaderNodeID string, replicaNodeIds []string) error {
	if n.raft.State() != raft.Leader {
		n.Logger.Debug("not leader, skipping create topic event", zap.String("topic", topic))
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
	n.Logger.Info("applying create topic event", zap.String("topic", topic), zap.String("leader_node_id", leaderNodeID))
	f := n.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		n.Logger.Error("raft apply create topic failed", zap.Error(err), zap.String("topic", topic))
		return ErrRaftApply(err)
	}
	return nil
}

func (n *Node) ApplyDeleteTopicEvent(topic string) error {
	if n.raft.State() != raft.Leader {
		n.Logger.Debug("not leader, skipping delete topic event", zap.String("topic", topic))
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
	n.Logger.Info("applying delete topic event", zap.String("topic", topic))
	f := n.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		n.Logger.Error("raft apply delete topic failed", zap.Error(err), zap.String("topic", topic))
		return ErrRaftApply(err)
	}
	return nil
}

func (n *Node) ApplyNodeAddEvent(nodeID, addr, rpcAddr string) error {
	if n.raft.State() != raft.Leader {
		n.Logger.Debug("not leader, skipping node add event", zap.String("node_id", nodeID))
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
	f := n.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		n.Logger.Error("raft apply node add failed", zap.Error(err), zap.String("add_node_id", nodeID))
		return ErrRaftApply(err)
	}
	return nil
}

func (n *Node) ApplyNodeRemoveEvent(nodeID string) error {
	if n.raft.State() != raft.Leader {
		n.Logger.Debug("not leader, skipping node remove event", zap.String("node_id", nodeID))
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
	f := n.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		n.Logger.Error("raft apply node remove failed", zap.Error(err), zap.String("remove_node_id", nodeID))
		return ErrRaftApply(err)
	}
	return nil
}

func (n *Node) ApplyIsrUpdateEvent(topic, replicaNodeID string, isr bool, leo int64) error {
	if n.raft.State() != raft.Leader {
		n.Logger.Debug("not leader, skipping ISR update event", zap.String("topic", topic))
		return nil
	}
	eventData, err := json.Marshal(protocol.IsrUpdateEvent{Topic: topic, ReplicaNodeID: replicaNodeID, Isr: isr, Leo: leo})
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
	f := n.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		msg := err.Error()
		if strings.Contains(msg, "shutdown") || strings.Contains(msg, "leadership lost") {
			n.Logger.Debug("raft apply ISR update failed (shutdown or leadership change)", zap.Error(err))
		} else {
			n.Logger.Error("raft apply ISR update failed", zap.Error(err))
		}
		return ErrRaftApply(err)
	}
	return nil
}

func (n *Node) ApplyLeaderChangeEvent(topic, leaderNodeID string, leaderEpoch int64) error {
	if n.raft.State() != raft.Leader {
		n.Logger.Debug("not leader, skipping leader change event", zap.String("topic", topic))
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
	n.Logger.Info("applying leader change event", zap.String("topic", topic), zap.String("new_leader_node_id", leaderNodeID), zap.Int64("leader_epoch", leaderEpoch))
	f := n.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		n.Logger.Error("raft apply leader change failed", zap.Error(err), zap.String("topic", topic))
		return ErrRaftApply(err)
	}
	return nil
}
