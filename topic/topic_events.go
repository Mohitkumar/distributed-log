package topic

import (
	"context"

	"github.com/mohitkumar/mlog/protocol"
)

// ApplyDeleteTopicEvent applies the delete topic event via Raft (used by RPC on Raft leader).
func (tm *TopicManager) ApplyDeleteTopicEvent(topic string) error {
	return tm.coordinator.ApplyDeleteTopicEventInternal(topic)
}

// ApplyIsrUpdateEvent applies the ISR update event via Raft (used by RPC on Raft leader).
func (tm *TopicManager) ApplyIsrUpdateEvent(topic, replicaNodeID string, isr bool, leo int64) error {
	return tm.coordinator.ApplyIsrUpdateEventInternal(topic, replicaNodeID, isr, leo)
}

// applyDeleteTopicEventOnRaftLeader applies the delete topic event via Raft (this node must be Raft leader).
func (tm *TopicManager) applyDeleteTopicEventOnRaftLeader(ctx context.Context, topicName string) error {

	if tm.coordinator.IsLeader() {
		return tm.coordinator.ApplyDeleteTopicEventInternal(topicName)
	}
	raftLeaderNodeID, err := tm.coordinator.GetRaftLeaderNodeID()
	if err != nil {
		return err
	}
	raftLeaderNode := tm.Nodes[raftLeaderNodeID]
	if raftLeaderNode == nil {
		return ErrCannotReachLeader
	}
	raftLeaderClient, err := raftLeaderNode.GetRpcClient()
	if err != nil {
		return err
	}
	_, err = raftLeaderClient.ApplyDeleteTopicEvent(ctx, &protocol.ApplyDeleteTopicEventRequest{Topic: topicName})
	if err != nil {
		return err
	}
	return nil
}

// applyIsrUpdateEventOnRaftLeader applies the ISR update event via Raft (this node must be Raft leader).
func (tm *TopicManager) applyIsrUpdateEventOnRaftLeader(ctx context.Context, topic, replicaNodeID string, isr bool, leo int64) error {
	if tm.coordinator.IsLeader() {
		return tm.coordinator.ApplyIsrUpdateEventInternal(topic, replicaNodeID, isr, leo)
	}
	raftLeaderNodeID, err := tm.coordinator.GetRaftLeaderNodeID()
	if err != nil {
		return err
	}
	raftLeaderNode := tm.Nodes[raftLeaderNodeID]
	if raftLeaderNode == nil {
		return ErrCannotReachLeader
	}
	raftLeaderClient, err := raftLeaderNode.GetRpcClient()
	if err != nil {
		return err
	}
	_, err = raftLeaderClient.ApplyIsrUpdateEvent(ctx, &protocol.ApplyIsrUpdateEventRequest{Topic: topic, ReplicaNodeID: replicaNodeID, Isr: isr, Leo: leo})
	if err != nil {
		return err
	}
	return nil
}
