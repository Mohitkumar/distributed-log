package topic

// ApplyDeleteTopicEvent delegates to the coordinator (used by RPC on Raft leader).
func (tm *TopicManager) ApplyDeleteTopicEvent(topic string) {
	tm.coordinator.ApplyEvent(NewDeleteTopicApplyEvent(topic))
}

// ApplyIsrUpdateEvent delegates to the coordinator (used by RPC on Raft leader).
func (tm *TopicManager) ApplyIsrUpdateEvent(topic, replicaNodeID string, isr bool, leo int64) {
	tm.coordinator.ApplyEvent(NewIsrUpdateApplyEvent(topic, replicaNodeID, isr, leo))
}
