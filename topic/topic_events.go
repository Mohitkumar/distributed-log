package topic

// ApplyDeleteTopicEvent applies the delete topic event via Raft (used by RPC on Raft leader).
func (tm *TopicManager) ApplyDeleteTopicEvent(topic string) error {
	return tm.coordinator.ApplyDeleteTopicEventInternal(topic)
}

// ApplyIsrUpdateEvent applies the ISR update event via Raft (used by RPC on Raft leader).
func (tm *TopicManager) ApplyIsrUpdateEvent(topic, replicaNodeID string, isr bool, leo int64) error {
	return tm.coordinator.ApplyIsrUpdateEventInternal(topic, replicaNodeID, isr, leo)
}
