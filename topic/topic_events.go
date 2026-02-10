package topic

// ApplyDeleteTopicEvent delegates to the coordinator (used by RPC on Raft leader).
func (tm *TopicManager) ApplyDeleteTopicEvent(topic string) error {
	return tm.coordinator.ApplyDeleteTopicEvent(topic)
}

// ApplyIsrUpdateEvent delegates to the coordinator (used by RPC on Raft leader).
func (tm *TopicManager) ApplyIsrUpdateEvent(topic, replicaNodeID string, isr bool, leo int64) error {
	return tm.coordinator.ApplyIsrUpdateEvent(topic, replicaNodeID, isr, leo)
}
