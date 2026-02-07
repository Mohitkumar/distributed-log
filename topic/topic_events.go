package topic

// ApplyDeleteTopicEvent delegates to the node (used by RPC on Raft leader).
func (tm *TopicManager) ApplyDeleteTopicEvent(topic string) error {
	return tm.node.ApplyDeleteTopicEvent(topic)
}

// ApplyIsrUpdateEvent delegates to the node (used by RPC on Raft leader).
func (tm *TopicManager) ApplyIsrUpdateEvent(topic, replicaNodeID string, isr bool, leo int64) error {
	return tm.node.ApplyIsrUpdateEvent(topic, replicaNodeID, isr, leo)
}
