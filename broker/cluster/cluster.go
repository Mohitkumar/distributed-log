package cluster

import (
	"strings"

	"github.com/mohitkumar/mlog/api/protocol"
	raft "github.com/mohitkumar/mlog/broker/cluster/raft"
	"github.com/mohitkumar/mlog/broker/config"
	"go.uber.org/zap"
)

type Cluster struct {
	Logger *zap.Logger
	node   *raft.RaftNode
	cfg    config.Config
}

func NewCluster(cfg config.Config, metadataStore raft.MetadataStore, logger *zap.Logger) (*Cluster, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	node, err := raft.NewRaftNode(cfg, metadataStore, logger)
	if err != nil {
		return nil, err
	}
	c := &Cluster{
		Logger: logger,
		node:   node,
		cfg:    cfg,
	}
	c.node.Start()
	return c, nil
}

func (c *Cluster) ApplyCreateTopicEvent(topic string, replicaCount uint32, leaderNodeID string, replicaNodeIds []string) error {
	if !c.node.IsLeader() {
		c.Logger.Debug("not leader, skipping create topic event", zap.String("topic", topic))
		return nil
	}
	eventData, err := protocol.EncodeCreateTopicEvent(protocol.CreateTopicEvent{
		Topic:          topic,
		ReplicaCount:   replicaCount,
		LeaderNodeID:   leaderNodeID,
		LeaderEpoch:    1,
		ReplicaNodeIds: replicaNodeIds,
	})
	if err != nil {
		return err
	}
	data, err := protocol.EncodeMetadataEvent(&protocol.MetadataEvent{
		EventType: protocol.MetadataEventTypeCreateTopic,
		Data:      eventData,
	})
	if err != nil {
		return err
	}
	c.Logger.Info("apply create topic event", zap.String("topic", topic), zap.String("leader_node_id", leaderNodeID))
	err = c.node.ApplyEvent(data)
	if err != nil {
		c.Logger.Error("raft apply create topic failed", zap.Error(err), zap.String("topic", topic))
		return err
	}
	return nil
}

func (c *Cluster) ApplyDeleteTopicEventInternal(topic string) error {
	if !c.node.IsLeader() {
		c.Logger.Debug("not leader, skipping delete topic event", zap.String("topic", topic))
		return nil
	}
	eventData, err := protocol.EncodeDeleteTopicEvent(protocol.DeleteTopicEvent{Topic: topic})
	if err != nil {
		return err
	}
	data, err := protocol.EncodeMetadataEvent(&protocol.MetadataEvent{
		EventType: protocol.MetadataEventTypeDeleteTopic,
		Data:      eventData,
	})
	if err != nil {
		return err
	}
	c.Logger.Info("apply delete topic event", zap.String("topic", topic))
	err = c.node.ApplyEvent(data)
	if err != nil {
		c.Logger.Error("raft apply delete topic failed", zap.Error(err), zap.String("topic", topic))
		return err
	}
	return nil
}

func (c *Cluster) ApplyIsrUpdateEventInternal(topic, replicaNodeID string, isr bool) error {
	if !c.node.IsLeader() {
		c.Logger.Debug("not leader, skipping ISR update event", zap.String("topic", topic))
		return nil
	}
	eventData, err := protocol.EncodeIsrUpdateEvent(protocol.IsrUpdateEvent{Topic: topic, ReplicaNodeID: replicaNodeID, Isr: isr})
	if err != nil {
		return err
	}
	data, err := protocol.EncodeMetadataEvent(&protocol.MetadataEvent{
		EventType: protocol.MetadataEventTypeIsrUpdate,
		Data:      eventData,
	})
	if err != nil {
		return err
	}
	err = c.node.ApplyEvent(data)
	if err != nil {
		msg := err.Error()
		if strings.Contains(msg, "shutdown") || strings.Contains(msg, "leadership lost") {
			c.Logger.Debug("raft apply ISR update failed (shutdown or leadership change)", zap.Error(err))
		} else {
			c.Logger.Error("raft apply ISR update failed", zap.Error(err))
		}
		return err
	}
	return nil
}

func (c *Cluster) ApplyLeaderChangeEvent(topic, leaderNodeID string, leaderEpoch int64) error {
	if !c.node.IsLeader() {
		c.Logger.Debug("not leader, skipping leader change event", zap.String("topic", topic))
		return nil
	}
	eventData, err := protocol.EncodeLeaderChangeEvent(protocol.LeaderChangeEvent{
		Topic:        topic,
		LeaderNodeID: leaderNodeID,
		LeaderEpoch:  leaderEpoch,
	})
	if err != nil {
		return err
	}
	data, err := protocol.EncodeMetadataEvent(&protocol.MetadataEvent{
		EventType: protocol.MetadataEventTypeLeaderChange,
		Data:      eventData,
	})
	if err != nil {
		return err
	}
	c.Logger.Info("apply leader change event", zap.String("topic", topic), zap.String("new_leader_node_id", leaderNodeID), zap.Int64("leader_epoch", leaderEpoch))
	err = c.node.ApplyEvent(data)
	if err != nil {
		c.Logger.Error("raft apply leader change failed", zap.Error(err), zap.String("topic", topic))
		return err
	}
	return nil
}

func (c *Cluster) Start() error {
	return c.node.Start()
}

func (c *Cluster) ShutDown() error {
	return c.node.Shutdown()
}
