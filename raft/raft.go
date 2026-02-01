package raft

import (
	"context"
	"time"

	"github.com/mohitkumar/mlog/protocol"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type MetadataRaft struct {
	node    raft.Node
	storage *raft.MemoryStorage
	applyCh chan *protocol.MetadataEvent
}

// sendRaftRPC is a placeholder for sending raft messages to peers.
// TODO: implement transport (TCP) and peer routing.
func sendRaftRPC(msg raftpb.Message) {
	_ = msg
}

func StartMetadataRaft(id uint64, peers []raft.Peer) *MetadataRaft {
	storage := raft.NewMemoryStorage()

	cfg := &raft.Config{
		ID:            id,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Storage:       storage,
	}

	node := raft.StartNode(cfg, peers)

	mr := &MetadataRaft{
		node:    node,
		storage: storage,
		applyCh: make(chan *protocol.MetadataEvent, 1024),
	}

	go mr.loop()
	return mr
}

func (m *MetadataRaft) loop() {
	ticker := time.NewTicker(100 * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			m.node.Tick()

		case rd := <-m.node.Ready():
			m.storage.Append(rd.Entries)

			for _, msg := range rd.Messages {
				sendRaftRPC(msg)
			}

			for _, ent := range rd.CommittedEntries {
				if ent.Type != raftpb.EntryNormal {
					continue
				}
				ev := &protocol.MetadataEvent{}
				if err := protocol.UnmarshalJSON(ent.Data, ev); err != nil {
					continue
				}
				m.applyCh <- ev
			}
			m.node.Advance()
		}
	}
}

func (m *MetadataRaft) Propose(ev *protocol.MetadataEvent) error {
	data, err := protocol.MarshalJSON(ev)
	if err != nil {
		return err
	}
	return m.node.Propose(context.Background(), data)
}
