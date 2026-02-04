package coordinator

import (
	"errors"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

const (
	SnapshotThreshold   = 10000
	SnapshotInterval    = 10
	RetainSnapshotCount = 10
)

func SetupRaft(fsm raft.FSM, id, raftAddress, raftDir string, boostrap bool) (*raft.Raft, error) {
	config := raft.DefaultConfig()
	// Override defaults with configured values
	config.SnapshotThreshold = uint64(SnapshotThreshold)
	config.SnapshotInterval = time.Duration(SnapshotInterval) * time.Second
	config.LocalID = raft.ServerID(id)
	config.LogLevel = "INFO"

	// Setup Raft communication.
	var TCPAddress *net.TCPAddr
	var transport *raft.NetworkTransport
	var err error
	var snapshots *raft.FileSnapshotStore
	if TCPAddress, err = net.ResolveTCPAddr("tcp", raftAddress); err != nil {
		log.Fatalf("failed to resolve TCP address %s: %s", raftAddress, err)
	}
	if transport, err = raft.NewTCPTransport(raftAddress, TCPAddress, 3, 10*time.Second, os.Stderr); err != nil {
		log.Fatalf("failed to make TCP transport on %s: %s", raftAddress, err.Error())
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	if snapshots, err = raft.NewFileSnapshotStore(raftDir, RetainSnapshotCount, os.Stderr); err != nil {
		log.Fatalf("failed to create snapshot store at %s: %s", raftDir, err.Error())
	}

	// Create the log store and stable store.
	var logStore *logStore
	var stableStore raft.StableStore

	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		log.Fatalf("failed to create new bolt store: %s", err)
	}
	logStore, err = NewLogStore(raftDir)
	if err != nil {
		log.Fatalf("failed to create log store: %s", err)
	}
	stableStore = boltDB

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, ErrNewRaft(err)
	}
	if boostrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		if err := ra.BootstrapCluster(configuration).Error(); err != nil {
			// Restart with existing data (e.g. Docker volume): cluster already bootstrapped; continue.
			if errors.Is(err, raft.ErrCantBootstrap) {
				return ra, nil
			}
			return nil, ErrBootstrapCluster(err)
		}
	}

	return ra, nil
}
