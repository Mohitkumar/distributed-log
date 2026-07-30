package raft

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/mohitkumar/mlog/broker/config"
	"go.uber.org/zap"
)

const (
	SnapshotThreshold   = 10000
	SnapshotInterval    = 10
	RetainSnapshotCount = 10
)

type RaftNode struct {
	Logger     *zap.Logger
	raft       *raft.Raft
	raftConfig *raft.Config
	LocalAddr  raft.ServerAddress
	cfg        config.Config
	stableLog  *raftboltdb.BoltStore // closed in Shutdown — raft.Shutdown() doesn't own it
	logStore   *logStore             // closed in Shutdown — raft.Shutdown() doesn't own it
}

func NewRaftNode(cfg config.Config, metadataStore MetadataStore, logger *zap.Logger) (*RaftNode, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	fsm, err := NewFSM(cfg.RaftConfig.Dir, metadataStore)
	if err != nil {
		return nil, err
	}
	raftNode, raftConfig, localAddr, stableLog, logStore, err := setupRaft(fsm, cfg.RaftConfig)
	if err != nil {
		return nil, err
	}

	c := &RaftNode{
		Logger:     logger,
		raft:       raftNode,
		raftConfig: raftConfig,
		LocalAddr:  localAddr,
		cfg:        cfg,
		stableLog:  stableLog,
		logStore:   logStore,
	}
	rpcAddr, err := cfg.RPCAddr()
	if err != nil {
		return nil, err
	}
	c.Logger.Info("coordinator started", zap.String("raft_addr", cfg.RaftConfig.Address), zap.String("rpc_addr", rpcAddr))
	return c, nil
}

func setupRaft(fsm raft.FSM, cfg config.RaftConfig) (*raft.Raft, *raft.Config, raft.ServerAddress, *raftboltdb.BoltStore, *logStore, error) {
	raftBindAddr := cfg.Address
	if cfg.BindAddress != "" {
		raftBindAddr = cfg.BindAddress
	}
	raftAdvertiseAddr := cfg.Address
	raftConfig := raft.DefaultConfig()
	raftConfig.SnapshotThreshold = uint64(SnapshotThreshold)
	raftConfig.SnapshotInterval = time.Duration(SnapshotInterval) * time.Second
	raftConfig.LocalID = raft.ServerID(cfg.ID)
	raftConfig.LogLevel = cfg.LogLevel

	advertiseAddr, err := net.ResolveTCPAddr("tcp", raftAdvertiseAddr)
	if err != nil {
		return nil, nil, "", nil, nil, fmt.Errorf("failed to resolve Raft advertise address %s: %w", raftAdvertiseAddr, err)
	}
	transport, err := raft.NewTCPTransport(raftBindAddr, advertiseAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, nil, "", nil, nil, fmt.Errorf("failed to make TCP transport bind %s advertise %s: %w", raftBindAddr, raftAdvertiseAddr, err)
	}
	snapshots, err := raft.NewFileSnapshotStore(cfg.Dir, RetainSnapshotCount, os.Stderr)
	if err != nil {
		return nil, nil, "", nil, nil, fmt.Errorf("failed to create snapshot store at %s: %w", cfg.Dir, err)
	}
	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(cfg.Dir, "raft.db"))
	if err != nil {
		return nil, nil, "", nil, nil, fmt.Errorf("failed to create bolt store: %w", err)
	}
	logStore, err := NewLogStore(cfg.Dir)
	if err != nil {
		return nil, nil, "", nil, nil, fmt.Errorf("failed to create log store: %w", err)
	}
	ra, err := raft.NewRaft(raftConfig, fsm, logStore, boltDB, snapshots, transport)
	if err != nil {
		return nil, nil, "", nil, nil, ErrNewRaft(err)
	}
	return ra, raftConfig, transport.LocalAddr(), boltDB, logStore, nil
}

func (c *RaftNode) Join(id, raftAddr, rpcAddr string) error {
	if !c.IsLeader() {
		c.Logger.Debug("not leader, skipping join", zap.String("joining_node_id", id), zap.String("raft_addr", raftAddr), zap.String("rpc_addr", rpcAddr))
		return nil
	}
	c.Logger.Info("join requested", zap.String("joining_node_id", id), zap.String("raft_addr", raftAddr), zap.String("rpc_addr", rpcAddr))
	configFuture := c.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(raftAddr)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				return nil
			}
			removeFuture := c.raft.RemoveServer(serverID, 0, 5*time.Second)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}
	addFuture := c.raft.AddVoter(serverID, serverAddr, 0, 5*time.Second)
	if err := addFuture.Error(); err != nil {
		c.Logger.Error("raft add voter failed", zap.Error(err), zap.String("node_id", id))
		return err
	}
	c.Logger.Info("node joined cluster", zap.String("joined_node_id", id), zap.String("raft_addr", raftAddr), zap.String("rpc_addr", rpcAddr))
	return nil
}

func (c *RaftNode) Leave(id string) error {
	if !c.IsLeader() {
		c.Logger.Debug("not leader, skipping leave", zap.String("leaving_node_id", id))
		return nil
	}
	c.Logger.Info("leave requested", zap.String("leaving_node_id", id))
	removeFuture := c.raft.RemoveServer(raft.ServerID(id), 0, 5*time.Second)
	if err := removeFuture.Error(); err != nil {
		c.Logger.Error("raft remove server failed", zap.Error(err), zap.String("node_id", id))
		return err
	}
	c.Logger.Info("node left cluster", zap.String("left_node_id", id))
	return nil
}

func (c *RaftNode) IsLeader() bool {
	return c.raft.State() == raft.Leader
}

func (c *RaftNode) ApplyEvent(data []byte) error {
	f := c.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		return ErrRaftApply(err)
	}
	return nil
}

func (c *RaftNode) GetRaftLeaderNodeID() (string, error) {
	_, id := c.raft.LeaderWithID()
	return string(id), nil
}

// RaftServerIDs returns the current Raft cluster server IDs (for reconciliation with Serf).
func (c *RaftNode) RaftServerIDs() ([]string, error) {
	f := c.raft.GetConfiguration()
	if err := f.Error(); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(f.Configuration().Servers))
	for _, s := range f.Configuration().Servers {
		ids = append(ids, string(s.ID))
	}
	return ids, nil
}

func (c *RaftNode) WaitforRaftReady(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out waiting for raft ready")
		case <-ticker.C:
			c.Logger.Info("waiting for raft ready", zap.String("leader", string(c.raft.Leader())))
			if c.raft.Leader() != "" {
				return nil
			}
		}
	}
}

func (c *RaftNode) IsRaftReady() bool {
	return c.raft.Leader() != ""
}

func (c *RaftNode) Start() error {
	cfg := c.cfg.RaftConfig
	raftConfig := c.raftConfig
	if cfg.Boostatrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: c.LocalAddr,
				},
			},
		}
		if err := c.raft.BootstrapCluster(configuration).Error(); err != nil {
			return ErrBootstrapCluster(err)
		}
	}
	return nil
}

// Shutdown stops Raft and closes the stable/log stores it opened in setupRaft —
// raft.Shutdown() only stops the FSM/transport, it never owns or closes the
// LogStore/StableStore passed into raft.NewRaft, so skipping this leaks the
// underlying BoltDB file lock and mmap'd segments (surfaces as a hang re-acquiring
// the same lock if the node is ever restarted in the same process, e.g. under test).
func (c *RaftNode) Shutdown() error {
	c.Logger.Info("coordinator shutting down")
	var errs []error
	if err := c.raft.Shutdown().Error(); err != nil {
		errs = append(errs, err)
	}
	if c.stableLog != nil {
		if err := c.stableLog.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if c.logStore != nil {
		if err := c.logStore.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// PeerChangeEvent describes a peer being added to or removed from this node's
// active Raft configuration. Only ever fires on whichever node currently holds
// Raft leadership — peer/replication tracking is a leader-only concept in Raft,
// so followers never observe these.
type PeerChangeEvent struct {
	NodeID  string
	Removed bool
}

// WatchPeerChanges registers a Raft observer for peer configuration changes
// (fired the moment an AddVoter/RemoveServer takes effect — see hashicorp/raft's
// PeerObservation) and returns a channel of translated events plus a function to
// stop watching and release the observer. The observer is non-blocking, so a
// slow consumer can miss events under heavy churn; that's fine here since each
// event is just a prompt to react, not a queue that must be drained exactly.
func (c *RaftNode) WatchPeerChanges() (<-chan PeerChangeEvent, func()) {
	raw := make(chan raft.Observation, 16)
	observer := raft.NewObserver(raw, false, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.PeerObservation)
		return ok
	})
	c.raft.RegisterObserver(observer)

	out := make(chan PeerChangeEvent, 16)
	stop := make(chan struct{})
	go func() {
		defer close(out)
		for {
			select {
			case <-stop:
				return
			case obs, ok := <-raw:
				if !ok {
					return
				}
				po, ok := obs.Data.(raft.PeerObservation)
				if !ok {
					continue
				}
				select {
				case out <- PeerChangeEvent{NodeID: string(po.Peer.ID), Removed: po.Removed}:
				case <-stop:
					return
				}
			}
		}
	}()

	return out, func() {
		c.raft.DeregisterObserver(observer)
		close(stop)
	}
}
