package coordinator

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/mohitkumar/mlog/config"
	"github.com/mohitkumar/mlog/discovery"
	"github.com/mohitkumar/mlog/errs"
	"go.uber.org/zap"
)

var _ discovery.Handler = (*Coordinator)(nil)

const (
	SnapshotThreshold   = 10000
	SnapshotInterval    = 10
	RetainSnapshotCount = 10
)

// Coordinator handles cluster-wide responsibility: Raft, metadata store, and membership (Join/Leave, Apply* events).
// Addresses and RPC clients to other nodes live in Node; Coordinator holds the local Node and delegates to it where appropriate.
type Coordinator struct {
	mu            sync.RWMutex
	Logger        *zap.Logger
	raft          *raft.Raft
	cfg           config.Config
	metadataStore MetadataStore
}

// NewCoordinatorFromConfig creates a Coordinator from the given config.
func NewCoordinatorFromConfig(cfg config.Config, metadataStore MetadataStore, logger *zap.Logger) (*Coordinator, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	fsm, err := NewCoordinatorFSM(cfg.RaftConfig.Dir, metadataStore)
	if err != nil {
		return nil, err
	}
	raftNode, err := setupRaft(fsm, cfg.RaftConfig)
	if err != nil {
		return nil, err
	}
	rpcAddr, err := cfg.RPCAddr()
	if err != nil {
		return nil, err
	}
	c := &Coordinator{
		Logger:        logger,
		raft:          raftNode,
		cfg:           cfg,
		metadataStore: metadataStore,
	}
	c.Logger.Info("coordinator started", zap.String("raft_addr", cfg.RaftConfig.Address), zap.String("rpc_addr", rpcAddr))
	return c, nil
}

// SetupRaft creates a Raft node. BindAddress is the listen address (e.g. 0.0.0.0:9093); Address is what others use to reach this node.
func setupRaft(fsm raft.FSM, cfg config.RaftConfig) (*raft.Raft, error) {
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
		log.Fatalf("failed to resolve Raft advertise address %s: %s", raftAdvertiseAddr, err)
	}
	transport, err := raft.NewTCPTransport(raftBindAddr, advertiseAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		log.Fatalf("failed to make TCP transport bind %s advertise %s: %s", raftBindAddr, raftAdvertiseAddr, err.Error())
	}
	snapshots, err := raft.NewFileSnapshotStore(cfg.Dir, RetainSnapshotCount, os.Stderr)
	if err != nil {
		log.Fatalf("failed to create snapshot store at %s: %s", cfg.Dir, err.Error())
	}
	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(cfg.Dir, "raft.db"))
	if err != nil {
		log.Fatalf("failed to create new bolt store: %s", err)
	}
	logStore, err := NewLogStore(cfg.Dir)
	if err != nil {
		log.Fatalf("failed to create log store: %s", err)
	}
	ra, err := raft.NewRaft(raftConfig, fsm, logStore, boltDB, snapshots, transport)
	if err != nil {
		return nil, errs.ErrNewRaft(err)
	}
	if cfg.Boostatrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		if err := ra.BootstrapCluster(configuration).Error(); err != nil {
			return nil, errs.ErrBootstrapCluster(err)
		}
	}
	return ra, nil
}

// EnsureSelfInMetadata adds this node to the metadata Nodes map (e.g. bootstrap node).
func (c *Coordinator) EnsureSelfInMetadata() error {
	if c.raft.State() != raft.Leader {
		return nil
	}
	rpcAddr, err := c.cfg.RPCAddr()
	if err != nil {
		return err
	}
	return c.ApplyNodeAddEvent(c.cfg.NodeConfig.ID, c.cfg.RaftConfig.Address, rpcAddr)
}

func (c *Coordinator) Join(id, raftAddr, rpcAddr string) error {
	c.WaitforRaftReadyWithRetryBackoff(2*time.Second, 5)
	if !c.IsLeader() {
		c.Logger.Error("not leader, skipping join", zap.String("joining_node_id", id), zap.String("raft_addr", raftAddr), zap.String("rpc_addr", rpcAddr))
		return nil
	}
	c.Logger.Info("joining cluster", zap.String("joining_node_id", id), zap.String("raft_addr", raftAddr), zap.String("rpc_addr", rpcAddr))
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
			removeFuture := c.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}
	addFuture := c.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		c.Logger.Error("raft add voter failed", zap.Error(err))
		return err
	}
	c.ApplyNodeAddEvent(id, raftAddr, rpcAddr)
	c.Logger.Info("node joined cluster", zap.String("joined_node_id", id), zap.String("raft_addr", raftAddr), zap.String("rpc_addr", rpcAddr))
	return nil
}

func (c *Coordinator) Leave(id string) error {
	c.WaitforRaftReadyWithRetryBackoff(2*time.Second, 5)
	if !c.IsLeader() {
		c.Logger.Error("not leader, skipping leave", zap.String("leaving_node_id", id))
		return nil
	}
	c.Logger.Info("leaving cluster", zap.String("leaving_node_id", id))
	removeFuture := c.raft.RemoveServer(raft.ServerID(id), 0, 0)
	if err := removeFuture.Error(); err != nil {
		c.Logger.Error("raft remove server failed", zap.Error(err))
		return err
	}
	c.ApplyNodeRemoveEvent(id)
	return nil
}

func (c *Coordinator) IsLeader() bool {
	return c.raft.State() == raft.Leader
}

func (c *Coordinator) GetRaftLeaderNodeID() (string, error) {
	if !c.IsLeader() {
		return "", fmt.Errorf("not leader")
	}
	_, id := c.raft.LeaderWithID()
	return string(id), nil
}

func (c *Coordinator) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out waiting for leader")
		case <-ticker.C:
			if c.IsLeader() {
				return nil
			}
		}
	}
}

func (c *Coordinator) WaitforRaftReadyWithRetryBackoff(timeout time.Duration, retryCount int) error {
	timeoutc := time.After(timeout)
	backoff := time.Duration(1 * time.Second)
	for i := 0; i < retryCount; i++ {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out waiting for leader")
		default:
			if c.raft.Leader() != "" {
				return nil
			}
			time.Sleep(backoff)
			backoff *= 2
		}
	}
	return fmt.Errorf("timed out waiting for leader")
}

func (c *Coordinator) Start() error {
	return nil
}

func (c *Coordinator) Shutdown() error {
	c.Logger.Info("node shutting down")
	f := c.raft.Shutdown()
	return f.Error()
}
