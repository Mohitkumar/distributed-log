package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/mohitkumar/mlog/config"
	"github.com/mohitkumar/mlog/consumer"
	"github.com/mohitkumar/mlog/coordinator"
	"github.com/mohitkumar/mlog/discovery"
	"github.com/mohitkumar/mlog/rpc"
	"github.com/mohitkumar/mlog/topic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// RaftReadyTimeout is how long to wait for a Raft leader before restoring topic manager (restart/replay can take time).
const RaftReadyTimeout = 15 * time.Second

type CommandHelper struct {
	config.Config
	membership   *discovery.Membership
	coord        *coordinator.Coordinator
	topicMgr     *topic.TopicManager
	rpcServer    *rpc.RpcServer
	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

func NewCommandHelper(config config.Config) (*CommandHelper, error) {
	cmdHelper := &CommandHelper{
		Config:    config,
		shutdowns: make(chan struct{}),
	}
	if err := cmdHelper.setupCoordinator(); err != nil {
		return nil, err
	}
	if err := cmdHelper.setupTopicManager(); err != nil {
		return nil, err
	}
	if err := cmdHelper.setupRpcServer(); err != nil {
		return nil, err
	}
	if err := cmdHelper.setupMembership(); err != nil {
		return nil, err
	}
	return cmdHelper, nil
}

func (cmdHelper *CommandHelper) setupCoordinator() error {
	enc := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())
	core := zapcore.NewCore(enc, zapcore.AddSync(os.Stdout), zapcore.InfoLevel)
	logger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(0))
	logger = logger.With(zap.String("node_id", cmdHelper.NodeConfig.ID))
	zap.ReplaceGlobals(logger)
	coord, err := coordinator.NewCoordinatorFromConfig(cmdHelper.Config, logger)
	if err != nil {
		logger.Sync()
		return err
	}
	cmdHelper.coord = coord
	if cmdHelper.RaftConfig.Boostatrap {
		if err := cmdHelper.coord.WaitForLeader(30 * time.Second); err != nil {
			return fmt.Errorf("wait for leader: %w", err)
		}
	}
	if cmdHelper.coord.IsLeader() {
		if err := cmdHelper.coord.EnsureSelfInMetadata(); err != nil {
			return fmt.Errorf("ensure self in metadata: %w", err)
		}
	}
	return nil
}

func (cmdHelper *CommandHelper) setupTopicManager() error {
	topicMgr, err := topic.NewTopicManager(cmdHelper.NodeConfig.DataDir, cmdHelper.coord, cmdHelper.coord.Logger)
	if err != nil {
		return fmt.Errorf("create topic manager: %w", err)
	}
	cmdHelper.topicMgr = topicMgr
	if err := cmdHelper.coord.WaitforRaftReadyWithRetryBackoff(RaftReadyTimeout, 2); err != nil {
		cmdHelper.coord.Logger.Warn("Raft not ready before restore (continuing anyway)", zap.Error(err))
	}
	if err := topicMgr.RestoreFromMetadata(); err != nil {
		return fmt.Errorf("restore topic manager from metadata: %w", err)
	}
	return nil
}

func (cmdHelper *CommandHelper) setupRpcServer() error {
	listenAddr, err := cmdHelper.Config.RPCListenAddr()
	if err != nil {
		return err
	}
	consumerMgr, err := consumer.NewConsumerManager(cmdHelper.NodeConfig.DataDir)
	if err != nil {
		return fmt.Errorf("create consumer manager: %w", err)
	}
	cmdHelper.rpcServer = rpc.NewRpcServer(listenAddr, cmdHelper.topicMgr, consumerMgr)
	return nil
}

func (cmdHelper *CommandHelper) setupMembership() error {
	membership, err := discovery.New(cmdHelper.coord, cmdHelper.Config)
	if err != nil {
		return err
	}
	cmdHelper.membership = membership
	return nil
}

func (cmdHelper *CommandHelper) Start() error {
	if err := cmdHelper.coord.Start(); err != nil {
		return fmt.Errorf("start coordinator: %w", err)
	}
	return cmdHelper.rpcServer.Start()
}

func (cmdHelper *CommandHelper) Shutdown() error {
	cmdHelper.shutdownLock.Lock()
	defer cmdHelper.shutdownLock.Unlock()
	if cmdHelper.shutdown {
		return nil
	}
	cmdHelper.shutdown = true
	close(cmdHelper.shutdowns)
	if cmdHelper.membership != nil {
		cmdHelper.membership.Leave()
	}
	if cmdHelper.rpcServer != nil {
		_ = cmdHelper.rpcServer.Stop()
	}
	return cmdHelper.coord.Shutdown()
}
