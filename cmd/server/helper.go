package main

import (
	"fmt"
	"sync"

	"github.com/mohitkumar/mlog/config"
	"github.com/mohitkumar/mlog/consumer"
	"github.com/mohitkumar/mlog/discovery"
	"github.com/mohitkumar/mlog/node"
	"github.com/mohitkumar/mlog/rpc"
	"github.com/mohitkumar/mlog/topic"
)

type CommandHelper struct {
	config.Config
	membership   *discovery.Membership
	node         *node.Node
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
	if err := cmdHelper.setupNode(); err != nil {
		return nil, err
	}
	if err := cmdHelper.setupMembership(); err != nil {
		return nil, err
	}
	if err := cmdHelper.setupRpcServer(); err != nil {
		return nil, err
	}
	return cmdHelper, nil
}

func (cmdHelper *CommandHelper) setupNode() error {
	n, err := node.NewNodeFromConfig(cmdHelper.Config)
	if err != nil {
		return err
	}
	cmdHelper.node = n
	return nil
}

func (cmdHelper *CommandHelper) setupRpcServer() error {
	rpcAddr, err := cmdHelper.Config.RPCAddr()
	if err != nil {
		return err
	}
	topicMgr, err := topic.NewTopicManager(cmdHelper.NodeConfig.DataDir, cmdHelper.node)
	if err != nil {
		return fmt.Errorf("create topic manager: %w", err)
	}
	consumerMgr, err := consumer.NewConsumerManager(cmdHelper.NodeConfig.DataDir)
	if err != nil {
		return fmt.Errorf("create consumer manager: %w", err)
	}
	cmdHelper.rpcServer = rpc.NewRpcServer(rpcAddr, topicMgr, consumerMgr)
	return nil
}

func (cmdHelper *CommandHelper) setupMembership() error {
	membership, err := discovery.New(cmdHelper.node, cmdHelper.Config)
	if err != nil {
		return err
	}
	cmdHelper.membership = membership
	return nil
}

func (cmdHelper *CommandHelper) Start() error {
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
	return cmdHelper.node.Shutdown()
}
