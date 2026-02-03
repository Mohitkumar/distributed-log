package main

import (
	"sync"

	"github.com/mohitkumar/mlog/config"
	"github.com/mohitkumar/mlog/discovery"
	"github.com/mohitkumar/mlog/node"
)

type CommandHelper struct {
	config.Config
	membership   *discovery.Membership
	node         *node.Node
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
	return cmdHelper, nil
}

func (cmdHelper *CommandHelper) setupNode() error {
	node, err := node.NewNodeFromConfig(cmdHelper.Config)
	if err != nil {
		return err
	}
	cmdHelper.node = node
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
	return cmdHelper.node.Start()
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
	return cmdHelper.node.Shutdown()
}
