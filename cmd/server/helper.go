package main

import (
	"fmt"
	"net"
	"sync"

	"github.com/mohitkumar/mlog/config"
	"github.com/mohitkumar/mlog/discovery"
	"github.com/mohitkumar/mlog/node"
	"github.com/soheilhy/cmux"
)

type CommandHelper struct {
	config.Config
	mux          cmux.CMux
	membership   *discovery.Membership
	node         *node.Node
	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

func (cmdHelper *CommandHelper) setupMux() error {
	rpcAddr := fmt.Sprintf(
		":%d",
		cmdHelper.Config.NodeConfig.RPCPort,
	)
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	cmdHelper.mux = cmux.New(ln)
	return nil
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
