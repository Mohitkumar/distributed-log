package config

import (
	"fmt"
	"net"
)

type Config struct {
	BindAddr       string
	NodeConfig     NodeConfig
	RaftConfig     RaftConfig
	StartJoinAddrs []string
}

type RaftConfig struct {
	ID         string
	Address    string
	Dir        string
	Boostatrap bool
}

type NodeConfig struct {
	ID      string
	RPCPort int
	DataDir string
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.NodeConfig.RPCPort), nil
}
