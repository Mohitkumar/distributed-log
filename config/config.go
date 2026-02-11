package config

import (
	"fmt"
	"net"
)

type Config struct {
	BindAddr       string
	AdvertiseAddr  string // optional; hostname others use to reach us (e.g. node1). When set, Serf/Raft/RPC advertise this; bind with 0.0.0.0 in Docker.
	NodeConfig     NodeConfig
	RaftConfig     RaftConfig
	Replication   ReplicationConfig
	StartJoinAddrs []string
}

// ReplicationConfig holds replication thread settings.
type ReplicationConfig struct {
	// BatchSize is the max number of records to fetch per topic per replication request (0 = use default).
	BatchSize uint32
}

type RaftConfig struct {
	ID          string
	Address     string // address others use to reach this node's Raft (e.g. node1:9093)
	BindAddress string // optional; listen address (e.g. 0.0.0.0:9093). When empty, listen on Address.
	Dir         string
	Boostatrap  bool
	LogLevel    string
}

type NodeConfig struct {
	ID      string
	RPCPort int
	DataDir string
}

func (c Config) RPCAddr() (string, error) {
	if c.AdvertiseAddr != "" {
		return fmt.Sprintf("%s:%d", c.AdvertiseAddr, c.NodeConfig.RPCPort), nil
	}
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.NodeConfig.RPCPort), nil
}

// RPCListenAddr returns the address the RPC server should bind to. When AdvertiseAddr is set, bind 0.0.0.0 so other nodes can connect.
func (c Config) RPCListenAddr() (string, error) {
	if c.AdvertiseAddr != "" {
		return fmt.Sprintf("0.0.0.0:%d", c.NodeConfig.RPCPort), nil
	}
	return c.RPCAddr()
}
