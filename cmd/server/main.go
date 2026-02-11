package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/mohitkumar/mlog/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func main() {
	var (
		bindAddr            string
		advertiseAddr       string
		rpcPort             int
		dataDir             string
		nodeID              string
		peers               []string
		raftAddr            string
		bootstrap           bool
		replicationBatchSize uint32
	)

	rootCmd := &cobra.Command{
		Use:   "server",
		Short: "Run the mlog TCP transport server",
		PreRun: func(cmd *cobra.Command, args []string) {
			bindAddr = viper.GetString("bind-addr")
			advertiseAddr = viper.GetString("advertise-addr")
			dataDir = viper.GetString("data_dir")
			nodeID = viper.GetString("node_id")
			raftAddr = viper.GetString("raft_addr")
			bootstrap = viper.GetBool("bootstrap")
			rpcPort = viper.GetInt("rpc-port")
			replicationBatchSize = uint32(viper.GetInt("replication-batch-size"))
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := buildConfig(bindAddr, advertiseAddr, rpcPort, dataDir, nodeID, peers, raftAddr, bootstrap, replicationBatchSize)
			if err != nil {
				return err
			}
			cmdHelper, err := NewCommandHelper(cfg)
			if err != nil {
				return fmt.Errorf("create command helper: %w", err)
			}
			if err := cmdHelper.Start(); err != nil {
				return fmt.Errorf("start server: %w", err)
			}
			// Block until shutdown signal so the process (and container) stays alive.
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
			<-sigCh
			return cmdHelper.Shutdown()
		},
	}

	rootCmd.Flags().StringVar(&bindAddr, "bind-addr", "127.0.0.1:9092", "Serf listen address (use 0.0.0.0 in Docker)")
	rootCmd.Flags().StringVar(&advertiseAddr, "advertise-addr", "", "Address other nodes use to reach this node (e.g. node1). When set, bind 0.0.0.0 for Serf/Raft/RPC")
	rootCmd.Flags().IntVar(&rpcPort, "rpc-port", 9094, "RPC listen port")
	rootCmd.Flags().StringVar(&dataDir, "data-dir", "/tmp/mlog", "data directory")
	rootCmd.Flags().StringVar(&nodeID, "node-id", "node-1", "node ID")
	rootCmd.Flags().StringSliceVar(&peers, "peer", nil, "peer nodes (nodeID=addr) for discovery join, repeatable")
	rootCmd.Flags().StringVar(&raftAddr, "raft-addr", "127.0.0.1:9093", "Raft transport address")
	rootCmd.Flags().BoolVar(&bootstrap, "bootstrap", false, "Bootstrap the Raft cluster")
	rootCmd.Flags().Uint32Var(&replicationBatchSize, "replication-batch-size", 5000, "Max records per topic per replication request")

	viper.SetEnvPrefix("mlog")
	viper.AutomaticEnv()
	viper.BindPFlag("bind-addr", rootCmd.Flags().Lookup("bind-addr"))
	viper.BindPFlag("advertise-addr", rootCmd.Flags().Lookup("advertise-addr"))
	viper.BindPFlag("data_dir", rootCmd.Flags().Lookup("data-dir"))
	viper.BindPFlag("node_id", rootCmd.Flags().Lookup("node-id"))
	viper.BindPFlag("raft_addr", rootCmd.Flags().Lookup("raft-addr"))
	viper.BindPFlag("bootstrap", rootCmd.Flags().Lookup("bootstrap"))
	viper.BindPFlag("rpc-port", rootCmd.Flags().Lookup("rpc-port"))
	viper.BindPFlag("replication-batch-size", rootCmd.Flags().Lookup("replication-batch-size"))
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func buildConfig(bindAddr, advertiseAddr string, rpcPort int, dataDir, nodeID string, peers []string, raftAddr string, bootstrap bool, replicationBatchSize uint32) (config.Config, error) {
	startJoinAddrs := make([]string, 0, len(peers))
	for _, p := range peers {
		parts := strings.SplitN(p, "=", 2)
		if len(parts) != 2 {
			continue
		}
		peerAddr := strings.TrimSpace(parts[1])
		if peerAddr == "" {
			continue
		}
		startJoinAddrs = append(startJoinAddrs, peerAddr)
	}
	raftConfig := config.RaftConfig{
		ID:         nodeID,
		Address:    raftAddr,
		Dir:        dataDir,
		Boostatrap: bootstrap,
	}
	if advertiseAddr != "" {
		_, raftPort, err := net.SplitHostPort(raftAddr)
		if err != nil {
			return config.Config{}, fmt.Errorf("invalid raft-addr %q: %w", raftAddr, err)
		}
		raftConfig.Address = net.JoinHostPort(advertiseAddr, raftPort)
		raftConfig.BindAddress = raftAddr
	}
	return config.Config{
		BindAddr:       bindAddr,
		AdvertiseAddr:  advertiseAddr,
		StartJoinAddrs: startJoinAddrs,
		NodeConfig: config.NodeConfig{
			ID:      nodeID,
			RPCPort: rpcPort,
			DataDir: dataDir,
		},
		RaftConfig: raftConfig,
		Replication: config.ReplicationConfig{
			BatchSize: replicationBatchSize,
		},
	}, nil
}
