package main

import (
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/mohitkumar/mlog/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func main() {
	var (
		addr      string
		dataDir   string
		nodeID    string
		peers     []string
		raftAddr  string
		bootstrap bool
	)

	rootCmd := &cobra.Command{
		Use:   "server",
		Short: "Run the mlog TCP transport server",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := buildConfig(addr, dataDir, nodeID, peers, raftAddr, bootstrap)
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
			return nil
		},
	}

	rootCmd.Flags().StringVar(&addr, "addr", "127.0.0.1:9092", "TCP listen address")
	rootCmd.Flags().StringVar(&dataDir, "data-dir", "/tmp/mlog", "data directory")
	rootCmd.Flags().StringVar(&nodeID, "node-id", "node-1", "node ID")
	rootCmd.Flags().StringSliceVar(&peers, "peer", nil, "peer nodes (nodeID=addr) for discovery join, repeatable")
	rootCmd.Flags().StringVar(&raftAddr, "raft-addr", "127.0.0.1:9093", "Raft transport address")
	rootCmd.Flags().BoolVar(&bootstrap, "bootstrap", false, "Bootstrap the Raft cluster")

	viper.SetEnvPrefix("mlog")
	viper.AutomaticEnv()
	viper.BindPFlag("addr", rootCmd.Flags().Lookup("addr"))
	viper.BindPFlag("data_dir", rootCmd.Flags().Lookup("data-dir"))
	viper.BindPFlag("node_id", rootCmd.Flags().Lookup("node-id"))
	viper.BindPFlag("peers", rootCmd.Flags().Lookup("peer"))
	viper.BindPFlag("raft_addr", rootCmd.Flags().Lookup("raft-addr"))
	viper.BindPFlag("bootstrap", rootCmd.Flags().Lookup("bootstrap"))

	if viper.IsSet("addr") {
		addr = viper.GetString("addr")
	}
	if viper.IsSet("data_dir") {
		dataDir = viper.GetString("data_dir")
	}
	if viper.IsSet("node_id") {
		nodeID = viper.GetString("node_id")
	}
	if viper.IsSet("peers") {
		peers = viper.GetStringSlice("peers")
	}
	if viper.IsSet("raft_addr") {
		raftAddr = viper.GetString("raft_addr")
	}
	if viper.IsSet("bootstrap") {
		bootstrap = viper.GetBool("bootstrap")
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func buildConfig(addr, dataDir, nodeID string, peers []string, raftAddr string, bootstrap bool) (config.Config, error) {
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return config.Config{}, fmt.Errorf("invalid addr %q: %w", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return config.Config{}, fmt.Errorf("invalid port in addr %q: %w", addr, err)
	}
	startJoinAddrs := make([]string, 0, len(peers))
	for _, p := range peers {
		var peerNode, peerAddr string
		if n, _ := fmt.Sscanf(p, "%[^=]=%s", &peerNode, &peerAddr); n == 2 {
			startJoinAddrs = append(startJoinAddrs, peerAddr)
		}
	}
	return config.Config{
		BindAddr:       addr,
		StartJoinAddrs: startJoinAddrs,
		NodeConfig: config.NodeConfig{
			ID:      nodeID,
			RPCPort: port,
			DataDir: dataDir,
		},
		RaftConfig: config.RaftConfig{
			ID:         nodeID,
			Address:    raftAddr,
			Dir:        dataDir,
			Boostatrap: bootstrap,
		},
	}, nil
}
