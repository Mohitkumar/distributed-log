package main

import (
	"fmt"
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
		bindAddr  string
		rpcPort   int
		dataDir   string
		nodeID    string
		peers     []string
		raftAddr  string
		bootstrap bool
	)

	rootCmd := &cobra.Command{
		Use:   "server",
		Short: "Run the mlog TCP transport server",
		PreRun: func(cmd *cobra.Command, args []string) {
			// Read final values from viper (env + flags). No IsSet checks needed
			// because BoundPFlags already provide defaults.
			bindAddr = viper.GetString("bind-addr")
			dataDir = viper.GetString("data_dir")
			nodeID = viper.GetString("node_id")
			raftAddr = viper.GetString("raft_addr")
			bootstrap = viper.GetBool("bootstrap")
			rpcPort = viper.GetInt("rpc-port")
			// NOTE: peers intentionally come only from Cobra flags (no viper override),
			// because viper empty-slice behavior can clobber --peer.
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := buildConfig(bindAddr, rpcPort, dataDir, nodeID, peers, raftAddr, bootstrap)
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

	rootCmd.Flags().StringVar(&bindAddr, "bind-addr", "127.0.0.1:9092", "TCP listen address")
	rootCmd.Flags().IntVar(&rpcPort, "rpc-port", 9094, "RPC listen port")
	rootCmd.Flags().StringVar(&dataDir, "data-dir", "/tmp/mlog", "data directory")
	rootCmd.Flags().StringVar(&nodeID, "node-id", "node-1", "node ID")
	rootCmd.Flags().StringSliceVar(&peers, "peer", nil, "peer nodes (nodeID=addr) for discovery join, repeatable")
	rootCmd.Flags().StringVar(&raftAddr, "raft-addr", "127.0.0.1:9093", "Raft transport address")
	rootCmd.Flags().BoolVar(&bootstrap, "bootstrap", false, "Bootstrap the Raft cluster")

	viper.SetEnvPrefix("mlog")
	viper.AutomaticEnv()
	viper.BindPFlag("bind-addr", rootCmd.Flags().Lookup("bind-addr"))
	viper.BindPFlag("data_dir", rootCmd.Flags().Lookup("data-dir"))
	viper.BindPFlag("node_id", rootCmd.Flags().Lookup("node-id"))
	viper.BindPFlag("raft_addr", rootCmd.Flags().Lookup("raft-addr"))
	viper.BindPFlag("bootstrap", rootCmd.Flags().Lookup("bootstrap"))
	viper.BindPFlag("rpc-port", rootCmd.Flags().Lookup("rpc-port"))
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func buildConfig(addr string, rpcPort int, dataDir, nodeID string, peers []string, raftAddr string, bootstrap bool) (config.Config, error) {
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
	return config.Config{
		BindAddr:       addr,
		StartJoinAddrs: startJoinAddrs,
		NodeConfig: config.NodeConfig{
			ID:      nodeID,
			RPCPort: rpcPort,
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
