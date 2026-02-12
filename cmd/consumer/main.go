package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/protocol"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func main() {
	var (
		addr          string
		id            string
		topic         string
		offset        uint64
		fromBeginning bool
	)

	rootCmd := &cobra.Command{
		Use:   "consumer",
		Short: "Consumer client for mlog",
	}

	rootCmd.PersistentFlags().StringVar(&addr, "addr", "127.0.0.1:9092", "TCP server address (discovery node)")
	rootCmd.PersistentFlags().StringVar(&id, "id", "default", "consumer id")
	rootCmd.PersistentFlags().StringVar(&topic, "topic", "", "topic name (required)")
	rootCmd.PersistentFlags().Uint64Var(&offset, "offset", 0, "start from specific offset (default: resume from last committed)")
	rootCmd.PersistentFlags().BoolVar(&fromBeginning, "from-beginning", false, "start from offset 0 instead of last committed offset")

	viper.SetEnvPrefix("mlog")
	viper.AutomaticEnv()
	viper.BindPFlag("addr", rootCmd.PersistentFlags().Lookup("addr"))
	if viper.IsSet("addr") {
		addr = viper.GetString("addr")
	}

	rootCmd.MarkPersistentFlagRequired("topic")

	connectCmd := &cobra.Command{
		Use:   "connect",
		Short: "Connect to the topic leader and consume messages (streaming)",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Remote client talks to the node specified by --addr for discovery (FindLeader).
			remoteClient, err := client.NewRemoteClient(addr)
			if err != nil {
				return err
			}
			defer remoteClient.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Resolve topic leader RPC address.
			leaderCtx, leaderCancel := context.WithTimeout(ctx, 5*time.Second)
			leaderResp, err := remoteClient.FindLeader(leaderCtx, &protocol.FindLeaderRequest{Topic: topic})
			leaderCancel()
			if err != nil {
				return err
			}
			if leaderResp.LeaderAddr == "" {
				return fmt.Errorf("empty leader address returned for topic %s", topic)
			}
			leaderAddr := leaderResp.LeaderAddr

			consumerClient, err := client.NewConsumerClient(leaderAddr)
			if err != nil {
				return err
			}
			defer consumerClient.Close()

			fmt.Fprintf(os.Stderr, "connected to topic %q leader at %s\n", topic, leaderAddr)

			offsetExplicitlySet := cmd.Flags().Changed("offset")
			startOffset := offset

			if fromBeginning {
				fmt.Fprintf(os.Stderr, "Starting from beginning (offset 0)\n")
				startOffset = 0
			} else if offsetExplicitlySet {
				fmt.Fprintf(os.Stderr, "Starting from offset %d (explicitly specified)\n", startOffset)
			} else {
				fetchCtx, fetchCancel := context.WithTimeout(ctx, 5*time.Second)
				resp, err := consumerClient.FetchOffset(fetchCtx, &protocol.FetchOffsetRequest{
					Id:    id,
					Topic: topic,
				})
				fetchCancel()
				if err == nil && resp != nil && resp.Offset > 0 {
					startOffset = resp.Offset
					fmt.Fprintf(os.Stderr, "Resuming from offset %d (last committed)\n", startOffset)
				} else {
					fmt.Fprintf(os.Stderr, "No previous offset found, starting from beginning\n")
					startOffset = 0
				}
			}

			currentOffset := startOffset

			for {
				resp, err := consumerClient.Fetch(ctx, &protocol.FetchRequest{
					Id:     id,
					Topic:  topic,
					Offset: currentOffset,
				})
				if err != nil {
					// Handle leader failover: if the node we are connected to is no longer leader,
					// re-resolve the topic leader via FindLeader and reconnect, then resume.
					msg := err.Error()
					if strings.Contains(msg, "not the topic leader") || strings.Contains(msg, "this node is not leader") {
						fmt.Fprintln(os.Stderr, "current node is no longer the topic leader; looking up new leader...")
						leaderCtx, leaderCancel := context.WithTimeout(ctx, 5*time.Second)
						leaderResp, findErr := remoteClient.FindLeader(leaderCtx, &protocol.FindLeaderRequest{Topic: topic})
						leaderCancel()
						if findErr != nil {
							return findErr
						}
						if leaderResp.LeaderAddr == "" {
							return fmt.Errorf("empty leader address returned for topic %s", topic)
						}
						leaderAddr = leaderResp.LeaderAddr

						_ = consumerClient.Close()
						consumerClient, findErr = client.NewConsumerClient(leaderAddr)
						if findErr != nil {
							return findErr
						}
						fmt.Fprintf(os.Stderr, "reconnected to new topic leader at %s\n", leaderAddr)
						continue
					}
					continue
				}
				if resp.Entry == nil {
					continue
				}

				// Print to stdout: "offset\tvalue"
				fmt.Printf("%d\t%s\n", resp.Entry.Offset, string(resp.Entry.Value))

				currentOffset = resp.Entry.Offset + 1

				commitCtx, commitCancel := context.WithTimeout(ctx, 5*time.Second)
				_, _ = consumerClient.CommitOffset(commitCtx, &protocol.CommitOffsetRequest{
					Id:     id,
					Topic:  topic,
					Offset: currentOffset,
				})
				commitCancel()
			}
		},
	}
	rootCmd.AddCommand(connectCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
