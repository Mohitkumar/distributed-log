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
		addrs         string
		id            string
		topic         string
		offset        uint64
		fromBeginning bool
	)

	rootCmd := &cobra.Command{
		Use:   "consumer",
		Short: "Consumer client for mlog",
	}

	rootCmd.PersistentFlags().StringVar(&addrs, "addrs", "127.0.0.1:9092", "Comma-separated RPC addresses to try for discovery (tried in order until one connects)")
	rootCmd.PersistentFlags().StringVar(&id, "id", "default", "consumer id")
	rootCmd.PersistentFlags().StringVar(&topic, "topic", "", "topic name (required)")
	rootCmd.PersistentFlags().Uint64Var(&offset, "offset", 0, "start from specific offset (default: resume from last committed)")
	rootCmd.PersistentFlags().BoolVar(&fromBeginning, "from-beginning", false, "start from offset 0 instead of last committed offset")

	viper.SetEnvPrefix("mlog")
	viper.AutomaticEnv()
	viper.BindPFlag("addrs", rootCmd.PersistentFlags().Lookup("addrs"))
	if viper.IsSet("addrs") {
		addrs = viper.GetString("addrs")
	}

	rootCmd.MarkPersistentFlagRequired("topic")

	addrList := func() []string { return strings.Split(addrs, ",") }

	connectCmd := &cobra.Command{
		Use:   "connect",
		Short: "Connect to the topic leader and consume messages (streaming)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Resolve topic leader by trying each --addrs until one returns the leader.
			findLeader := func(ctx context.Context) (string, error) {
				return client.TryAddrs(ctx, addrList(), func(c *client.RemoteClient) (string, error) {
					resp, err := c.FindTopicLeader(ctx, &protocol.FindTopicLeaderRequest{Topic: topic})
					if err != nil {
						return "", err
					}
					if resp.LeaderAddr == "" {
						return "", fmt.Errorf("empty leader address returned for topic %s", topic)
					}
					return resp.LeaderAddr, nil
				})
			}

			leaderCtx, leaderCancel := context.WithTimeout(ctx, 5*time.Second)
			leaderAddr, err := findLeader(leaderCtx)
			leaderCancel()
			if err != nil {
				return err
			}

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
					// On leader change or connection failure, try addrs again and re-resolve leader.
					if client.ShouldReconnect(err) {
						fmt.Fprintln(os.Stderr, "reconnecting (leader change or connection issue)...")
						leaderCtx, leaderCancel := context.WithTimeout(ctx, 5*time.Second)
						newLeaderAddr, findErr := findLeader(leaderCtx)
						leaderCancel()
						if findErr != nil {
							return findErr
						}
						leaderAddr = newLeaderAddr

						_ = consumerClient.Close()
						consumerClient, findErr = client.NewConsumerClient(leaderAddr)
						if findErr != nil {
							return findErr
						}
						fmt.Fprintf(os.Stderr, "reconnected to topic leader at %s\n", leaderAddr)
						continue
					}
					return err
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
