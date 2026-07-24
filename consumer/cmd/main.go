package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	consumerclient "github.com/mohitkumar/mlog/consumer/client"
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

			// NewClient discovers the topic leader among --addrs and connects; Client
			// itself handles re-discovery and reconnecting on failover from here on,
			// so this command never has to.
			connectCtx, connectCancel := context.WithTimeout(ctx, 5*time.Second)
			c, err := consumerclient.NewClient(connectCtx, addrList(), topic, id)
			connectCancel()
			if err != nil {
				return err
			}
			defer c.Close()
			c.OnReconnect = func(addr string) {
				fmt.Fprintf(os.Stderr, "reconnected to topic %q leader at %s\n", topic, addr)
			}

			fmt.Fprintf(os.Stderr, "connected to topic %q leader at %s\n", topic, c.LeaderAddr())

			offsetExplicitlySet := cmd.Flags().Changed("offset")
			startOffset := offset

			if fromBeginning {
				fmt.Fprintf(os.Stderr, "Starting from beginning (offset 0)\n")
				startOffset = 0
			} else if offsetExplicitlySet {
				fmt.Fprintf(os.Stderr, "Starting from offset %d (explicitly specified)\n", startOffset)
			} else {
				fetchCtx, fetchCancel := context.WithTimeout(ctx, 5*time.Second)
				committed, err := c.FetchCommittedOffset(fetchCtx)
				fetchCancel()
				if err == nil && committed > 0 {
					startOffset = committed
					fmt.Fprintf(os.Stderr, "Resuming from offset %d (last committed)\n", startOffset)
				} else {
					fmt.Fprintf(os.Stderr, "No previous offset found, starting from beginning\n")
					startOffset = 0
				}
			}

			currentOffset := startOffset
			pollInterval := 500 * time.Millisecond

			for {
				// Poll blocks internally (backing off pollInterval) while the topic has
				// no new data yet, and reconnects transparently on failover — this
				// command only ever sees "got a record" or "something's actually wrong".
				entry, err := c.Poll(ctx, currentOffset, pollInterval)
				if err != nil {
					return err
				}

				// Print to stdout: "offset\tvalue"
				fmt.Printf("%d\t%s\n", entry.Offset, string(entry.Value))

				currentOffset = entry.Offset + 1

				commitCtx, commitCancel := context.WithTimeout(ctx, 5*time.Second)
				if err := c.Commit(commitCtx, currentOffset); err != nil {
					fmt.Fprintf(os.Stderr, "warning: commit offset %d failed: %v\n", currentOffset, err)
				}
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
