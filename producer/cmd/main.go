package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mohitkumar/mlog/api/protocol"
	producerclient "github.com/mohitkumar/mlog/producer/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func main() {
	var (
		addrs string
		topic string
		acks  int32
	)

	rootCmd := &cobra.Command{
		Use:   "producer",
		Short: "Producer client for mlog",
	}

	rootCmd.PersistentFlags().StringVar(&addrs, "addrs", "127.0.0.1:9094", "Comma-separated RPC addresses to try for discovery (tried in order until one connects)")
	rootCmd.PersistentFlags().StringVar(&topic, "topic", "", "topic name (required)")
	rootCmd.PersistentFlags().Int32Var(&acks, "acks", int32(protocol.AckLeader), "acks: 0=none,1=leader,2=all")

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
		Short: "Connect to the topic leader and produce messages from stdin",
		RunE: func(cmd *cobra.Command, args []string) error {
			ackMode := protocol.AckMode(acks)
			if ackMode != protocol.AckNone && ackMode != protocol.AckLeader && ackMode != protocol.AckAll {
				ackMode = protocol.AckLeader
			}

			ctx := context.Background()

			// NewClient discovers the topic leader among --addrs and connects; Client
			// itself handles re-discovery and reconnecting on failover from here on,
			// so this command never has to.
			connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			c, err := producerclient.NewClient(connectCtx, addrList(), topic)
			cancel()
			if err != nil {
				return err
			}
			defer c.Close()
			c.OnReconnect = func(addr string) {
				fmt.Fprintf(os.Stderr, "reconnected to topic %q leader at %s\n", topic, addr)
			}

			fmt.Fprintf(os.Stderr, "connected to topic %q leader at %s\n", topic, c.LeaderAddr())
			fmt.Fprintln(os.Stderr, "enter messages, each line will be produced to the topic (Ctrl-D to exit)")

			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				line := strings.TrimRight(scanner.Text(), "\r\n")
				// Treat empty lines as no-op.
				if line == "" {
					continue
				}

				msgCtx, cancelMsg := context.WithTimeout(ctx, 10*time.Second)
				offset, err := c.Send(msgCtx, []byte(line), ackMode)
				cancelMsg()
				if err != nil {
					return err
				}
				fmt.Printf("offset=%d\n", offset)
			}

			if err := scanner.Err(); err != nil {
				return err
			}
			return nil
		},
	}
	rootCmd.AddCommand(connectCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
