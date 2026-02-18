package main

import (
	"bufio"
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

			ctx := context.Background()

			leaderCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			leaderAddr, err := findLeader(leaderCtx)
			cancel()
			if err != nil {
				return err
			}

			producerClient, err := client.NewProducerClient(leaderAddr)
			if err != nil {
				return err
			}
			defer producerClient.Close()

			fmt.Fprintf(os.Stderr, "connected to topic %q leader at %s\n", topic, leaderAddr)
			fmt.Fprintln(os.Stderr, "enter messages, each line will be produced to the topic (Ctrl-D to exit)")

			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				line := strings.TrimRight(scanner.Text(), "\r\n")
				// Treat empty lines as no-op.
				if line == "" {
					continue
				}

				for {
					msgCtx, cancelMsg := context.WithTimeout(ctx, 10*time.Second)
					resp, err := producerClient.Produce(msgCtx, &protocol.ProduceRequest{
						Topic: topic,
						Value: []byte(line),
						Acks:  ackMode,
					})
					cancelMsg()

					if err == nil {
						fmt.Printf("offset=%d\n", resp.Offset)
						break
					}

					// On leader change or connection failure, re-resolve leader with retries.
					if client.ShouldReconnect(err) {
						fmt.Fprintln(os.Stderr, "reconnecting (leader change or connection issue)...")
						_ = producerClient.Close()

						var newLeaderAddr string
						for attempt := 0; attempt < 10; attempt++ {
							leaderCtx, cancelLeader := context.WithTimeout(ctx, 10*time.Second)
							newLeaderAddr, err = findLeader(leaderCtx)
							cancelLeader()
							if err == nil {
								break
							}
							fmt.Fprintf(os.Stderr, "find leader attempt %d failed: %v\n", attempt+1, err)
							time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
						}
						if err != nil {
							return fmt.Errorf("failed to find new leader after retries: %w", err)
						}

						producerClient, err = client.NewProducerClient(newLeaderAddr)
						if err != nil {
							return err
						}

						fmt.Fprintf(os.Stderr, "reconnected to topic leader at %s\n", newLeaderAddr)
						continue
					}

					return err
				}
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
