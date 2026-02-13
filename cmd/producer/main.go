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
		addr  string
		topic string
		acks  int32
	)

	rootCmd := &cobra.Command{
		Use:   "producer",
		Short: "Producer client for mlog",
	}

	rootCmd.PersistentFlags().StringVar(&addr, "addr", "127.0.0.1:9094", "RPC server address (use 9094 for node1; when cluster in Docker, use localhost:9094)")
	rootCmd.PersistentFlags().StringVar(&topic, "topic", "", "topic name (required)")
	rootCmd.PersistentFlags().Int32Var(&acks, "acks", int32(protocol.AckLeader), "acks: 0=none,1=leader,2=all")

	viper.SetEnvPrefix("mlog")
	viper.AutomaticEnv()
	viper.BindPFlag("addr", rootCmd.PersistentFlags().Lookup("addr"))
	if viper.IsSet("addr") {
		addr = viper.GetString("addr")
	}

	rootCmd.MarkPersistentFlagRequired("topic")

	var createTopicName string
	var replicas uint32
	createTopicCmd := &cobra.Command{
		Use:   "create-topic",
		Short: "Create a topic on the cluster (metadata RPC to get Raft leader, then CreateTopic on Raft leader)",
		RunE: func(cmd *cobra.Command, args []string) error {
			remoteClient, err := client.NewRemoteClient(addr)
			if err != nil {
				return err
			}
			defer remoteClient.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			// Metadata RPC: get Raft leader address (like produce uses FindLeader for topic leader).
			raftLeaderResp, err := remoteClient.FindRaftLeader(ctx, &protocol.FindRaftLeaderRequest{})
			if err != nil {
				return fmt.Errorf("get raft leader: %w", err)
			}
			if raftLeaderResp.RaftLeaderAddr == "" {
				return fmt.Errorf("empty raft leader address")
			}
			// Send CreateTopic to the Raft leader; it selects topic leader + replicas and applies a Raft event.
			raftLeaderClient, err := client.NewRemoteClient(raftLeaderResp.RaftLeaderAddr)
			if err != nil {
				return fmt.Errorf("connect to raft leader: %w", err)
			}
			defer raftLeaderClient.Close()
			resp, err := raftLeaderClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
				Topic:        createTopicName,
				ReplicaCount: replicas,
			})
			if err != nil {
				return err
			}
			fmt.Printf("topic=%s\n", resp.Topic)
			return nil
		},
	}
	createTopicCmd.Flags().StringVar(&createTopicName, "topic", "", "topic name (required)")
	createTopicCmd.Flags().Uint32Var(&replicas, "replicas", 1, "replica count")
	createTopicCmd.MarkFlagRequired("topic")
	rootCmd.AddCommand(createTopicCmd)

	connectCmd := &cobra.Command{
		Use:   "connect",
		Short: "Connect to the topic leader and produce messages from stdin",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Remote client talks to the node specified by --addr for discovery (FindLeader).
			remoteClient, err := client.NewRemoteClient(addr)
			if err != nil {
				return err
			}
			defer remoteClient.Close()

			ackMode := protocol.AckMode(acks)
			if ackMode != protocol.AckNone && ackMode != protocol.AckLeader && ackMode != protocol.AckAll {
				ackMode = protocol.AckLeader
			}

			// Helper to resolve the current topic leader RPC address.
			findLeader := func(ctx context.Context) (string, error) {
				resp, err := remoteClient.FindTopicLeader(ctx, &protocol.FindTopicLeaderRequest{Topic: topic})
				if err != nil {
					return "", err
				}
				if resp.LeaderAddr == "" {
					return "", fmt.Errorf("empty leader address returned for topic %s", topic)
				}
				return resp.LeaderAddr, nil
			}

			ctx := context.Background()

			// Initial leader discovery and connection.
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

					// On leader change or connection failure, re-resolve leader and reconnect.
					if client.ShouldReconnect(err) {
						fmt.Fprintln(os.Stderr, "reconnecting (leader change or connection issue)...")
						leaderCtx, cancelLeader := context.WithTimeout(ctx, 10*time.Second)
						newLeaderAddr, findErr := findLeader(leaderCtx)
						cancelLeader()
						if findErr != nil {
							return findErr
						}

						_ = producerClient.Close()
						producerClient, findErr = client.NewProducerClient(newLeaderAddr)
						if findErr != nil {
							return findErr
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
