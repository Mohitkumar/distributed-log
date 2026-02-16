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
	var addrs string

	rootCmd := &cobra.Command{
		Use:   "topic",
		Short: "Topic management: create and delete topics",
	}

	rootCmd.PersistentFlags().StringVar(&addrs, "addrs", "127.0.0.1:9094", "Comma-separated RPC addresses to try for discovery (tried in order until one returns Raft leader)")

	viper.SetEnvPrefix("mlog")
	viper.AutomaticEnv()
	viper.BindPFlag("addrs", rootCmd.PersistentFlags().Lookup("addrs"))
	if viper.IsSet("addrs") {
		addrs = viper.GetString("addrs")
	}

	addrList := func() []string { return strings.Split(addrs, ",") }

	createCmd := &cobra.Command{
		Use:   "create",
		Short: "Create a topic on the cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			topicName, _ := cmd.Flags().GetString("topic")
			replicas, _ := cmd.Flags().GetUint32("replicas")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			raftLeaderAddr, err := client.TryAddrs(ctx, addrList(), func(c *client.RemoteClient) (string, error) {
				resp, err := c.FindRaftLeader(ctx, &protocol.FindRaftLeaderRequest{})
				if err != nil {
					return "", err
				}
				if resp.RaftLeaderAddr == "" {
					return "", fmt.Errorf("empty raft leader address")
				}
				return resp.RaftLeaderAddr, nil
			})
			if err != nil {
				return fmt.Errorf("get raft leader: %w", err)
			}
			raftLeaderClient, err := client.NewRemoteClient(raftLeaderAddr)
			if err != nil {
				return fmt.Errorf("connect to raft leader: %w", err)
			}
			defer raftLeaderClient.Close()
			resp, err := raftLeaderClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
				Topic:        topicName,
				ReplicaCount: replicas,
			})
			if err != nil {
				return err
			}
			fmt.Printf("topic=%s\n", resp.Topic)
			return nil
		},
	}
	createCmd.Flags().String("topic", "", "topic name (required)")
	createCmd.Flags().Uint32("replicas", 1, "replica count")
	createCmd.MarkFlagRequired("topic")
	rootCmd.AddCommand(createCmd)

	deleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete a topic from the cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			topicName, _ := cmd.Flags().GetString("topic")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			raftLeaderAddr, err := client.TryAddrs(ctx, addrList(), func(c *client.RemoteClient) (string, error) {
				resp, err := c.FindRaftLeader(ctx, &protocol.FindRaftLeaderRequest{})
				if err != nil {
					return "", err
				}
				if resp.RaftLeaderAddr == "" {
					return "", fmt.Errorf("empty raft leader address")
				}
				return resp.RaftLeaderAddr, nil
			})
			if err != nil {
				return fmt.Errorf("get raft leader: %w", err)
			}
			raftLeaderClient, err := client.NewRemoteClient(raftLeaderAddr)
			if err != nil {
				return fmt.Errorf("connect to raft leader: %w", err)
			}
			defer raftLeaderClient.Close()
			resp, err := raftLeaderClient.DeleteTopic(ctx, &protocol.DeleteTopicRequest{Topic: topicName})
			if err != nil {
				return err
			}
			fmt.Printf("topic=%s\n", resp.Topic)
			return nil
		},
	}
	deleteCmd.Flags().String("topic", "", "topic name (required)")
	deleteCmd.MarkFlagRequired("topic")
	rootCmd.AddCommand(deleteCmd)

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List topics with leader and replica info",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var listErr error
			for _, addr := range addrList() {
				addr = strings.TrimSpace(addr)
				if addr == "" {
					continue
				}
				c, err := client.NewRemoteClient(addr)
				if err != nil {
					listErr = err
					continue
				}
				resp, err := c.ListTopics(ctx, &protocol.ListTopicsRequest{})
				_ = c.Close()
				if err != nil {
					listErr = err
					continue
				}
				if len(resp.Topics) == 0 {
					fmt.Println("(no topics)")
					return nil
				}
				for _, t := range resp.Topics {
					fmt.Printf("topic=%s leader=%s epoch=%d", t.Name, t.LeaderNodeID, t.LeaderEpoch)
					if len(t.Replicas) > 0 {
						fmt.Print(" replicas=")
						for i, r := range t.Replicas {
							if i > 0 {
								fmt.Print(",")
							}
							fmt.Printf("%s(isr=%v,leo=%d)", r.NodeID, r.IsISR, r.LEO)
						}
					}
					fmt.Println()
				}
				return nil
			}
			if listErr != nil {
				return fmt.Errorf("list topics: %w", listErr)
			}
			return fmt.Errorf("could not connect to any address")
		},
	}
	rootCmd.AddCommand(listCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
