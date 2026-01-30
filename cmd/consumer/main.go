package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/mohitkumar/mlog/client"
	"github.com/mohitkumar/mlog/protocol"
	"github.com/mohitkumar/mlog/transport"
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
		Short: "Consume messages from a topic (streaming)",
		RunE: func(cmd *cobra.Command, args []string) error {
			tr := transport.NewTransport()
			conn, err := tr.Connect(addr)
			if err != nil {
				return err
			}
			defer conn.Close()

			consumerClient := client.NewConsumerClient(conn)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

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

			stream, err := consumerClient.FetchStream(ctx, &protocol.FetchRequest{
				Id:     id,
				Topic:  topic,
				Offset: startOffset,
			})
			if err != nil {
				return err
			}

			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				if resp.Entry == nil {
					continue
				}

				fmt.Printf("%d\t%s\n", resp.Entry.Offset, string(resp.Entry.Value))

				commitCtx, commitCancel := context.WithTimeout(ctx, 5*time.Second)
				_, _ = consumerClient.CommitOffset(commitCtx, &protocol.CommitOffsetRequest{
					Id:     id,
					Topic:  topic,
					Offset: resp.Entry.Offset + 1,
				})
				commitCancel()
			}
		},
	}

	rootCmd.Flags().StringVar(&addr, "addr", "127.0.0.1:9092", "TCP server address")
	rootCmd.Flags().StringVar(&id, "id", "default", "consumer id")
	rootCmd.Flags().StringVar(&topic, "topic", "", "topic name (required)")
	rootCmd.Flags().Uint64Var(&offset, "offset", 0, "start from specific offset (default: resume from last committed)")
	rootCmd.Flags().BoolVar(&fromBeginning, "from-beginning", false, "start from offset 0 instead of last committed offset")

	viper.SetEnvPrefix("mlog")
	viper.AutomaticEnv()
	viper.BindPFlag("addr", rootCmd.Flags().Lookup("addr"))
	if viper.IsSet("addr") {
		addr = viper.GetString("addr")
	}

	rootCmd.MarkFlagRequired("topic")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
