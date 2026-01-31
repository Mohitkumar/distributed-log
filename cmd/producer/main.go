package main

import (
	"context"
	"fmt"
	"os"
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
		value string
		acks  int32
	)

	rootCmd := &cobra.Command{
		Use:   "producer",
		Short: "Produce messages to a topic",
		RunE: func(cmd *cobra.Command, args []string) error {
			producerClient, err := client.NewProducerClient(addr)
			if err != nil {
				return err
			}
			defer producerClient.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			ackMode := protocol.AckMode(acks)
			if ackMode != protocol.AckNone && ackMode != protocol.AckLeader && ackMode != protocol.AckAll {
				ackMode = protocol.AckLeader
			}

			resp, err := producerClient.Produce(ctx, &protocol.ProduceRequest{
				Topic: topic,
				Value: []byte(value),
				Acks:  ackMode,
			})
			if err != nil {
				return err
			}

			fmt.Printf("offset=%d\n", resp.Offset)
			return nil
		},
	}

	rootCmd.Flags().StringVar(&addr, "addr", "127.0.0.1:9092", "TCP server address")
	rootCmd.Flags().StringVar(&topic, "topic", "", "topic name (required)")
	rootCmd.Flags().StringVar(&value, "value", "", "message value (required)")
	rootCmd.Flags().Int32Var(&acks, "acks", int32(protocol.AckLeader), "acks: 0=none,1=leader,2=all")

	viper.SetEnvPrefix("mlog")
	viper.AutomaticEnv()
	viper.BindPFlag("addr", rootCmd.Flags().Lookup("addr"))
	if viper.IsSet("addr") {
		addr = viper.GetString("addr")
	}

	rootCmd.MarkFlagRequired("topic")
	rootCmd.MarkFlagRequired("value")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
