package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/harshithgowda/streamq/pkg/consumer"
)

func main() {
	brokerAddr := flag.String("broker", "localhost:9092", "broker address")
	topic := flag.String("topic", "", "topic name (required)")
	partition := flag.Int("partition", 0, "partition number")
	offset := flag.Int64("offset", 0, "starting offset")
	follow := flag.Bool("follow", false, "continuously poll for new messages")
	flag.Parse()

	if *topic == "" {
		fmt.Fprintln(os.Stderr, "error: --topic is required")
		flag.Usage()
		os.Exit(1)
	}

	c, err := consumer.NewConsumer(consumer.Config{
		BrokerAddr: *brokerAddr,
		ClientID:   "streamq-cli-consumer",
		MaxBytes:   1024 * 1024,
	})
	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}
	defer c.Close()

	c.Subscribe(*topic, int32(*partition), *offset)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	for {
		messages, err := c.Poll()
		if err != nil {
			log.Fatalf("poll: %v", err)
		}

		for _, msg := range messages {
			fmt.Printf("offset=%d value=%s\n", msg.Offset, string(msg.Value))
		}

		if !*follow {
			break
		}

		select {
		case <-sigCh:
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}
