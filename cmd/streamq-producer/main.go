package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/harshithgowda/streamq/pkg/producer"
)

func main() {
	brokerAddr := flag.String("broker", "localhost:9092", "broker address")
	topic := flag.String("topic", "", "topic name (required)")
	partition := flag.Int("partition", 0, "partition number")
	flag.Parse()

	if *topic == "" {
		fmt.Fprintln(os.Stderr, "error: --topic is required")
		flag.Usage()
		os.Exit(1)
	}

	p, err := producer.NewProducer(producer.Config{
		BrokerAddr: *brokerAddr,
		BatchSize:  100,
		LingerTime: 10 * time.Millisecond,
		ClientID:   "streamq-cli-producer",
	})
	if err != nil {
		log.Fatalf("failed to create producer: %v", err)
	}
	defer p.Close()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		offset, err := p.Send(producer.Message{
			Topic:     *topic,
			Partition: int32(*partition),
			Value:     []byte(line),
		})
		if err != nil {
			log.Fatalf("send failed: %v", err)
		}
		fmt.Fprintf(os.Stderr, "sent offset=%d\n", offset)
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("read stdin: %v", err)
	}
}
