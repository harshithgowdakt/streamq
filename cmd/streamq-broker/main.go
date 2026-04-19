package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/harshithgowda/streamq/internal/broker"
	"github.com/harshithgowda/streamq/internal/server"
)

func main() {
	addr := flag.String("addr", ":9092", "broker listen address")
	dataDir := flag.String("data-dir", "/tmp/streamq-data", "data directory")
	defaultPartitions := flag.Int("default-partitions", 1, "default number of partitions for auto-created topics")
	maxSegmentBytes := flag.Int64("max-segment-bytes", 1024*1024*1024, "max segment size in bytes")
	autoCreate := flag.Bool("auto-create-topics", true, "auto-create topics on produce")
	flag.Parse()

	cfg := broker.Config{
		DataDir:           *dataDir,
		DefaultPartitions: int32(*defaultPartitions),
		MaxSegmentBytes:   *maxSegmentBytes,
		AutoCreateTopics:  *autoCreate,
		Addr:              *addr,
	}

	b := broker.NewBroker(cfg)

	srv := server.NewServer(b)
	if err := srv.Start(*addr); err != nil {
		log.Fatalf("failed to start: %v", err)
	}

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("received %v, shutting down...", sig)

	srv.Stop()
	if err := b.Close(); err != nil {
		log.Printf("error closing broker: %v", err)
	}
	log.Println("broker stopped")
}
