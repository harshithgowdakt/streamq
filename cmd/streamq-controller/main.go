package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/harshithgowda/streamq/internal/controller"
)

func main() {
	raftAddr := flag.String("raft-addr", ":7000", "Raft peer address")
	rpcAddr := flag.String("rpc-addr", ":7001", "Broker-facing RPC address")
	dataDir := flag.String("data-dir", "/tmp/streamq-controller", "Data directory")
	bootstrap := flag.Bool("bootstrap", false, "Bootstrap new cluster (first node only)")
	peers := flag.String("peers", "", "Comma-separated peer Raft addresses")
	flag.Parse()

	var peerList []string
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
	}

	cfg := controller.Config{
		RaftAddr:  *raftAddr,
		RPCAddr:   *rpcAddr,
		DataDir:   *dataDir,
		Bootstrap: *bootstrap,
		Peers:     peerList,
	}

	ctrl, err := controller.NewController(cfg)
	if err != nil {
		log.Fatalf("failed to start controller: %v", err)
	}

	log.Printf("streamq controller started (raft=%s, rpc=%s)", *raftAddr, *rpcAddr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("received %v, shutting down...", sig)

	if err := ctrl.Shutdown(); err != nil {
		log.Printf("error shutting down controller: %v", err)
	}
	log.Println("controller stopped")
}
