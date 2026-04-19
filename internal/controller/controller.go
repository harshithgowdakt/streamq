package controller

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/harshithgowda/streamq/internal/cluster"
)

// Config holds controller configuration.
type Config struct {
	RaftAddr  string   // Raft peer address
	RPCAddr   string   // Broker-facing RPC address
	DataDir   string   // Data directory for Raft state
	Bootstrap bool     // Bootstrap new cluster
	Peers     []string // Raft peer addresses
}

// Controller manages cluster metadata via Raft consensus.
type Controller struct {
	config    Config
	raft      *raft.Raft
	fsm       *FSM
	rpcServer *RPCServer
	health    *HealthChecker

	transport *raft.NetworkTransport
}

// NewController creates and starts a new Controller.
func NewController(cfg Config) (*Controller, error) {
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	fsm := NewFSM()

	// Raft configuration
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.RaftAddr)
	raftCfg.SnapshotThreshold = 1024
	raftCfg.SnapshotInterval = 30 * time.Second

	// Transport
	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve raft addr: %w", err)
	}
	transport, err := raft.NewTCPTransport(cfg.RaftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("create transport: %w", err)
	}

	// Log store and stable store
	boltPath := filepath.Join(cfg.DataDir, "raft.db")
	boltStore, err := raftboltdb.NewBoltStore(boltPath)
	if err != nil {
		return nil, fmt.Errorf("create bolt store: %w", err)
	}

	// Snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}

	r, err := raft.NewRaft(raftCfg, fsm, boltStore, boltStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("create raft: %w", err)
	}

	// Bootstrap if requested
	if cfg.Bootstrap {
		servers := []raft.Server{
			{
				ID:      raft.ServerID(cfg.RaftAddr),
				Address: raft.ServerAddress(cfg.RaftAddr),
			},
		}
		for _, peer := range cfg.Peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(peer),
				Address: raft.ServerAddress(peer),
			})
		}
		f := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if err := f.Error(); err != nil && err != raft.ErrCantBootstrap {
			return nil, fmt.Errorf("bootstrap: %w", err)
		}
	}

	c := &Controller{
		config:    cfg,
		raft:      r,
		fsm:       fsm,
		transport: transport,
	}

	// Start RPC server
	rpcServer, err := NewRPCServer(cfg.RPCAddr, c)
	if err != nil {
		r.Shutdown()
		return nil, fmt.Errorf("start rpc server: %w", err)
	}
	c.rpcServer = rpcServer

	// Start health checker
	c.health = NewHealthChecker(c, 10*time.Second)
	c.health.Start()

	// Set up metadata push on FSM apply
	fsm.SetOnApply(func(meta *cluster.ClusterMetadata) {
		c.rpcServer.BroadcastMetadata(meta)
	})

	return c, nil
}

// proposeCommand proposes a command to Raft. Returns the result from FSM.Apply.
func (c *Controller) proposeCommand(cmdType string, data interface{}) (interface{}, error) {
	if c.raft.State() != raft.Leader {
		return nil, fmt.Errorf("not the leader")
	}

	rawData, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("marshal data: %w", err)
	}

	cmd := FSMCommand{
		Type: cmdType,
		Data: rawData,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal command: %w", err)
	}

	f := c.raft.Apply(cmdBytes, 5*time.Second)
	if err := f.Error(); err != nil {
		return nil, fmt.Errorf("raft apply: %w", err)
	}

	result := f.Response()
	if err, ok := result.(error); ok {
		return nil, err
	}
	return result, nil
}

// RegisterBroker registers a new broker and returns its ID.
func (c *Controller) RegisterBroker(host string, port int32) (cluster.BrokerID, *cluster.ClusterMetadata, error) {
	result, err := c.proposeCommand(CmdRegisterBroker, RegisterBrokerData{
		Host: host,
		Port: port,
	})
	if err != nil {
		return 0, nil, err
	}

	id, ok := result.(cluster.BrokerID)
	if !ok {
		return 0, nil, fmt.Errorf("unexpected result type: %T", result)
	}

	meta := c.fsm.GetMetadata()
	return id, meta, nil
}

// CreateTopic creates a new topic with the given parameters.
func (c *Controller) CreateTopic(topic string, numPartitions int32, replicationFactor int16) error {
	_, err := c.proposeCommand(CmdCreateTopic, CreateTopicData{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	})
	return err
}

// UpdateISR updates the ISR for a partition.
func (c *Controller) UpdateISR(topic string, partition int32, newISR []cluster.BrokerID) error {
	_, err := c.proposeCommand(CmdUpdateISR, UpdateISRData{
		Topic:     topic,
		Partition: partition,
		NewISR:    newISR,
	})
	return err
}

// RecordHeartbeat records a heartbeat from a broker.
func (c *Controller) RecordHeartbeat(brokerID cluster.BrokerID) error {
	_, err := c.proposeCommand(CmdHeartbeat, HeartbeatData{
		BrokerID: brokerID,
		Time:     time.Now(),
	})
	return err
}

// TimeoutBroker removes a timed-out broker.
func (c *Controller) TimeoutBroker(brokerID cluster.BrokerID) error {
	_, err := c.proposeCommand(CmdBrokerTimeout, BrokerTimeoutData{
		BrokerID: brokerID,
	})
	return err
}

// GetMetadata returns the current cluster metadata.
func (c *Controller) GetMetadata() *cluster.ClusterMetadata {
	return c.fsm.GetMetadata()
}

// IsLeader returns true if this controller is the Raft leader.
func (c *Controller) IsLeader() bool {
	return c.raft.State() == raft.Leader
}

// Shutdown gracefully shuts down the controller.
func (c *Controller) Shutdown() error {
	c.health.Stop()
	c.rpcServer.Stop()
	f := c.raft.Shutdown()
	return f.Error()
}
