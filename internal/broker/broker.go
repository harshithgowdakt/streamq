package broker

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/harshithgowda/streamq/internal/cluster"
	ilog "github.com/harshithgowda/streamq/internal/log"
)

// Config holds broker configuration.
type Config struct {
	DataDir           string
	DefaultPartitions int32
	MaxSegmentBytes   int64
	RetentionBytes    int64
	RetentionAge      time.Duration
	RetentionCheckMs  int64 // how often to check retention (ms), 0 = 60000
	AutoCreateTopics  bool
	Addr              string

	// Cluster mode fields (empty = single-node mode)
	ControllerAddr string
	NodeID         int32 // ignored in cluster mode (assigned by controller)
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		DataDir:           "/tmp/streamq-data",
		DefaultPartitions: 1,
		MaxSegmentBytes:   1024 * 1024 * 1024, // 1 GB
		RetentionCheckMs:  60000,
		AutoCreateTopics:  true,
		Addr:              ":9092",
	}
}

// Broker is the core broker that wires together topic management and request handling.
type Broker struct {
	Config       Config
	TopicManager *TopicManager

	// Consumer group coordination
	groupCoordinator *GroupCoordinator

	// Cluster mode
	controllerClient *ControllerClient
	nodeID           cluster.BrokerID
	clusterMode      bool
	partitionStates  sync.Map // "topic-partition" -> *PartitionState
	fetchers         sync.Map // cluster.BrokerID -> *ReplicaFetcher
	isrManager       *ISRManager

	retentionStop chan struct{}
	retentionWg   sync.WaitGroup
}

// NewBroker creates a new Broker.
func NewBroker(cfg Config) *Broker {
	logConfig := ilog.CommitLogConfig{
		MaxSegmentBytes: cfg.MaxSegmentBytes,
		RetentionBytes:  cfg.RetentionBytes,
		RetentionAge:    cfg.RetentionAge,
	}

	b := &Broker{
		Config:        cfg,
		TopicManager:  NewTopicManager(cfg.DataDir, logConfig),
		retentionStop: make(chan struct{}),
	}

	// Initialize group coordinator
	b.groupCoordinator = NewGroupCoordinator(b)

	// Start retention enforcement goroutine
	checkMs := cfg.RetentionCheckMs
	if checkMs <= 0 {
		checkMs = 60000
	}
	if cfg.RetentionBytes > 0 || cfg.RetentionAge > 0 {
		b.retentionWg.Add(1)
		go b.retentionLoop(time.Duration(checkMs) * time.Millisecond)
	}

	return b
}

// ConnectToController registers this broker with the controller (cluster mode).
func (b *Broker) ConnectToController() error {
	if b.Config.ControllerAddr == "" {
		return nil // single-node mode
	}

	host, portStr := parseAddr(b.Config.Addr)
	port, _ := strconv.Atoi(portStr)

	cc := NewControllerClient(b.Config.ControllerAddr, b.onMetadataUpdate)
	id, err := cc.Register(host, int32(port))
	if err != nil {
		return fmt.Errorf("register with controller: %w", err)
	}

	b.controllerClient = cc
	b.nodeID = id
	b.clusterMode = true

	// Start ISR manager
	b.isrManager = NewISRManager(b, 10000)
	b.isrManager.Start()

	log.Printf("registered with controller as broker %d", id)
	return nil
}

// IsClusterMode returns true if this broker is running in cluster mode.
func (b *Broker) IsClusterMode() bool {
	return b.clusterMode
}

// GetNodeID returns this broker's node ID.
func (b *Broker) GetNodeID() int32 {
	if b.clusterMode {
		return int32(b.nodeID)
	}
	return 0
}

// GetPartitionState returns the partition state for the given topic-partition.
func (b *Broker) GetPartitionState(topic string, partition int32) *PartitionState {
	key := fmt.Sprintf("%s-%d", topic, partition)
	if v, ok := b.partitionStates.Load(key); ok {
		return v.(*PartitionState)
	}
	return nil
}

// SetPartitionState stores the partition state.
func (b *Broker) SetPartitionState(topic string, partition int32, state *PartitionState) {
	key := fmt.Sprintf("%s-%d", topic, partition)
	b.partitionStates.Store(key, state)
}

// onMetadataUpdate is called when the controller pushes new metadata.
func (b *Broker) onMetadataUpdate(meta *cluster.ClusterMetadata) {
	if meta == nil {
		return
	}

	for topic, assignments := range meta.Assignments {
		for _, pa := range assignments {
			// Check if this broker is in the replica set
			isReplica := false
			for _, r := range pa.Replicas {
				if r == b.nodeID {
					isReplica = true
					break
				}
			}
			if !isReplica {
				continue
			}

			// Ensure partition exists locally
			_, err := b.TopicManager.GetPartition(topic, pa.Partition)
			if err != nil {
				b.TopicManager.CreateLocalPartition(topic, pa.Partition)
			}

			// Update partition state
			state := b.GetPartitionState(topic, pa.Partition)
			if state == nil {
				state = NewPartitionState()
				b.SetPartitionState(topic, pa.Partition, state)
			}

			state.mu.Lock()
			oldRole := state.Role
			oldEpoch := state.Epoch

			if pa.Leader == b.nodeID {
				state.Role = RoleLeader
			} else {
				state.Role = RoleFollower
			}
			state.Epoch = pa.Epoch
			state.ISR = append([]cluster.BrokerID(nil), pa.ISR...)

			newRole := state.Role
			newEpoch := state.Epoch
			state.mu.Unlock()

			// Manage fetchers based on role changes
			if newRole == RoleFollower && (oldRole != RoleFollower || oldEpoch != newEpoch) {
				// Find leader address
				leaderInfo, ok := meta.Brokers[pa.Leader]
				if ok {
					leaderAddr := net.JoinHostPort(leaderInfo.Host, strconv.Itoa(int(leaderInfo.Port)))
					b.startFetcher(pa.Leader, leaderAddr, topic, pa.Partition)
				}
			} else if newRole == RoleLeader && oldRole == RoleFollower {
				// Stop fetching when promoted to leader
				b.stopFetchersForPartition(topic, pa.Partition)
			}
		}
	}
}

func (b *Broker) startFetcher(leaderID cluster.BrokerID, leaderAddr string, topic string, partition int32) {
	part, err := b.TopicManager.GetPartition(topic, partition)
	if err != nil {
		return
	}

	// Create or get fetcher for this leader
	fetcher := NewReplicaFetcher(b, leaderAddr, leaderID)
	fetcher.AddPartition(part, topic)

	key := fmt.Sprintf("%s-%d", topic, partition)
	if old, loaded := b.fetchers.LoadOrStore(key, fetcher); loaded {
		// Stop old fetcher first
		old.(*ReplicaFetcher).Stop()
		b.fetchers.Store(key, fetcher)
	}

	fetcher.Start()
}

func (b *Broker) stopFetchersForPartition(topic string, partition int32) {
	key := fmt.Sprintf("%s-%d", topic, partition)
	if v, ok := b.fetchers.LoadAndDelete(key); ok {
		v.(*ReplicaFetcher).Stop()
	}
}

func (b *Broker) retentionLoop(interval time.Duration) {
	defer b.retentionWg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.TopicManager.EnforceRetention()
		case <-b.retentionStop:
			return
		}
	}
}

// Close shuts down the broker.
func (b *Broker) Close() error {
	if b.groupCoordinator != nil {
		b.groupCoordinator.Close()
	}

	close(b.retentionStop)
	b.retentionWg.Wait()

	if b.isrManager != nil {
		b.isrManager.Stop()
	}

	// Stop all fetchers
	b.fetchers.Range(func(key, value interface{}) bool {
		value.(*ReplicaFetcher).Stop()
		return true
	})

	if b.controllerClient != nil {
		b.controllerClient.Close()
	}

	return b.TopicManager.Close()
}

// GetClusterMetadata returns the current cluster metadata (or nil in single-node).
func (b *Broker) GetClusterMetadata() *cluster.ClusterMetadata {
	if b.controllerClient != nil {
		return b.controllerClient.GetMetadata()
	}
	return nil
}

func parseAddr(addr string) (host, port string) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "localhost", "9092"
	}
	if host == "" {
		host = "localhost"
	}
	return host, port
}

