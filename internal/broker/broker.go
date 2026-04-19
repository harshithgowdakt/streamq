package broker

import (
	"sync"
	"time"

	"github.com/harshithgowda/streamq/internal/log"
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

	retentionStop chan struct{}
	retentionWg   sync.WaitGroup
}

// NewBroker creates a new Broker.
func NewBroker(cfg Config) *Broker {
	logConfig := log.CommitLogConfig{
		MaxSegmentBytes: cfg.MaxSegmentBytes,
		RetentionBytes:  cfg.RetentionBytes,
		RetentionAge:    cfg.RetentionAge,
	}

	b := &Broker{
		Config:        cfg,
		TopicManager:  NewTopicManager(cfg.DataDir, logConfig),
		retentionStop: make(chan struct{}),
	}

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
	close(b.retentionStop)
	b.retentionWg.Wait()
	return b.TopicManager.Close()
}
