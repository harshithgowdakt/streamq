package broker

import (
	"github.com/harshithgowda/streamq/internal/log"
)

// Config holds broker configuration.
type Config struct {
	DataDir           string
	DefaultPartitions int32
	MaxSegmentBytes   int64
	Addr              string
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		DataDir:           "/tmp/streamq-data",
		DefaultPartitions: 1,
		MaxSegmentBytes:   1024 * 1024 * 1024, // 1 GB
		Addr:              ":9092",
	}
}

// Broker is the core broker that wires together topic management and request handling.
type Broker struct {
	Config       Config
	TopicManager *TopicManager
}

// NewBroker creates a new Broker.
func NewBroker(cfg Config) *Broker {
	logConfig := log.CommitLogConfig{
		MaxSegmentBytes: cfg.MaxSegmentBytes,
	}

	return &Broker{
		Config:       cfg,
		TopicManager: NewTopicManager(cfg.DataDir, logConfig),
	}
}

// Close shuts down the broker.
func (b *Broker) Close() error {
	return b.TopicManager.Close()
}
