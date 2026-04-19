package broker

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/harshithgowda/streamq/internal/log"
)

// Partition owns a CommitLog for a single topic-partition.
type Partition struct {
	ID  int32
	Log *log.CommitLog
}

// Topic holds an ordered list of partitions.
type Topic struct {
	Name       string
	Partitions []*Partition
}

// TopicManager manages topics and their partitions.
type TopicManager struct {
	mu     sync.RWMutex
	topics map[string]*Topic
	dataDir string
	logConfig log.CommitLogConfig
}

// NewTopicManager creates a new TopicManager.
func NewTopicManager(dataDir string, logConfig log.CommitLogConfig) *TopicManager {
	return &TopicManager{
		topics:    make(map[string]*Topic),
		dataDir:   dataDir,
		logConfig: logConfig,
	}
}

// CreateTopic creates a topic with the given number of partitions.
func (tm *TopicManager) CreateTopic(name string, numPartitions int32) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.topics[name]; exists {
		return fmt.Errorf("topic %q already exists", name)
	}

	topic := &Topic{
		Name:       name,
		Partitions: make([]*Partition, numPartitions),
	}

	for i := int32(0); i < numPartitions; i++ {
		dir := filepath.Join(tm.dataDir, fmt.Sprintf("%s-%d", name, i))
		cl, err := log.NewCommitLog(dir, tm.logConfig)
		if err != nil {
			// Clean up already-created partitions
			for j := int32(0); j < i; j++ {
				topic.Partitions[j].Log.Close()
			}
			return fmt.Errorf("create partition %d: %w", i, err)
		}
		topic.Partitions[i] = &Partition{ID: i, Log: cl}
	}

	tm.topics[name] = topic
	return nil
}

// GetTopic returns a topic by name, or nil if not found.
func (tm *TopicManager) GetTopic(name string) *Topic {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.topics[name]
}

// GetPartition returns a specific partition of a topic.
func (tm *TopicManager) GetPartition(topicName string, partitionID int32) (*Partition, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	topic, ok := tm.topics[topicName]
	if !ok {
		return nil, fmt.Errorf("unknown topic %q", topicName)
	}
	if partitionID < 0 || int(partitionID) >= len(topic.Partitions) {
		return nil, fmt.Errorf("partition %d out of range for topic %q (has %d)", partitionID, topicName, len(topic.Partitions))
	}
	return topic.Partitions[partitionID], nil
}

// ListTopics returns a list of all topic names.
func (tm *TopicManager) ListTopics() []string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	names := make([]string, 0, len(tm.topics))
	for name := range tm.topics {
		names = append(names, name)
	}
	return names
}

// Close closes all partition logs.
func (tm *TopicManager) Close() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for _, topic := range tm.topics {
		for _, p := range topic.Partitions {
			if err := p.Log.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}
