package broker

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	mu        sync.RWMutex
	topics    map[string]*Topic
	dataDir   string
	logConfig log.CommitLogConfig
}

// NewTopicManager creates a new TopicManager.
func NewTopicManager(dataDir string, logConfig log.CommitLogConfig) *TopicManager {
	tm := &TopicManager{
		topics:    make(map[string]*Topic),
		dataDir:   dataDir,
		logConfig: logConfig,
	}
	tm.discoverExistingTopics()
	return tm
}

// discoverExistingTopics scans dataDir for existing topic-partition
// directories (named "topic-N") and re-opens their commit logs so data
// survives broker restarts. Unknown directories are ignored.
func (tm *TopicManager) discoverExistingTopics() {
	entries, err := os.ReadDir(tm.dataDir)
	if err != nil {
		return
	}
	// Group directory names by topic: "topic-N" -> topic="topic", N=partition.
	byTopic := make(map[string][]int32)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		idx := strings.LastIndex(name, "-")
		if idx <= 0 || idx == len(name)-1 {
			continue
		}
		pStr := name[idx+1:]
		pid, err := strconv.Atoi(pStr)
		if err != nil || pid < 0 {
			continue
		}
		topic := name[:idx]
		byTopic[topic] = append(byTopic[topic], int32(pid))
	}

	for topic, parts := range byTopic {
		maxID := int32(-1)
		for _, p := range parts {
			if p > maxID {
				maxID = p
			}
		}
		t := &Topic{
			Name:       topic,
			Partitions: make([]*Partition, maxID+1),
		}
		opened := 0
		for _, pid := range parts {
			dir := filepath.Join(tm.dataDir, fmt.Sprintf("%s-%d", topic, pid))
			cl, err := log.NewCommitLog(dir, tm.logConfig)
			if err != nil {
				continue
			}
			t.Partitions[pid] = &Partition{ID: pid, Log: cl}
			opened++
		}
		if opened > 0 {
			tm.topics[topic] = t
		}
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

// EnforceRetention runs retention enforcement on all partition logs.
func (tm *TopicManager) EnforceRetention() {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	for _, topic := range tm.topics {
		for _, p := range topic.Partitions {
			p.Log.EnforceRetention()
		}
	}
}

// CreateLocalPartition creates a single partition for a topic if it doesn't exist.
// Used by cluster mode when receiving partition assignments.
func (tm *TopicManager) CreateLocalPartition(topicName string, partitionID int32) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	topic, ok := tm.topics[topicName]
	if !ok {
		topic = &Topic{
			Name:       topicName,
			Partitions: make([]*Partition, 0),
		}
		tm.topics[topicName] = topic
	}

	// Check if partition already exists
	for _, p := range topic.Partitions {
		if p.ID == partitionID {
			return nil // already exists
		}
	}

	dir := filepath.Join(tm.dataDir, fmt.Sprintf("%s-%d", topicName, partitionID))
	cl, err := log.NewCommitLog(dir, tm.logConfig)
	if err != nil {
		return fmt.Errorf("create partition %d: %w", partitionID, err)
	}

	p := &Partition{ID: partitionID, Log: cl}
	topic.Partitions = append(topic.Partitions, p)

	// Sort partitions by ID to maintain order
	for i := len(topic.Partitions) - 1; i > 0; i-- {
		if topic.Partitions[i].ID < topic.Partitions[i-1].ID {
			topic.Partitions[i], topic.Partitions[i-1] = topic.Partitions[i-1], topic.Partitions[i]
		}
	}

	return nil
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
