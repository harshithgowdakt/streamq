package broker

import (
	"sort"

	"github.com/harshithgowda/streamq/internal/protocol"
)

// PartitionAssignor assigns partitions to consumer group members.
type PartitionAssignor interface {
	Name() string
	Assign(members map[string]*protocol.ConsumerProtocolSubscription, partitions map[string][]int32) map[string]*protocol.ConsumerProtocolAssignment
}

// RangeAssignor assigns partitions per-topic using range-based distribution.
type RangeAssignor struct{}

func (r *RangeAssignor) Name() string { return "range" }

func (r *RangeAssignor) Assign(members map[string]*protocol.ConsumerProtocolSubscription, partitions map[string][]int32) map[string]*protocol.ConsumerProtocolAssignment {
	result := make(map[string]*protocol.ConsumerProtocolAssignment, len(members))
	for memberID := range members {
		result[memberID] = &protocol.ConsumerProtocolAssignment{
			PartitionsByTopic: make(map[string][]int32),
		}
	}

	// Collect all topics
	allTopics := make(map[string]bool)
	for _, sub := range members {
		for _, t := range sub.Topics {
			allTopics[t] = true
		}
	}

	for topic := range allTopics {
		parts, ok := partitions[topic]
		if !ok || len(parts) == 0 {
			continue
		}

		// Find members subscribed to this topic
		var subscribedMembers []string
		for memberID, sub := range members {
			for _, t := range sub.Topics {
				if t == topic {
					subscribedMembers = append(subscribedMembers, memberID)
					break
				}
			}
		}
		sort.Strings(subscribedMembers)

		sortedParts := make([]int32, len(parts))
		copy(sortedParts, parts)
		sort.Slice(sortedParts, func(i, j int) bool { return sortedParts[i] < sortedParts[j] })

		numParts := len(sortedParts)
		numMembers := len(subscribedMembers)
		if numMembers == 0 {
			continue
		}

		rangeSize := numParts / numMembers
		extra := numParts % numMembers

		idx := 0
		for i, memberID := range subscribedMembers {
			count := rangeSize
			if i < extra {
				count++
			}
			if count > 0 {
				result[memberID].PartitionsByTopic[topic] = append(
					result[memberID].PartitionsByTopic[topic],
					sortedParts[idx:idx+count]...,
				)
				idx += count
			}
		}
	}

	return result
}

// RoundRobinAssignor assigns all topic-partitions round-robin across sorted members.
type RoundRobinAssignor struct{}

func (r *RoundRobinAssignor) Name() string { return "roundrobin" }

func (r *RoundRobinAssignor) Assign(members map[string]*protocol.ConsumerProtocolSubscription, partitions map[string][]int32) map[string]*protocol.ConsumerProtocolAssignment {
	result := make(map[string]*protocol.ConsumerProtocolAssignment, len(members))
	for memberID := range members {
		result[memberID] = &protocol.ConsumerProtocolAssignment{
			PartitionsByTopic: make(map[string][]int32),
		}
	}

	// Sort member IDs
	sortedMembers := make([]string, 0, len(members))
	for id := range members {
		sortedMembers = append(sortedMembers, id)
	}
	sort.Strings(sortedMembers)

	// Collect all topic-partitions sorted
	type topicPartition struct {
		topic     string
		partition int32
	}
	var allTP []topicPartition

	topics := make([]string, 0, len(partitions))
	for t := range partitions {
		topics = append(topics, t)
	}
	sort.Strings(topics)

	for _, t := range topics {
		parts := make([]int32, len(partitions[t]))
		copy(parts, partitions[t])
		sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
		for _, p := range parts {
			allTP = append(allTP, topicPartition{topic: t, partition: p})
		}
	}

	// Round-robin assignment (only to members subscribed to that topic)
	memberIdx := 0
	for _, tp := range allTP {
		// Find next member subscribed to this topic
		assigned := false
		for attempts := 0; attempts < len(sortedMembers); attempts++ {
			candidateID := sortedMembers[memberIdx%len(sortedMembers)]
			memberIdx++
			sub := members[candidateID]
			for _, t := range sub.Topics {
				if t == tp.topic {
					result[candidateID].PartitionsByTopic[tp.topic] = append(
						result[candidateID].PartitionsByTopic[tp.topic],
						tp.partition,
					)
					assigned = true
					break
				}
			}
			if assigned {
				break
			}
		}
	}

	return result
}
