package protocol

import (
	"bytes"
	"sort"
)

// ConsumerProtocolSubscription is the metadata embedded in JoinGroup protocol metadata bytes.
// Wire format: version(int16), topic_count(int32), topics(string...), user_data(bytes)
type ConsumerProtocolSubscription struct {
	Version  int16
	Topics   []string
	UserData []byte
}

// ConsumerProtocolAssignment is the assignment embedded in SyncGroup assignment bytes.
// Wire format: version(int16), topic_count(int32), [topic(string), partition_count(int32), partitions(int32...)...], user_data(bytes)
type ConsumerProtocolAssignment struct {
	Version           int16
	PartitionsByTopic map[string][]int32
	UserData          []byte
}

// EncodeConsumerSubscription encodes a subscription to Kafka wire format bytes.
func EncodeConsumerSubscription(sub *ConsumerProtocolSubscription) []byte {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	enc.WriteInt16(sub.Version)
	enc.WriteInt32(int32(len(sub.Topics)))
	for _, t := range sub.Topics {
		enc.WriteString(t)
	}
	enc.WriteBytes(sub.UserData)

	return buf.Bytes()
}

// DecodeConsumerSubscription decodes a subscription from Kafka wire format bytes.
func DecodeConsumerSubscription(data []byte) (*ConsumerProtocolSubscription, error) {
	dec := NewDecoder(bytes.NewReader(data))

	sub := &ConsumerProtocolSubscription{}
	sub.Version = dec.ReadInt16()
	topicCount := dec.ReadInt32()
	sub.Topics = make([]string, topicCount)
	for i := range sub.Topics {
		sub.Topics[i] = dec.ReadString()
	}
	sub.UserData = dec.ReadBytes()

	if dec.Err() != nil {
		return nil, dec.Err()
	}
	return sub, nil
}

// EncodeConsumerAssignment encodes an assignment to Kafka wire format bytes.
func EncodeConsumerAssignment(assign *ConsumerProtocolAssignment) []byte {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	enc.WriteInt16(assign.Version)

	// Sort topics for deterministic output
	topics := make([]string, 0, len(assign.PartitionsByTopic))
	for t := range assign.PartitionsByTopic {
		topics = append(topics, t)
	}
	sort.Strings(topics)

	enc.WriteInt32(int32(len(topics)))
	for _, t := range topics {
		enc.WriteString(t)
		parts := assign.PartitionsByTopic[t]
		enc.WriteInt32(int32(len(parts)))
		for _, p := range parts {
			enc.WriteInt32(p)
		}
	}
	enc.WriteBytes(assign.UserData)

	return buf.Bytes()
}

// DecodeConsumerAssignment decodes an assignment from Kafka wire format bytes.
func DecodeConsumerAssignment(data []byte) (*ConsumerProtocolAssignment, error) {
	if len(data) == 0 {
		return &ConsumerProtocolAssignment{
			PartitionsByTopic: make(map[string][]int32),
		}, nil
	}

	dec := NewDecoder(bytes.NewReader(data))

	assign := &ConsumerProtocolAssignment{
		PartitionsByTopic: make(map[string][]int32),
	}
	assign.Version = dec.ReadInt16()
	topicCount := dec.ReadInt32()
	for i := int32(0); i < topicCount; i++ {
		topic := dec.ReadString()
		partCount := dec.ReadInt32()
		parts := make([]int32, partCount)
		for j := range parts {
			parts[j] = dec.ReadInt32()
		}
		assign.PartitionsByTopic[topic] = parts
	}
	assign.UserData = dec.ReadBytes()

	if dec.Err() != nil {
		return nil, dec.Err()
	}
	return assign, nil
}
