package broker

import (
	"encoding/binary"
	"fmt"
	"log"

	ilog "github.com/harshithgowda/streamq/internal/log"
)

// The internal topic used to durably store consumer-group offset commits.
// Modeled after Kafka's __consumer_offsets. Each partition is a Kafka-style
// log of offset-commit records; the latest record for a given (group, topic,
// partition) key wins (log-compaction semantics, simulated at load time).
const (
	offsetsTopicName       = "__consumer_offsets"
	offsetsTopicPartitions = 8
)

// OffsetAndMetadata is the in-memory form of a committed offset.
type OffsetAndMetadata struct {
	Offset   int64
	Metadata string
}

// offsetCommitKey uniquely identifies a single committed offset.
// Wire format (all big-endian): int16 groupLen, group bytes, int16 topicLen,
// topic bytes, int32 partition.
type offsetCommitKey struct {
	GroupID   string
	Topic     string
	Partition int32
}

// offsetCommitValue holds the committed offset payload. A tombstone has
// Offset == -1 (used for future group deletion; unused today).
// Wire format: int64 offset, int16 metaLen, metadata bytes, int64 timestamp.
type offsetCommitValue struct {
	Offset    int64
	Metadata  string
	Timestamp int64
}

func encodeOffsetKey(k offsetCommitKey) []byte {
	buf := make([]byte, 2+len(k.GroupID)+2+len(k.Topic)+4)
	off := 0
	binary.BigEndian.PutUint16(buf[off:], uint16(len(k.GroupID)))
	off += 2
	copy(buf[off:], k.GroupID)
	off += len(k.GroupID)
	binary.BigEndian.PutUint16(buf[off:], uint16(len(k.Topic)))
	off += 2
	copy(buf[off:], k.Topic)
	off += len(k.Topic)
	binary.BigEndian.PutUint32(buf[off:], uint32(k.Partition))
	return buf
}

func decodeOffsetKey(b []byte) (offsetCommitKey, error) {
	var k offsetCommitKey
	if len(b) < 2 {
		return k, fmt.Errorf("offset key too short")
	}
	off := 0
	glen := int(binary.BigEndian.Uint16(b[off:]))
	off += 2
	if off+glen+2 > len(b) {
		return k, fmt.Errorf("offset key truncated at group")
	}
	k.GroupID = string(b[off : off+glen])
	off += glen
	tlen := int(binary.BigEndian.Uint16(b[off:]))
	off += 2
	if off+tlen+4 > len(b) {
		return k, fmt.Errorf("offset key truncated at topic")
	}
	k.Topic = string(b[off : off+tlen])
	off += tlen
	k.Partition = int32(binary.BigEndian.Uint32(b[off:]))
	return k, nil
}

func encodeOffsetValue(v offsetCommitValue) []byte {
	buf := make([]byte, 8+2+len(v.Metadata)+8)
	off := 0
	binary.BigEndian.PutUint64(buf[off:], uint64(v.Offset))
	off += 8
	binary.BigEndian.PutUint16(buf[off:], uint16(len(v.Metadata)))
	off += 2
	copy(buf[off:], v.Metadata)
	off += len(v.Metadata)
	binary.BigEndian.PutUint64(buf[off:], uint64(v.Timestamp))
	return buf
}

func decodeOffsetValue(b []byte) (offsetCommitValue, error) {
	var v offsetCommitValue
	if len(b) < 10 {
		return v, fmt.Errorf("offset value too short")
	}
	off := 0
	v.Offset = int64(binary.BigEndian.Uint64(b[off:]))
	off += 8
	mlen := int(binary.BigEndian.Uint16(b[off:]))
	off += 2
	if off+mlen+8 > len(b) {
		return v, fmt.Errorf("offset value truncated")
	}
	v.Metadata = string(b[off : off+mlen])
	off += mlen
	v.Timestamp = int64(binary.BigEndian.Uint64(b[off:]))
	return v, nil
}

// partitionFor picks the __consumer_offsets partition for a group via Java's
// String.hashCode contract, so two brokers route a given group the same way.
func offsetsPartitionFor(groupID string) int32 {
	var h int32
	for i := 0; i < len(groupID); i++ {
		h = h*31 + int32(groupID[i])
	}
	if h < 0 {
		h = -h
		if h < 0 { // INT_MIN
			h = 0
		}
	}
	return h % offsetsTopicPartitions
}

// ensureOffsetsTopic creates __consumer_offsets if it doesn't exist yet.
// In cluster mode this goes through the controller so replication is set up;
// in single-node mode we create it directly.
func (gc *GroupCoordinator) ensureOffsetsTopic() error {
	b := gc.broker
	if b.TopicManager.GetTopic(offsetsTopicName) != nil {
		return nil
	}
	if b.clusterMode && b.controllerClient != nil {
		if err := b.controllerClient.CreateTopic(offsetsTopicName, offsetsTopicPartitions, 3); err != nil {
			return fmt.Errorf("create %s via controller: %w", offsetsTopicName, err)
		}
		return nil
	}
	return b.TopicManager.CreateTopic(offsetsTopicName, offsetsTopicPartitions)
}

// appendOffsetRecord writes a single (key, value) record to the partition of
// __consumer_offsets owned by groupID. Returns the base offset of the append.
//
// This must return only after the record is durable on disk (commit log
// semantics). In cluster mode durability extends across replicas because
// __consumer_offsets is replicated like any other topic.
func (gc *GroupCoordinator) appendOffsetRecord(groupID string, key, value []byte) error {
	partID := offsetsPartitionFor(groupID)
	part, err := gc.broker.TopicManager.GetPartition(offsetsTopicName, partID)
	if err != nil {
		return fmt.Errorf("%s-%d missing: %w", offsetsTopicName, partID, err)
	}
	batch := &ilog.RecordBatch{
		Records: []ilog.Record{{Key: key, Value: value}},
	}
	if _, err := part.Log.Append(batch); err != nil {
		return fmt.Errorf("append to %s-%d: %w", offsetsTopicName, partID, err)
	}
	return nil
}

// loadOffsetsFromLog replays every partition of __consumer_offsets and
// rebuilds the in-memory offset table for each group. The last record for a
// given (group, topic, partition) wins — this is the log-compaction semantic
// Kafka relies on.
func (gc *GroupCoordinator) loadOffsetsFromLog() {
	topic := gc.broker.TopicManager.GetTopic(offsetsTopicName)
	if topic == nil {
		return
	}

	loaded := 0
	for _, part := range topic.Partitions {
		offset := int64(0)
		for {
			batches, err := part.Log.Read(offset, 4*1024*1024)
			if err != nil || len(batches) == 0 {
				break
			}
			for _, batch := range batches {
				for i, rec := range batch.Records {
					_ = i
					k, err := decodeOffsetKey(rec.Key)
					if err != nil {
						continue
					}
					v, err := decodeOffsetValue(rec.Value)
					if err != nil {
						continue
					}
					group := gc.getOrCreateGroup(k.GroupID)
					group.mu.Lock()
					if v.Offset < 0 {
						// tombstone
						if m, ok := group.Offsets[k.Topic]; ok {
							delete(m, k.Partition)
							if len(m) == 0 {
								delete(group.Offsets, k.Topic)
							}
						}
					} else {
						if group.Offsets[k.Topic] == nil {
							group.Offsets[k.Topic] = make(map[int32]OffsetAndMetadata)
						}
						group.Offsets[k.Topic][k.Partition] = OffsetAndMetadata{
							Offset:   v.Offset,
							Metadata: v.Metadata,
						}
					}
					group.mu.Unlock()
					loaded++
				}
			}
			last := batches[len(batches)-1]
			offset = last.BaseOffset + int64(len(last.Records))
		}
	}
	if loaded > 0 {
		log.Printf("group coordinator: restored %d offset commits from %s", loaded, offsetsTopicName)
	}
}
