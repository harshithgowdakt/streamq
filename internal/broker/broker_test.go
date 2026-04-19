package broker

import (
	"testing"

	"github.com/harshithgowda/streamq/internal/log"
	"github.com/harshithgowda/streamq/internal/protocol"
)

func testBroker(t *testing.T) *Broker {
	t.Helper()
	cfg := Config{
		DataDir:           t.TempDir(),
		DefaultPartitions: 3,
		MaxSegmentBytes:   1024 * 1024,
		Addr:              ":0",
	}
	b := NewBroker(cfg)
	t.Cleanup(func() { b.Close() })
	return b
}

func TestCreateTopics(t *testing.T) {
	b := testBroker(t)

	resp := b.HandleCreateTopics(&protocol.CreateTopicsRequest{
		Header: protocol.RequestHeader{CorrelationID: 1},
		Topics: []protocol.CreateTopicRequest{
			{Topic: "test-topic", NumPartitions: 4},
		},
	})

	if resp.Topics[0].ErrorCode != protocol.ErrNone {
		t.Fatalf("create topic error: %d", resp.Topics[0].ErrorCode)
	}

	// Duplicate should fail
	resp2 := b.HandleCreateTopics(&protocol.CreateTopicsRequest{
		Header: protocol.RequestHeader{CorrelationID: 2},
		Topics: []protocol.CreateTopicRequest{
			{Topic: "test-topic", NumPartitions: 4},
		},
	})

	if resp2.Topics[0].ErrorCode != protocol.ErrTopicAlreadyExists {
		t.Fatalf("expected TopicAlreadyExists, got %d", resp2.Topics[0].ErrorCode)
	}
}

func TestProduceAndFetch(t *testing.T) {
	b := testBroker(t)

	// Create topic
	b.HandleCreateTopics(&protocol.CreateTopicsRequest{
		Topics: []protocol.CreateTopicRequest{
			{Topic: "test", NumPartitions: 1},
		},
	})

	// Produce using Kafka RecordBatch format
	batch := &log.RecordBatch{
		Records: []log.Record{
			{Key: []byte("k1"), Value: []byte("hello")},
			{Key: []byte("k2"), Value: []byte("world")},
		},
	}
	kafkaBatchBytes := protocol.InternalToKafkaRecordBatch(batch)

	produceResp := b.HandleProduce(&protocol.ProduceRequest{
		Header: protocol.RequestHeader{CorrelationID: 10, APIVersion: 3},
		Acks:   -1,
		Topics: []protocol.ProduceTopicData{
			{
				Topic: "test",
				Partitions: []protocol.ProducePartitionData{
					{Partition: 0, Records: kafkaBatchBytes},
				},
			},
		},
	})

	if produceResp.Topics[0].Partitions[0].ErrorCode != protocol.ErrNone {
		t.Fatalf("produce error: %d", produceResp.Topics[0].Partitions[0].ErrorCode)
	}
	if produceResp.Topics[0].Partitions[0].BaseOffset != 0 {
		t.Fatalf("expected base offset 0, got %d", produceResp.Topics[0].Partitions[0].BaseOffset)
	}

	// Fetch
	fetchResp := b.HandleFetch(&protocol.FetchRequest{
		Header:    protocol.RequestHeader{CorrelationID: 11, APIVersion: 4},
		ReplicaID: -1,
		Topics: []protocol.FetchTopicData{
			{
				Topic: "test",
				Partitions: []protocol.FetchPartitionData{
					{Partition: 0, FetchOffset: 0, MaxBytes: 1024 * 1024},
				},
			},
		},
	})

	if fetchResp.Topics[0].Partitions[0].ErrorCode != protocol.ErrNone {
		t.Fatalf("fetch error: %d", fetchResp.Topics[0].Partitions[0].ErrorCode)
	}

	rawBatches := fetchResp.Topics[0].Partitions[0].RecordBatches
	if len(rawBatches) == 0 {
		t.Fatal("fetch returned no data")
	}

	// Decode using Kafka format
	batches, err := protocol.KafkaRecordBatchToInternal(rawBatches)
	if err != nil {
		t.Fatalf("decode fetched batch: %v", err)
	}

	totalRecords := 0
	for _, b := range batches {
		totalRecords += len(b.Records)
	}
	if totalRecords != 2 {
		t.Fatalf("expected 2 records, got %d", totalRecords)
	}
	if string(batches[0].Records[0].Value) != "hello" {
		t.Errorf("record 0 value: %q", batches[0].Records[0].Value)
	}
}

func TestProduceMultiBatch(t *testing.T) {
	b := testBroker(t)

	b.HandleCreateTopics(&protocol.CreateTopicsRequest{
		Topics: []protocol.CreateTopicRequest{
			{Topic: "multi", NumPartitions: 1},
		},
	})

	// Encode two Kafka batches concatenated
	batch1 := &log.RecordBatch{Records: []log.Record{{Value: []byte("a")}}}
	batch2 := &log.RecordBatch{Records: []log.Record{{Value: []byte("b")}}}
	combined := append(protocol.InternalToKafkaRecordBatch(batch1), protocol.InternalToKafkaRecordBatch(batch2)...)

	resp := b.HandleProduce(&protocol.ProduceRequest{
		Header: protocol.RequestHeader{APIVersion: 3},
		Acks:   -1,
		Topics: []protocol.ProduceTopicData{
			{
				Topic: "multi",
				Partitions: []protocol.ProducePartitionData{
					{Partition: 0, Records: combined},
				},
			},
		},
	})

	if resp.Topics[0].Partitions[0].ErrorCode != protocol.ErrNone {
		t.Fatalf("produce error: %d", resp.Topics[0].Partitions[0].ErrorCode)
	}
	if resp.Topics[0].Partitions[0].BaseOffset != 0 {
		t.Fatalf("expected base offset 0, got %d", resp.Topics[0].Partitions[0].BaseOffset)
	}

	// Fetch should return 2 records
	fetchResp := b.HandleFetch(&protocol.FetchRequest{
		Header:    protocol.RequestHeader{APIVersion: 4},
		ReplicaID: -1,
		Topics: []protocol.FetchTopicData{
			{
				Topic: "multi",
				Partitions: []protocol.FetchPartitionData{
					{Partition: 0, FetchOffset: 0, MaxBytes: 1024 * 1024},
				},
			},
		},
	})

	hwm := fetchResp.Topics[0].Partitions[0].HighWatermark
	if hwm != 2 {
		t.Fatalf("expected HWM 2 (next offset), got %d", hwm)
	}
}

func TestAutoCreateTopics(t *testing.T) {
	cfg := Config{
		DataDir:           t.TempDir(),
		DefaultPartitions: 2,
		MaxSegmentBytes:   1024 * 1024,
		AutoCreateTopics:  true,
		Addr:              ":0",
	}
	b := NewBroker(cfg)
	t.Cleanup(func() { b.Close() })

	batch := &log.RecordBatch{
		Records: []log.Record{{Value: []byte("auto-created")}},
	}

	resp := b.HandleProduce(&protocol.ProduceRequest{
		Header: protocol.RequestHeader{APIVersion: 3},
		Acks:   -1,
		Topics: []protocol.ProduceTopicData{
			{
				Topic: "new-topic",
				Partitions: []protocol.ProducePartitionData{
					{Partition: 0, Records: protocol.InternalToKafkaRecordBatch(batch)},
				},
			},
		},
	})

	if resp.Topics[0].Partitions[0].ErrorCode != protocol.ErrNone {
		t.Fatalf("auto-create produce error: %d", resp.Topics[0].Partitions[0].ErrorCode)
	}

	// Verify topic was created with default partitions
	topic := b.TopicManager.GetTopic("new-topic")
	if topic == nil {
		t.Fatal("topic was not auto-created")
	}
	if len(topic.Partitions) != 2 {
		t.Errorf("expected 2 partitions, got %d", len(topic.Partitions))
	}
}

func TestMetadata(t *testing.T) {
	b := testBroker(t)

	b.HandleCreateTopics(&protocol.CreateTopicsRequest{
		Topics: []protocol.CreateTopicRequest{
			{Topic: "t1", NumPartitions: 3},
			{Topic: "t2", NumPartitions: 1},
		},
	})

	// Get all metadata
	resp := b.HandleMetadata(&protocol.MetadataRequest{
		Header: protocol.RequestHeader{CorrelationID: 20},
	})

	if len(resp.Topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(resp.Topics))
	}

	// Get specific topic
	resp2 := b.HandleMetadata(&protocol.MetadataRequest{
		Header: protocol.RequestHeader{CorrelationID: 21},
		Topics: []string{"t1"},
	})

	if len(resp2.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp2.Topics))
	}
	if len(resp2.Topics[0].Partitions) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(resp2.Topics[0].Partitions))
	}

	// Verify replicas and ISR are populated
	if len(resp2.Topics[0].Partitions[0].Replicas) != 1 {
		t.Errorf("expected 1 replica, got %d", len(resp2.Topics[0].Partitions[0].Replicas))
	}
	if len(resp2.Topics[0].Partitions[0].ISR) != 1 {
		t.Errorf("expected 1 ISR, got %d", len(resp2.Topics[0].Partitions[0].ISR))
	}
}

func TestProduceUnknownTopic(t *testing.T) {
	b := testBroker(t) // AutoCreateTopics defaults to false

	batch := &log.RecordBatch{
		Records: []log.Record{{Value: []byte("test")}},
	}

	resp := b.HandleProduce(&protocol.ProduceRequest{
		Header: protocol.RequestHeader{APIVersion: 3},
		Acks:   -1,
		Topics: []protocol.ProduceTopicData{
			{
				Topic: "nonexistent",
				Partitions: []protocol.ProducePartitionData{
					{Partition: 0, Records: protocol.InternalToKafkaRecordBatch(batch)},
				},
			},
		},
	})

	if resp.Topics[0].Partitions[0].ErrorCode != protocol.ErrUnknownTopicOrPart {
		t.Fatalf("expected ErrUnknownTopicOrPart, got %d", resp.Topics[0].Partitions[0].ErrorCode)
	}
}

func TestFetchOffsetOutOfRange(t *testing.T) {
	b := testBroker(t)

	b.HandleCreateTopics(&protocol.CreateTopicsRequest{
		Topics: []protocol.CreateTopicRequest{
			{Topic: "test", NumPartitions: 1},
		},
	})

	// Produce a message so the log isn't empty
	batch := &log.RecordBatch{
		Records: []log.Record{{Value: []byte("msg")}},
	}
	b.HandleProduce(&protocol.ProduceRequest{
		Header: protocol.RequestHeader{APIVersion: 3},
		Acks:   -1,
		Topics: []protocol.ProduceTopicData{
			{Topic: "test", Partitions: []protocol.ProducePartitionData{
				{Partition: 0, Records: protocol.InternalToKafkaRecordBatch(batch)},
			}},
		},
	})

	// Fetch with offset -1 (before earliest)
	fetchResp := b.HandleFetch(&protocol.FetchRequest{
		Header:    protocol.RequestHeader{APIVersion: 4},
		ReplicaID: -1,
		Topics: []protocol.FetchTopicData{
			{Topic: "test", Partitions: []protocol.FetchPartitionData{
				{Partition: 0, FetchOffset: -1, MaxBytes: 1024 * 1024},
			}},
		},
	})

	if fetchResp.Topics[0].Partitions[0].ErrorCode != protocol.ErrOffsetOutOfRange {
		t.Fatalf("expected ErrOffsetOutOfRange, got %d", fetchResp.Topics[0].Partitions[0].ErrorCode)
	}
}

func TestApiVersions(t *testing.T) {
	b := testBroker(t)

	resp := b.HandleApiVersions(&protocol.ApiVersionsRequest{
		Header: protocol.RequestHeader{CorrelationID: 1, APIVersion: 0},
	})

	if resp.ErrorCode != protocol.ErrNone {
		t.Fatalf("error code: %d", resp.ErrorCode)
	}

	if len(resp.ApiVersions) == 0 {
		t.Fatal("expected api versions")
	}

	// Check that Produce, Fetch, Metadata, ApiVersions are in the list
	found := make(map[int16]bool)
	for _, av := range resp.ApiVersions {
		found[av.APIKey] = true
	}
	for _, key := range []int16{protocol.APIKeyProduce, protocol.APIKeyFetch, protocol.APIKeyMetadata, protocol.APIKeyApiVersions} {
		if !found[key] {
			t.Errorf("missing API key %d", key)
		}
	}
}

func TestListOffsets(t *testing.T) {
	b := testBroker(t)

	b.HandleCreateTopics(&protocol.CreateTopicsRequest{
		Topics: []protocol.CreateTopicRequest{
			{Topic: "test", NumPartitions: 1},
		},
	})

	// Produce some messages
	batch := &log.RecordBatch{
		Records: []log.Record{
			{Value: []byte("msg1")},
			{Value: []byte("msg2")},
		},
	}
	b.HandleProduce(&protocol.ProduceRequest{
		Header: protocol.RequestHeader{APIVersion: 3},
		Acks:   -1,
		Topics: []protocol.ProduceTopicData{
			{Topic: "test", Partitions: []protocol.ProducePartitionData{
				{Partition: 0, Records: protocol.InternalToKafkaRecordBatch(batch)},
			}},
		},
	})

	// List latest offset
	resp := b.HandleListOffsets(&protocol.ListOffsetsRequest{
		Header:    protocol.RequestHeader{CorrelationID: 1},
		ReplicaID: -1,
		Topics: []protocol.ListOffsetsTopic{
			{Topic: "test", Partitions: []protocol.ListOffsetsPartition{
				{Partition: 0, Timestamp: -1},
			}},
		},
	})

	if resp.Topics[0].Partitions[0].ErrorCode != protocol.ErrNone {
		t.Fatalf("error: %d", resp.Topics[0].Partitions[0].ErrorCode)
	}
	if resp.Topics[0].Partitions[0].Offset != 2 {
		t.Errorf("expected latest offset 2, got %d", resp.Topics[0].Partitions[0].Offset)
	}

	// List earliest offset
	resp2 := b.HandleListOffsets(&protocol.ListOffsetsRequest{
		Header:    protocol.RequestHeader{CorrelationID: 2},
		ReplicaID: -1,
		Topics: []protocol.ListOffsetsTopic{
			{Topic: "test", Partitions: []protocol.ListOffsetsPartition{
				{Partition: 0, Timestamp: -2},
			}},
		},
	})

	if resp2.Topics[0].Partitions[0].Offset != 0 {
		t.Errorf("expected earliest offset 0, got %d", resp2.Topics[0].Partitions[0].Offset)
	}
}
