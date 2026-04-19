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

	// Produce
	batch := &log.RecordBatch{
		Records: []log.Record{
			{Key: []byte("k1"), Value: []byte("hello")},
			{Key: []byte("k2"), Value: []byte("world")},
		},
	}
	batchBytes := batch.Encode()

	produceResp := b.HandleProduce(&protocol.ProduceRequest{
		Header: protocol.RequestHeader{CorrelationID: 10},
		Topics: []protocol.ProduceTopicData{
			{
				Topic: "test",
				Partitions: []protocol.ProducePartitionData{
					{Partition: 0, Records: batchBytes},
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
		Header: protocol.RequestHeader{CorrelationID: 11},
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

	decoded, err := log.DecodeRecordBatch(rawBatches)
	if err != nil {
		t.Fatalf("decode fetched batch: %v", err)
	}

	if len(decoded.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(decoded.Records))
	}
	if string(decoded.Records[0].Value) != "hello" {
		t.Errorf("record 0 value: %q", decoded.Records[0].Value)
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
}

func TestProduceUnknownTopic(t *testing.T) {
	b := testBroker(t)

	batch := &log.RecordBatch{
		Records: []log.Record{{Value: []byte("test")}},
	}

	resp := b.HandleProduce(&protocol.ProduceRequest{
		Topics: []protocol.ProduceTopicData{
			{
				Topic: "nonexistent",
				Partitions: []protocol.ProducePartitionData{
					{Partition: 0, Records: batch.Encode()},
				},
			},
		},
	})

	if resp.Topics[0].Partitions[0].ErrorCode != protocol.ErrUnknownTopicOrPart {
		t.Fatalf("expected ErrUnknownTopicOrPart, got %d", resp.Topics[0].Partitions[0].ErrorCode)
	}
}
