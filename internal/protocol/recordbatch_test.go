package protocol

import (
	"testing"

	"github.com/harshithgowda/streamq/internal/log"
)

func TestKafkaRecordBatchRoundTrip(t *testing.T) {
	batch := &log.RecordBatch{
		BaseOffset: 42,
		Records: []log.Record{
			{Key: []byte("key1"), Value: []byte("value1")},
			{Key: []byte("key2"), Value: []byte("value2")},
			{Key: nil, Value: []byte("no-key")},
		},
	}

	encoded := InternalToKafkaRecordBatch(batch)

	batches, err := KafkaRecordBatchToInternal(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}

	got := batches[0]
	if got.BaseOffset != 42 {
		t.Errorf("baseOffset: got %d, want 42", got.BaseOffset)
	}
	if len(got.Records) != 3 {
		t.Fatalf("records: got %d, want 3", len(got.Records))
	}

	if string(got.Records[0].Key) != "key1" {
		t.Errorf("record 0 key: %q", got.Records[0].Key)
	}
	if string(got.Records[0].Value) != "value1" {
		t.Errorf("record 0 value: %q", got.Records[0].Value)
	}
	if string(got.Records[1].Key) != "key2" {
		t.Errorf("record 1 key: %q", got.Records[1].Key)
	}
	if got.Records[2].Key != nil {
		t.Errorf("record 2 key should be nil, got %q", got.Records[2].Key)
	}
	if string(got.Records[2].Value) != "no-key" {
		t.Errorf("record 2 value: %q", got.Records[2].Value)
	}
}

func TestKafkaRecordBatchConcatenated(t *testing.T) {
	batch1 := &log.RecordBatch{
		BaseOffset: 0,
		Records:    []log.Record{{Value: []byte("a")}},
	}
	batch2 := &log.RecordBatch{
		BaseOffset: 1,
		Records:    []log.Record{{Value: []byte("b")}},
	}

	combined := append(InternalToKafkaRecordBatch(batch1), InternalToKafkaRecordBatch(batch2)...)

	batches, err := KafkaRecordBatchToInternal(combined)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}

	if string(batches[0].Records[0].Value) != "a" {
		t.Errorf("batch 0 value: %q", batches[0].Records[0].Value)
	}
	if string(batches[1].Records[0].Value) != "b" {
		t.Errorf("batch 1 value: %q", batches[1].Records[0].Value)
	}
}

func TestKafkaRecordBatchNilValues(t *testing.T) {
	batch := &log.RecordBatch{
		Records: []log.Record{
			{Key: nil, Value: nil},
			{Key: []byte{}, Value: []byte{}},
		},
	}

	encoded := InternalToKafkaRecordBatch(batch)
	batches, err := KafkaRecordBatchToInternal(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(batches[0].Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(batches[0].Records))
	}

	// nil becomes nil
	if batches[0].Records[0].Key != nil {
		t.Errorf("expected nil key, got %v", batches[0].Records[0].Key)
	}
	if batches[0].Records[0].Value != nil {
		t.Errorf("expected nil value, got %v", batches[0].Records[0].Value)
	}

	// empty becomes empty
	if batches[0].Records[1].Key == nil || len(batches[0].Records[1].Key) != 0 {
		t.Errorf("expected empty key, got %v", batches[0].Records[1].Key)
	}
	if batches[0].Records[1].Value == nil || len(batches[0].Records[1].Value) != 0 {
		t.Errorf("expected empty value, got %v", batches[0].Records[1].Value)
	}
}

func TestKafkaRecordBatchCRCValidation(t *testing.T) {
	batch := &log.RecordBatch{
		Records: []log.Record{{Value: []byte("test")}},
	}

	encoded := InternalToKafkaRecordBatch(batch)

	// Corrupt a byte in the CRC-covered region
	encoded[25] ^= 0xFF

	_, err := KafkaRecordBatchToInternal(encoded)
	if err == nil {
		t.Fatal("expected CRC error")
	}
}
