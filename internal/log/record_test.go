package log

import (
	"testing"
)

func TestRecordBatchRoundTrip(t *testing.T) {
	batch := &RecordBatch{
		BaseOffset: 42,
		Attributes: 0,
		Records: []Record{
			{Key: []byte("key1"), Value: []byte("value1")},
			{Key: nil, Value: []byte("value2")},
			{Key: []byte("key3"), Value: nil},
		},
	}

	data := batch.Encode()
	decoded, err := DecodeRecordBatch(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.BaseOffset != batch.BaseOffset {
		t.Errorf("baseOffset: got %d, want %d", decoded.BaseOffset, batch.BaseOffset)
	}
	if len(decoded.Records) != len(batch.Records) {
		t.Fatalf("record count: got %d, want %d", len(decoded.Records), len(batch.Records))
	}

	// Record 0: both key and value
	if string(decoded.Records[0].Key) != "key1" {
		t.Errorf("record 0 key: got %q", decoded.Records[0].Key)
	}
	if string(decoded.Records[0].Value) != "value1" {
		t.Errorf("record 0 value: got %q", decoded.Records[0].Value)
	}

	// Record 1: nil key
	if decoded.Records[1].Key != nil {
		t.Errorf("record 1 key: expected nil, got %q", decoded.Records[1].Key)
	}

	// Record 2: nil value
	if decoded.Records[2].Value != nil {
		t.Errorf("record 2 value: expected nil, got %q", decoded.Records[2].Value)
	}
}

func TestRecordBatchCRCValidation(t *testing.T) {
	batch := &RecordBatch{
		Records: []Record{{Key: []byte("k"), Value: []byte("v")}},
	}

	data := batch.Encode()
	// Corrupt a byte in the records area
	data[len(data)-1] ^= 0xFF

	_, err := DecodeRecordBatch(data)
	if err == nil {
		t.Fatal("expected CRC error on corrupted data")
	}
}

func TestBatchSize(t *testing.T) {
	batch := &RecordBatch{
		Records: []Record{{Key: []byte("hello"), Value: []byte("world")}},
	}
	data := batch.Encode()
	if batch.BatchSize() != len(data) {
		t.Errorf("BatchSize() = %d, len(Encode()) = %d", batch.BatchSize(), len(data))
	}
}
