package protocol

import (
	"bytes"
	"testing"
)

func TestEncoderDecoder(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	enc.WriteInt8(42)
	enc.WriteInt16(1000)
	enc.WriteInt32(100000)
	enc.WriteInt64(9999999999)
	enc.WriteString("hello")
	enc.WriteBytes([]byte("world"))
	enc.WriteBytes(nil) // nil bytes

	if enc.Err() != nil {
		t.Fatalf("encode: %v", enc.Err())
	}

	dec := NewDecoder(&buf)
	if v := dec.ReadInt8(); v != 42 {
		t.Errorf("int8: got %d", v)
	}
	if v := dec.ReadInt16(); v != 1000 {
		t.Errorf("int16: got %d", v)
	}
	if v := dec.ReadInt32(); v != 100000 {
		t.Errorf("int32: got %d", v)
	}
	if v := dec.ReadInt64(); v != 9999999999 {
		t.Errorf("int64: got %d", v)
	}
	if v := dec.ReadString(); v != "hello" {
		t.Errorf("string: got %q", v)
	}
	if v := dec.ReadBytes(); string(v) != "world" {
		t.Errorf("bytes: got %q", v)
	}
	if v := dec.ReadBytes(); v != nil {
		t.Errorf("nil bytes: got %v", v)
	}
	if dec.Err() != nil {
		t.Fatalf("decode: %v", dec.Err())
	}
}

func TestVarintRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	enc.WriteVarint(0)
	enc.WriteVarint(1)
	enc.WriteVarint(-1)
	enc.WriteVarint(300)
	enc.WriteVarint(-300)
	enc.WriteUvarint(0)
	enc.WriteUvarint(127)
	enc.WriteUvarint(128)

	if enc.Err() != nil {
		t.Fatalf("encode: %v", enc.Err())
	}

	dec := NewDecoder(&buf)
	if v := dec.ReadVarint(); v != 0 {
		t.Errorf("varint 0: got %d", v)
	}
	if v := dec.ReadVarint(); v != 1 {
		t.Errorf("varint 1: got %d", v)
	}
	if v := dec.ReadVarint(); v != -1 {
		t.Errorf("varint -1: got %d", v)
	}
	if v := dec.ReadVarint(); v != 300 {
		t.Errorf("varint 300: got %d", v)
	}
	if v := dec.ReadVarint(); v != -300 {
		t.Errorf("varint -300: got %d", v)
	}
	if v := dec.ReadUvarint(); v != 0 {
		t.Errorf("uvarint 0: got %d", v)
	}
	if v := dec.ReadUvarint(); v != 127 {
		t.Errorf("uvarint 127: got %d", v)
	}
	if v := dec.ReadUvarint(); v != 128 {
		t.Errorf("uvarint 128: got %d", v)
	}
	if dec.Err() != nil {
		t.Fatalf("decode: %v", dec.Err())
	}
}

func TestCompactStringRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	enc.WriteCompactString("hello")
	enc.WriteCompactString("")
	enc.WriteTaggedFields()

	if enc.Err() != nil {
		t.Fatalf("encode: %v", enc.Err())
	}

	dec := NewDecoder(&buf)
	if v := dec.ReadCompactString(); v != "hello" {
		t.Errorf("compact string: got %q", v)
	}
	if v := dec.ReadCompactString(); v != "" {
		t.Errorf("empty compact string: got %q", v)
	}
	dec.ReadTaggedFields()
	if dec.Err() != nil {
		t.Fatalf("decode: %v", dec.Err())
	}
}

func TestProduceRequestRoundTrip(t *testing.T) {
	req := &ProduceRequest{
		Header: RequestHeader{
			APIKey:        APIKeyProduce,
			APIVersion:    3,
			CorrelationID: 1,
			ClientID:      "test-client",
		},
		Acks:      -1,
		TimeoutMs: 5000,
		Topics: []ProduceTopicData{
			{
				Topic: "test-topic",
				Partitions: []ProducePartitionData{
					{Partition: 0, Records: []byte("fake-records")},
				},
			},
		},
	}

	data, err := EncodeRequest(req)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeRequest(data)
	if err != nil {
		t.Fatal(err)
	}

	got := decoded.(*ProduceRequest)
	if got.Header.CorrelationID != 1 {
		t.Errorf("correlationID: %d", got.Header.CorrelationID)
	}
	if got.Acks != -1 {
		t.Errorf("acks: %d", got.Acks)
	}
	if got.TimeoutMs != 5000 {
		t.Errorf("timeoutMs: %d", got.TimeoutMs)
	}
	if len(got.Topics) != 1 || got.Topics[0].Topic != "test-topic" {
		t.Errorf("topics: %+v", got.Topics)
	}
	if string(got.Topics[0].Partitions[0].Records) != "fake-records" {
		t.Errorf("records: %q", got.Topics[0].Partitions[0].Records)
	}
}

func TestFetchRequestRoundTrip(t *testing.T) {
	req := &FetchRequest{
		Header: RequestHeader{
			APIKey:        APIKeyFetch,
			APIVersion:    4,
			CorrelationID: 2,
			ClientID:      "test",
		},
		ReplicaID: -1,
		MaxWaitMs: 500,
		MinBytes:  1,
		MaxBytes:  1048576,
		Topics: []FetchTopicData{
			{
				Topic: "my-topic",
				Partitions: []FetchPartitionData{
					{Partition: 0, FetchOffset: 100, MaxBytes: 65536},
				},
			},
		},
	}

	data, err := EncodeRequest(req)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeRequest(data)
	if err != nil {
		t.Fatal(err)
	}

	got := decoded.(*FetchRequest)
	if got.ReplicaID != -1 {
		t.Errorf("replicaID: %d", got.ReplicaID)
	}
	if got.Topics[0].Partitions[0].FetchOffset != 100 {
		t.Errorf("fetchOffset: %d", got.Topics[0].Partitions[0].FetchOffset)
	}
}

func TestMetadataRequestRoundTrip(t *testing.T) {
	req := &MetadataRequest{
		Header: RequestHeader{
			APIKey:        APIKeyMetadata,
			CorrelationID: 3,
			ClientID:      "test",
		},
		Topics: []string{"topic-a", "topic-b"},
	}

	data, err := EncodeRequest(req)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeRequest(data)
	if err != nil {
		t.Fatal(err)
	}

	got := decoded.(*MetadataRequest)
	if len(got.Topics) != 2 || got.Topics[0] != "topic-a" {
		t.Errorf("topics: %v", got.Topics)
	}
}

func TestCreateTopicsRoundTrip(t *testing.T) {
	req := &CreateTopicsRequest{
		Header: RequestHeader{
			APIKey:        APIKeyCreateTopics,
			CorrelationID: 4,
			ClientID:      "test",
		},
		Topics: []CreateTopicRequest{
			{Topic: "new-topic", NumPartitions: 8},
		},
		TimeoutMs: 10000,
	}

	data, err := EncodeRequest(req)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeRequest(data)
	if err != nil {
		t.Fatal(err)
	}

	got := decoded.(*CreateTopicsRequest)
	if got.Topics[0].NumPartitions != 8 {
		t.Errorf("numPartitions: %d", got.Topics[0].NumPartitions)
	}
}

func TestProduceResponseRoundTrip(t *testing.T) {
	resp := &ProduceResponse{
		Header: ResponseHeader{CorrelationID: 1},
		Topics: []ProduceTopicResponse{
			{
				Topic: "test",
				Partitions: []ProducePartitionResponse{
					{Partition: 0, ErrorCode: ErrNone, BaseOffset: 42},
				},
			},
		},
	}

	data, err := EncodeResponse(resp)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeResponse(APIKeyProduce, 0, data)
	if err != nil {
		t.Fatal(err)
	}

	got := decoded.(*ProduceResponse)
	if got.Topics[0].Partitions[0].BaseOffset != 42 {
		t.Errorf("baseOffset: %d", got.Topics[0].Partitions[0].BaseOffset)
	}
}

func TestFetchResponseRoundTrip(t *testing.T) {
	resp := &FetchResponse{
		Header: ResponseHeader{CorrelationID: 2},
		Topics: []FetchTopicResponse{
			{
				Topic: "test",
				Partitions: []FetchPartitionResponse{
					{Partition: 0, ErrorCode: ErrNone, HighWatermark: 100, RecordBatches: []byte("batch-data")},
				},
			},
		},
	}

	data, err := EncodeResponse(resp)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeResponse(APIKeyFetch, 0, data)
	if err != nil {
		t.Fatal(err)
	}

	got := decoded.(*FetchResponse)
	if string(got.Topics[0].Partitions[0].RecordBatches) != "batch-data" {
		t.Errorf("recordBatches: %q", got.Topics[0].Partitions[0].RecordBatches)
	}
}

func TestApiVersionsRoundTrip(t *testing.T) {
	req := &ApiVersionsRequest{
		Header: RequestHeader{
			APIKey:        APIKeyApiVersions,
			APIVersion:    0,
			CorrelationID: 5,
			ClientID:      "test",
		},
	}

	data, err := EncodeRequest(req)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeRequest(data)
	if err != nil {
		t.Fatal(err)
	}

	got := decoded.(*ApiVersionsRequest)
	if got.Header.APIKey != APIKeyApiVersions {
		t.Errorf("apiKey: %d", got.Header.APIKey)
	}
}
