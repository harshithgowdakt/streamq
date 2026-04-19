package protocol

import (
	"bytes"
	"fmt"
)

// API keys
const (
	APIKeyProduce      int16 = 0
	APIKeyFetch        int16 = 1
	APIKeyMetadata     int16 = 3
	APIKeyCreateTopics int16 = 19
)

// Error codes
const (
	ErrNone                 int16 = 0
	ErrUnknown              int16 = -1
	ErrUnknownTopicOrPart   int16 = 3
	ErrInvalidMessage       int16 = 4
	ErrTopicAlreadyExists   int16 = 36
	ErrInvalidPartitions    int16 = 37
)

// RequestHeader is the common header for all requests.
// Wire format: APIKey(int16) + APIVersion(int16) + CorrelationID(int32) + ClientID(string)
type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string
}

// ResponseHeader is the common header for all responses.
type ResponseHeader struct {
	CorrelationID int32
}

// --- Produce ---

type ProduceRequest struct {
	Header    RequestHeader
	TimeoutMs int32
	Topics    []ProduceTopicData
}

type ProduceTopicData struct {
	Topic      string
	Partitions []ProducePartitionData
}

type ProducePartitionData struct {
	Partition int32
	Records   []byte // raw RecordBatch bytes
}

type ProduceResponse struct {
	Header ResponseHeader
	Topics []ProduceTopicResponse
}

type ProduceTopicResponse struct {
	Topic      string
	Partitions []ProducePartitionResponse
}

type ProducePartitionResponse struct {
	Partition  int32
	ErrorCode  int16
	BaseOffset int64
}

// --- Fetch ---

type FetchRequest struct {
	Header  RequestHeader
	MaxWaitMs int32
	MinBytes  int32
	MaxBytes  int32
	Topics    []FetchTopicData
}

type FetchTopicData struct {
	Topic      string
	Partitions []FetchPartitionData
}

type FetchPartitionData struct {
	Partition   int32
	FetchOffset int64
	MaxBytes    int32
}

type FetchResponse struct {
	Header ResponseHeader
	Topics []FetchTopicResponse
}

type FetchTopicResponse struct {
	Topic      string
	Partitions []FetchPartitionResponse
}

type FetchPartitionResponse struct {
	Partition  int32
	ErrorCode  int16
	HighWatermark int64
	RecordBatches []byte // raw bytes of concatenated RecordBatches
}

// --- Metadata ---

type MetadataRequest struct {
	Header RequestHeader
	Topics []string // empty = all topics
}

type MetadataResponse struct {
	Header ResponseHeader
	Brokers []BrokerInfo
	Topics  []TopicMetadata
}

type BrokerInfo struct {
	NodeID int32
	Host   string
	Port   int32
}

type TopicMetadata struct {
	ErrorCode  int16
	Topic      string
	Partitions []PartitionMetadata
}

type PartitionMetadata struct {
	ErrorCode int16
	Partition int32
	Leader    int32
}

// --- CreateTopics ---

type CreateTopicsRequest struct {
	Header    RequestHeader
	Topics    []CreateTopicRequest
	TimeoutMs int32
}

type CreateTopicRequest struct {
	Topic         string
	NumPartitions int32
}

type CreateTopicsResponse struct {
	Header ResponseHeader
	Topics []CreateTopicResult
}

type CreateTopicResult struct {
	Topic     string
	ErrorCode int16
}

// EncodeRequest encodes a request to bytes (header + body, no frame length prefix).
func EncodeRequest(req interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	switch r := req.(type) {
	case *ProduceRequest:
		encodeRequestHeader(enc, &r.Header)
		enc.WriteInt32(r.TimeoutMs)
		enc.WriteInt32(int32(len(r.Topics)))
		for _, t := range r.Topics {
			enc.WriteString(t.Topic)
			enc.WriteInt32(int32(len(t.Partitions)))
			for _, p := range t.Partitions {
				enc.WriteInt32(p.Partition)
				enc.WriteBytes(p.Records)
			}
		}

	case *FetchRequest:
		encodeRequestHeader(enc, &r.Header)
		enc.WriteInt32(r.MaxWaitMs)
		enc.WriteInt32(r.MinBytes)
		enc.WriteInt32(r.MaxBytes)
		enc.WriteInt32(int32(len(r.Topics)))
		for _, t := range r.Topics {
			enc.WriteString(t.Topic)
			enc.WriteInt32(int32(len(t.Partitions)))
			for _, p := range t.Partitions {
				enc.WriteInt32(p.Partition)
				enc.WriteInt64(p.FetchOffset)
				enc.WriteInt32(p.MaxBytes)
			}
		}

	case *MetadataRequest:
		encodeRequestHeader(enc, &r.Header)
		enc.WriteInt32(int32(len(r.Topics)))
		for _, t := range r.Topics {
			enc.WriteString(t)
		}

	case *CreateTopicsRequest:
		encodeRequestHeader(enc, &r.Header)
		enc.WriteInt32(int32(len(r.Topics)))
		for _, t := range r.Topics {
			enc.WriteString(t.Topic)
			enc.WriteInt32(t.NumPartitions)
		}
		enc.WriteInt32(r.TimeoutMs)

	default:
		return nil, fmt.Errorf("unknown request type: %T", req)
	}

	if enc.Err() != nil {
		return nil, enc.Err()
	}
	return buf.Bytes(), nil
}

// DecodeRequest decodes a request from bytes (after frame length has been read).
func DecodeRequest(data []byte) (interface{}, error) {
	dec := NewDecoder(bytes.NewReader(data))

	header := RequestHeader{
		APIKey:        dec.ReadInt16(),
		APIVersion:    dec.ReadInt16(),
		CorrelationID: dec.ReadInt32(),
		ClientID:      dec.ReadString(),
	}
	if dec.Err() != nil {
		return nil, fmt.Errorf("decode header: %w", dec.Err())
	}

	switch header.APIKey {
	case APIKeyProduce:
		req := &ProduceRequest{Header: header}
		req.TimeoutMs = dec.ReadInt32()
		topicCount := dec.ReadInt32()
		req.Topics = make([]ProduceTopicData, topicCount)
		for i := range req.Topics {
			req.Topics[i].Topic = dec.ReadString()
			partCount := dec.ReadInt32()
			req.Topics[i].Partitions = make([]ProducePartitionData, partCount)
			for j := range req.Topics[i].Partitions {
				req.Topics[i].Partitions[j].Partition = dec.ReadInt32()
				req.Topics[i].Partitions[j].Records = dec.ReadBytes()
			}
		}
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return req, nil

	case APIKeyFetch:
		req := &FetchRequest{Header: header}
		req.MaxWaitMs = dec.ReadInt32()
		req.MinBytes = dec.ReadInt32()
		req.MaxBytes = dec.ReadInt32()
		topicCount := dec.ReadInt32()
		req.Topics = make([]FetchTopicData, topicCount)
		for i := range req.Topics {
			req.Topics[i].Topic = dec.ReadString()
			partCount := dec.ReadInt32()
			req.Topics[i].Partitions = make([]FetchPartitionData, partCount)
			for j := range req.Topics[i].Partitions {
				req.Topics[i].Partitions[j].Partition = dec.ReadInt32()
				req.Topics[i].Partitions[j].FetchOffset = dec.ReadInt64()
				req.Topics[i].Partitions[j].MaxBytes = dec.ReadInt32()
			}
		}
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return req, nil

	case APIKeyMetadata:
		req := &MetadataRequest{Header: header}
		topicCount := dec.ReadInt32()
		req.Topics = make([]string, topicCount)
		for i := range req.Topics {
			req.Topics[i] = dec.ReadString()
		}
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return req, nil

	case APIKeyCreateTopics:
		req := &CreateTopicsRequest{Header: header}
		topicCount := dec.ReadInt32()
		req.Topics = make([]CreateTopicRequest, topicCount)
		for i := range req.Topics {
			req.Topics[i].Topic = dec.ReadString()
			req.Topics[i].NumPartitions = dec.ReadInt32()
		}
		req.TimeoutMs = dec.ReadInt32()
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return req, nil

	default:
		return nil, fmt.Errorf("unknown API key: %d", header.APIKey)
	}
}

// EncodeResponse encodes a response to bytes (header + body, no frame length prefix).
func EncodeResponse(resp interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	switch r := resp.(type) {
	case *ProduceResponse:
		enc.WriteInt32(r.Header.CorrelationID)
		enc.WriteInt32(int32(len(r.Topics)))
		for _, t := range r.Topics {
			enc.WriteString(t.Topic)
			enc.WriteInt32(int32(len(t.Partitions)))
			for _, p := range t.Partitions {
				enc.WriteInt32(p.Partition)
				enc.WriteInt16(p.ErrorCode)
				enc.WriteInt64(p.BaseOffset)
			}
		}

	case *FetchResponse:
		enc.WriteInt32(r.Header.CorrelationID)
		enc.WriteInt32(int32(len(r.Topics)))
		for _, t := range r.Topics {
			enc.WriteString(t.Topic)
			enc.WriteInt32(int32(len(t.Partitions)))
			for _, p := range t.Partitions {
				enc.WriteInt32(p.Partition)
				enc.WriteInt16(p.ErrorCode)
				enc.WriteInt64(p.HighWatermark)
				enc.WriteBytes(p.RecordBatches)
			}
		}

	case *MetadataResponse:
		enc.WriteInt32(r.Header.CorrelationID)
		enc.WriteInt32(int32(len(r.Brokers)))
		for _, b := range r.Brokers {
			enc.WriteInt32(b.NodeID)
			enc.WriteString(b.Host)
			enc.WriteInt32(b.Port)
		}
		enc.WriteInt32(int32(len(r.Topics)))
		for _, t := range r.Topics {
			enc.WriteInt16(t.ErrorCode)
			enc.WriteString(t.Topic)
			enc.WriteInt32(int32(len(t.Partitions)))
			for _, p := range t.Partitions {
				enc.WriteInt16(p.ErrorCode)
				enc.WriteInt32(p.Partition)
				enc.WriteInt32(p.Leader)
			}
		}

	case *CreateTopicsResponse:
		enc.WriteInt32(r.Header.CorrelationID)
		enc.WriteInt32(int32(len(r.Topics)))
		for _, t := range r.Topics {
			enc.WriteString(t.Topic)
			enc.WriteInt16(t.ErrorCode)
		}

	default:
		return nil, fmt.Errorf("unknown response type: %T", resp)
	}

	if enc.Err() != nil {
		return nil, enc.Err()
	}
	return buf.Bytes(), nil
}

// DecodeResponse decodes a response from bytes given the API key.
func DecodeResponse(apiKey int16, data []byte) (interface{}, error) {
	dec := NewDecoder(bytes.NewReader(data))

	correlationID := dec.ReadInt32()

	switch apiKey {
	case APIKeyProduce:
		resp := &ProduceResponse{Header: ResponseHeader{CorrelationID: correlationID}}
		topicCount := dec.ReadInt32()
		resp.Topics = make([]ProduceTopicResponse, topicCount)
		for i := range resp.Topics {
			resp.Topics[i].Topic = dec.ReadString()
			partCount := dec.ReadInt32()
			resp.Topics[i].Partitions = make([]ProducePartitionResponse, partCount)
			for j := range resp.Topics[i].Partitions {
				resp.Topics[i].Partitions[j].Partition = dec.ReadInt32()
				resp.Topics[i].Partitions[j].ErrorCode = dec.ReadInt16()
				resp.Topics[i].Partitions[j].BaseOffset = dec.ReadInt64()
			}
		}
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return resp, nil

	case APIKeyFetch:
		resp := &FetchResponse{Header: ResponseHeader{CorrelationID: correlationID}}
		topicCount := dec.ReadInt32()
		resp.Topics = make([]FetchTopicResponse, topicCount)
		for i := range resp.Topics {
			resp.Topics[i].Topic = dec.ReadString()
			partCount := dec.ReadInt32()
			resp.Topics[i].Partitions = make([]FetchPartitionResponse, partCount)
			for j := range resp.Topics[i].Partitions {
				resp.Topics[i].Partitions[j].Partition = dec.ReadInt32()
				resp.Topics[i].Partitions[j].ErrorCode = dec.ReadInt16()
				resp.Topics[i].Partitions[j].HighWatermark = dec.ReadInt64()
				resp.Topics[i].Partitions[j].RecordBatches = dec.ReadBytes()
			}
		}
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return resp, nil

	case APIKeyMetadata:
		resp := &MetadataResponse{Header: ResponseHeader{CorrelationID: correlationID}}
		brokerCount := dec.ReadInt32()
		resp.Brokers = make([]BrokerInfo, brokerCount)
		for i := range resp.Brokers {
			resp.Brokers[i].NodeID = dec.ReadInt32()
			resp.Brokers[i].Host = dec.ReadString()
			resp.Brokers[i].Port = dec.ReadInt32()
		}
		topicCount := dec.ReadInt32()
		resp.Topics = make([]TopicMetadata, topicCount)
		for i := range resp.Topics {
			resp.Topics[i].ErrorCode = dec.ReadInt16()
			resp.Topics[i].Topic = dec.ReadString()
			partCount := dec.ReadInt32()
			resp.Topics[i].Partitions = make([]PartitionMetadata, partCount)
			for j := range resp.Topics[i].Partitions {
				resp.Topics[i].Partitions[j].ErrorCode = dec.ReadInt16()
				resp.Topics[i].Partitions[j].Partition = dec.ReadInt32()
				resp.Topics[i].Partitions[j].Leader = dec.ReadInt32()
			}
		}
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return resp, nil

	case APIKeyCreateTopics:
		resp := &CreateTopicsResponse{Header: ResponseHeader{CorrelationID: correlationID}}
		topicCount := dec.ReadInt32()
		resp.Topics = make([]CreateTopicResult, topicCount)
		for i := range resp.Topics {
			resp.Topics[i].Topic = dec.ReadString()
			resp.Topics[i].ErrorCode = dec.ReadInt16()
		}
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return resp, nil

	default:
		return nil, fmt.Errorf("unknown API key: %d", apiKey)
	}
}

func encodeRequestHeader(enc *Encoder, h *RequestHeader) {
	enc.WriteInt16(h.APIKey)
	enc.WriteInt16(h.APIVersion)
	enc.WriteInt32(h.CorrelationID)
	enc.WriteString(h.ClientID)
}
