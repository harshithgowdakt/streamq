package protocol

import (
	"bytes"
	"fmt"
)

// API keys
const (
	APIKeyProduce         int16 = 0
	APIKeyFetch           int16 = 1
	APIKeyListOffsets     int16 = 2
	APIKeyMetadata        int16 = 3
	APIKeyOffsetCommit    int16 = 8
	APIKeyOffsetFetch     int16 = 9
	APIKeyFindCoordinator int16 = 10
	APIKeyJoinGroup       int16 = 11
	APIKeyHeartbeat       int16 = 12
	APIKeyLeaveGroup      int16 = 13
	APIKeySyncGroup       int16 = 14
	APIKeyApiVersions     int16 = 18
	APIKeyCreateTopics    int16 = 19
)

// Error codes
const (
	ErrNone                         int16 = 0
	ErrUnknown                      int16 = -1
	ErrOffsetOutOfRange             int16 = 1
	ErrCorruptMessage               int16 = 2
	ErrUnknownTopicOrPart           int16 = 3
	ErrInvalidMessage               int16 = 4
	ErrGroupCoordinatorNotAvailable int16 = 15
	ErrNotCoordinator               int16 = 16
	ErrNotLeaderForPartition        int16 = 6
	ErrRequestTimedOut              int16 = 7
	ErrNotEnoughReplicas            int16 = 19
	ErrIllegalGeneration            int16 = 22
	ErrInconsistentGroupProtocol    int16 = 23
	ErrUnknownMemberID              int16 = 25
	ErrRebalanceInProgress          int16 = 27
	ErrUnsupportedVersion           int16 = 35
	ErrTopicAlreadyExists           int16 = 36
	ErrInvalidPartitions            int16 = 37
)

// RequestHeader is the common header for all requests.
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
	Header          RequestHeader
	TransactionalID *string // v3+
	Acks            int16
	TimeoutMs       int32
	Topics          []ProduceTopicData
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
	Header  ResponseHeader
	Version int16
	Topics  []ProduceTopicResponse
}

type ProduceTopicResponse struct {
	Topic      string
	Partitions []ProducePartitionResponse
}

type ProducePartitionResponse struct {
	Partition       int32
	ErrorCode       int16
	BaseOffset      int64
	LogAppendTimeMs int64 // v2+
}

// --- Fetch ---

type FetchRequest struct {
	Header         RequestHeader
	ReplicaID      int32 // v4
	MaxWaitMs      int32
	MinBytes       int32
	MaxBytes       int32
	IsolationLevel int8 // v4
	Topics         []FetchTopicData
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
	Header  ResponseHeader
	Version int16
	Topics  []FetchTopicResponse
}

type FetchTopicResponse struct {
	Topic      string
	Partitions []FetchPartitionResponse
}

type FetchPartitionResponse struct {
	Partition     int32
	ErrorCode     int16
	HighWatermark int64
	RecordBatches []byte // raw bytes of concatenated RecordBatches
}

// --- Metadata ---

type MetadataRequest struct {
	Header RequestHeader
	Topics []string // empty = all topics
}

type MetadataResponse struct {
	Header  ResponseHeader
	Version int16
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
	Replicas  []int32
	ISR       []int32
}

// --- CreateTopics ---

type CreateTopicsRequest struct {
	Header       RequestHeader
	Topics       []CreateTopicRequest
	TimeoutMs    int32
	ValidateOnly bool
}

type CreateTopicRequest struct {
	Topic             string
	NumPartitions     int32
	ReplicationFactor int16
}

type CreateTopicsResponse struct {
	Header ResponseHeader
	Topics []CreateTopicResult
}

type CreateTopicResult struct {
	Topic     string
	ErrorCode int16
}

// --- ApiVersions ---

type ApiVersionsRequest struct {
	Header RequestHeader
}

type ApiVersionsResponse struct {
	Header      ResponseHeader
	Version     int16
	ErrorCode   int16
	ApiVersions []ApiVersionRange
}

type ApiVersionRange struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

// --- ListOffsets ---

type ListOffsetsRequest struct {
	Header    RequestHeader
	ReplicaID int32
	Topics    []ListOffsetsTopic
}

type ListOffsetsTopic struct {
	Topic      string
	Partitions []ListOffsetsPartition
}

type ListOffsetsPartition struct {
	Partition int32
	Timestamp int64 // -1=latest, -2=earliest
}

type ListOffsetsResponse struct {
	Header ResponseHeader
	Topics []ListOffsetsTopicResponse
}

type ListOffsetsTopicResponse struct {
	Topic      string
	Partitions []ListOffsetsPartitionResponse
}

type ListOffsetsPartitionResponse struct {
	Partition int32
	ErrorCode int16
	Timestamp int64
	Offset    int64
}

// --- FindCoordinator ---

type FindCoordinatorRequest struct {
	Header  RequestHeader
	Key     string // group id
	KeyType int8   // 0=group, 1=transactional
}

type FindCoordinatorResponse struct {
	Header    ResponseHeader
	ErrorCode int16
	NodeID    int32
	Host      string
	Port      int32
}

// --- JoinGroup ---

type JoinGroupProtocol struct {
	Name     string
	Metadata []byte
}

type JoinGroupRequest struct {
	Header             RequestHeader
	GroupID            string
	SessionTimeoutMs   int32
	RebalanceTimeoutMs int32 // v1+
	MemberID           string
	ProtocolType       string
	Protocols          []JoinGroupProtocol
}

type JoinGroupMember struct {
	MemberID string
	Metadata []byte
}

type JoinGroupResponse struct {
	Header       ResponseHeader
	ErrorCode    int16
	GenerationID int32
	ProtocolName string
	Leader       string
	MemberID     string
	Members      []JoinGroupMember
}

// --- SyncGroup ---

type SyncGroupAssignment struct {
	MemberID   string
	Assignment []byte
}

type SyncGroupRequest struct {
	Header       RequestHeader
	GroupID      string
	GenerationID int32
	MemberID     string
	Assignments  []SyncGroupAssignment
}

type SyncGroupResponse struct {
	Header     ResponseHeader
	ErrorCode  int16
	Assignment []byte
}

// --- Heartbeat ---

type HeartbeatRequest struct {
	Header       RequestHeader
	GroupID      string
	GenerationID int32
	MemberID     string
}

type HeartbeatResponse struct {
	Header    ResponseHeader
	ErrorCode int16
}

// --- LeaveGroup ---

type LeaveGroupRequest struct {
	Header   RequestHeader
	GroupID  string
	MemberID string
}

type LeaveGroupResponse struct {
	Header    ResponseHeader
	ErrorCode int16
}

// --- OffsetCommit ---

type OffsetCommitPartition struct {
	Partition int32
	Offset    int64
	Metadata  *string
}

type OffsetCommitTopic struct {
	Topic      string
	Partitions []OffsetCommitPartition
}

type OffsetCommitRequest struct {
	Header       RequestHeader
	GroupID      string
	GenerationID int32
	MemberID     string
	RetentionMs  int64
	Topics       []OffsetCommitTopic
}

type OffsetCommitPartitionResponse struct {
	Partition int32
	ErrorCode int16
}

type OffsetCommitTopicResponse struct {
	Topic      string
	Partitions []OffsetCommitPartitionResponse
}

type OffsetCommitResponse struct {
	Header ResponseHeader
	Topics []OffsetCommitTopicResponse
}

// --- OffsetFetch ---

type OffsetFetchRequest struct {
	Header  RequestHeader
	GroupID string
	Topics  []OffsetFetchTopic
}

type OffsetFetchTopic struct {
	Topic      string
	Partitions []int32
}

type OffsetFetchResponse struct {
	Header ResponseHeader
	Topics []OffsetFetchTopicResponse
}

type OffsetFetchTopicResponse struct {
	Topic      string
	Partitions []OffsetFetchPartitionResponse
}

type OffsetFetchPartitionResponse struct {
	Partition       int32
	CommittedOffset int64
	Metadata        *string
	ErrorCode       int16
}

// EncodeRequest encodes a request to bytes (header + body, no frame length prefix).
func EncodeRequest(req interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	switch r := req.(type) {
	case *ProduceRequest:
		encodeRequestHeader(enc, &r.Header)
		if r.Header.APIVersion >= 3 {
			enc.WriteNullableString(r.TransactionalID)
		}
		enc.WriteInt16(r.Acks)
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
		enc.WriteInt32(r.ReplicaID)
		enc.WriteInt32(r.MaxWaitMs)
		enc.WriteInt32(r.MinBytes)
		if r.Header.APIVersion >= 3 {
			enc.WriteInt32(r.MaxBytes)
		}
		if r.Header.APIVersion >= 4 {
			enc.WriteInt8(r.IsolationLevel)
		}
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
			enc.WriteInt16(t.ReplicationFactor)
			enc.WriteInt32(0) // num assignments
			enc.WriteInt32(0) // num configs
		}
		enc.WriteInt32(r.TimeoutMs)

	case *ApiVersionsRequest:
		encodeRequestHeader(enc, &r.Header)

	case *ListOffsetsRequest:
		encodeRequestHeader(enc, &r.Header)
		enc.WriteInt32(r.ReplicaID)
		enc.WriteInt32(int32(len(r.Topics)))
		for _, t := range r.Topics {
			enc.WriteString(t.Topic)
			enc.WriteInt32(int32(len(t.Partitions)))
			for _, p := range t.Partitions {
				enc.WriteInt32(p.Partition)
				enc.WriteInt64(p.Timestamp)
			}
		}

	case *FindCoordinatorRequest:
		encodeRequestHeader(enc, &r.Header)
		enc.WriteString(r.Key)

	case *JoinGroupRequest:
		encodeRequestHeader(enc, &r.Header)
		enc.WriteString(r.GroupID)
		enc.WriteInt32(r.SessionTimeoutMs)
		if r.Header.APIVersion >= 1 {
			enc.WriteInt32(r.RebalanceTimeoutMs)
		}
		enc.WriteString(r.MemberID)
		enc.WriteString(r.ProtocolType)
		enc.WriteInt32(int32(len(r.Protocols)))
		for _, p := range r.Protocols {
			enc.WriteString(p.Name)
			enc.WriteBytes(p.Metadata)
		}

	case *SyncGroupRequest:
		encodeRequestHeader(enc, &r.Header)
		enc.WriteString(r.GroupID)
		enc.WriteInt32(r.GenerationID)
		enc.WriteString(r.MemberID)
		enc.WriteInt32(int32(len(r.Assignments)))
		for _, a := range r.Assignments {
			enc.WriteString(a.MemberID)
			enc.WriteBytes(a.Assignment)
		}

	case *HeartbeatRequest:
		encodeRequestHeader(enc, &r.Header)
		enc.WriteString(r.GroupID)
		enc.WriteInt32(r.GenerationID)
		enc.WriteString(r.MemberID)

	case *LeaveGroupRequest:
		encodeRequestHeader(enc, &r.Header)
		enc.WriteString(r.GroupID)
		enc.WriteString(r.MemberID)

	case *OffsetCommitRequest:
		encodeRequestHeader(enc, &r.Header)
		enc.WriteString(r.GroupID)
		enc.WriteInt32(r.GenerationID)
		enc.WriteString(r.MemberID)
		if r.Header.APIVersion >= 2 {
			enc.WriteInt64(r.RetentionMs)
		}
		enc.WriteInt32(int32(len(r.Topics)))
		for _, t := range r.Topics {
			enc.WriteString(t.Topic)
			enc.WriteInt32(int32(len(t.Partitions)))
			for _, p := range t.Partitions {
				enc.WriteInt32(p.Partition)
				enc.WriteInt64(p.Offset)
				enc.WriteNullableString(p.Metadata)
			}
		}

	case *OffsetFetchRequest:
		encodeRequestHeader(enc, &r.Header)
		enc.WriteString(r.GroupID)
		enc.WriteInt32(int32(len(r.Topics)))
		for _, t := range r.Topics {
			enc.WriteString(t.Topic)
			enc.WriteInt32(int32(len(t.Partitions)))
			for _, p := range t.Partitions {
				enc.WriteInt32(p)
			}
		}

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

	// For ApiVersions v2+ (flexible versions), skip tagged fields in the header
	if header.APIKey == APIKeyApiVersions && header.APIVersion >= 2 {
		dec.ReadTaggedFields()
	}

	if dec.Err() != nil {
		return nil, fmt.Errorf("decode header: %w", dec.Err())
	}

	switch header.APIKey {
	case APIKeyProduce:
		req := &ProduceRequest{Header: header}
		if header.APIVersion >= 3 {
			req.TransactionalID = dec.ReadNullableString()
		}
		req.Acks = dec.ReadInt16()
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
		req.ReplicaID = dec.ReadInt32()
		req.MaxWaitMs = dec.ReadInt32()
		req.MinBytes = dec.ReadInt32()
		if header.APIVersion >= 3 {
			req.MaxBytes = dec.ReadInt32()
		}
		if header.APIVersion >= 4 {
			req.IsolationLevel = dec.ReadInt8()
		}
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
		if topicCount >= 0 {
			req.Topics = make([]string, topicCount)
			for i := range req.Topics {
				req.Topics[i] = dec.ReadString()
			}
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
			req.Topics[i].ReplicationFactor = dec.ReadInt16()
			// Read and skip assignments
			numAssignments := dec.ReadInt32()
			for a := int32(0); a < numAssignments && dec.Err() == nil; a++ {
				_ = dec.ReadInt32() // partition
				numReplicas := dec.ReadInt32()
				for r := int32(0); r < numReplicas && dec.Err() == nil; r++ {
					_ = dec.ReadInt32() // broker
				}
			}
			// Read and skip configs
			numConfigs := dec.ReadInt32()
			for c := int32(0); c < numConfigs && dec.Err() == nil; c++ {
				_ = dec.ReadString()         // key
				_ = dec.ReadNullableString() // value
			}
		}
		req.TimeoutMs = dec.ReadInt32()
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return req, nil

	case APIKeyApiVersions:
		req := &ApiVersionsRequest{Header: header}
		// v0-v2: empty body (v2 tagged fields already read above)
		return req, nil

	case APIKeyListOffsets:
		req := &ListOffsetsRequest{Header: header}
		req.ReplicaID = dec.ReadInt32()
		topicCount := dec.ReadInt32()
		req.Topics = make([]ListOffsetsTopic, topicCount)
		for i := range req.Topics {
			req.Topics[i].Topic = dec.ReadString()
			partCount := dec.ReadInt32()
			req.Topics[i].Partitions = make([]ListOffsetsPartition, partCount)
			for j := range req.Topics[i].Partitions {
				req.Topics[i].Partitions[j].Partition = dec.ReadInt32()
				req.Topics[i].Partitions[j].Timestamp = dec.ReadInt64()
				if header.APIVersion == 0 {
					_ = dec.ReadInt32() // maxNumOffsets (v0 only)
				}
			}
		}
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return req, nil

	case APIKeyFindCoordinator:
		req := &FindCoordinatorRequest{Header: header}
		req.Key = dec.ReadString()
		if header.APIVersion >= 1 {
			req.KeyType = dec.ReadInt8()
		}
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return req, nil

	case APIKeyJoinGroup:
		req := &JoinGroupRequest{Header: header}
		req.GroupID = dec.ReadString()
		req.SessionTimeoutMs = dec.ReadInt32()
		if header.APIVersion >= 1 {
			req.RebalanceTimeoutMs = dec.ReadInt32()
		} else {
			req.RebalanceTimeoutMs = req.SessionTimeoutMs
		}
		req.MemberID = dec.ReadString()
		req.ProtocolType = dec.ReadString()
		protocolCount := dec.ReadInt32()
		req.Protocols = make([]JoinGroupProtocol, protocolCount)
		for i := range req.Protocols {
			req.Protocols[i].Name = dec.ReadString()
			req.Protocols[i].Metadata = dec.ReadBytes()
		}
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return req, nil

	case APIKeySyncGroup:
		req := &SyncGroupRequest{Header: header}
		req.GroupID = dec.ReadString()
		req.GenerationID = dec.ReadInt32()
		req.MemberID = dec.ReadString()
		assignmentCount := dec.ReadInt32()
		req.Assignments = make([]SyncGroupAssignment, assignmentCount)
		for i := range req.Assignments {
			req.Assignments[i].MemberID = dec.ReadString()
			req.Assignments[i].Assignment = dec.ReadBytes()
		}
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return req, nil

	case APIKeyHeartbeat:
		req := &HeartbeatRequest{Header: header}
		req.GroupID = dec.ReadString()
		req.GenerationID = dec.ReadInt32()
		req.MemberID = dec.ReadString()
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return req, nil

	case APIKeyLeaveGroup:
		req := &LeaveGroupRequest{Header: header}
		req.GroupID = dec.ReadString()
		req.MemberID = dec.ReadString()
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return req, nil

	case APIKeyOffsetCommit:
		req := &OffsetCommitRequest{Header: header}
		req.GroupID = dec.ReadString()
		req.GenerationID = dec.ReadInt32()
		req.MemberID = dec.ReadString()
		if header.APIVersion >= 2 {
			req.RetentionMs = dec.ReadInt64()
		}
		topicCount := dec.ReadInt32()
		req.Topics = make([]OffsetCommitTopic, topicCount)
		for i := range req.Topics {
			req.Topics[i].Topic = dec.ReadString()
			partCount := dec.ReadInt32()
			req.Topics[i].Partitions = make([]OffsetCommitPartition, partCount)
			for j := range req.Topics[i].Partitions {
				req.Topics[i].Partitions[j].Partition = dec.ReadInt32()
				req.Topics[i].Partitions[j].Offset = dec.ReadInt64()
				req.Topics[i].Partitions[j].Metadata = dec.ReadNullableString()
			}
		}
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return req, nil

	case APIKeyOffsetFetch:
		req := &OffsetFetchRequest{Header: header}
		req.GroupID = dec.ReadString()
		topicCount := dec.ReadInt32()
		req.Topics = make([]OffsetFetchTopic, topicCount)
		for i := range req.Topics {
			req.Topics[i].Topic = dec.ReadString()
			partCount := dec.ReadInt32()
			req.Topics[i].Partitions = make([]int32, partCount)
			for j := range req.Topics[i].Partitions {
				req.Topics[i].Partitions[j] = dec.ReadInt32()
			}
		}
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
				if r.Version >= 2 {
					enc.WriteInt64(p.LogAppendTimeMs)
				}
			}
		}
		if r.Version >= 1 {
			enc.WriteInt32(0) // throttleTimeMs
		}

	case *FetchResponse:
		enc.WriteInt32(r.Header.CorrelationID)
		if r.Version >= 1 {
			enc.WriteInt32(0) // throttleTimeMs
		}
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
				// Replicas
				enc.WriteInt32(int32(len(p.Replicas)))
				for _, rep := range p.Replicas {
					enc.WriteInt32(rep)
				}
				// ISR
				enc.WriteInt32(int32(len(p.ISR)))
				for _, isr := range p.ISR {
					enc.WriteInt32(isr)
				}
			}
		}

	case *CreateTopicsResponse:
		enc.WriteInt32(r.Header.CorrelationID)
		enc.WriteInt32(int32(len(r.Topics)))
		for _, t := range r.Topics {
			enc.WriteString(t.Topic)
			enc.WriteInt16(t.ErrorCode)
		}

	case *ApiVersionsResponse:
		enc.WriteInt32(r.Header.CorrelationID)
		enc.WriteInt16(r.ErrorCode)
		enc.WriteInt32(int32(len(r.ApiVersions)))
		for _, av := range r.ApiVersions {
			enc.WriteInt16(av.APIKey)
			enc.WriteInt16(av.MinVersion)
			enc.WriteInt16(av.MaxVersion)
		}
		if r.Version >= 1 {
			enc.WriteInt32(0) // throttleTimeMs
		}

	case *ListOffsetsResponse:
		enc.WriteInt32(r.Header.CorrelationID)
		enc.WriteInt32(int32(len(r.Topics)))
		for _, t := range r.Topics {
			enc.WriteString(t.Topic)
			enc.WriteInt32(int32(len(t.Partitions)))
			for _, p := range t.Partitions {
				enc.WriteInt32(p.Partition)
				enc.WriteInt16(p.ErrorCode)
				enc.WriteInt64(p.Timestamp)
				enc.WriteInt64(p.Offset)
			}
		}

	case *FindCoordinatorResponse:
		enc.WriteInt32(r.Header.CorrelationID)
		enc.WriteInt16(r.ErrorCode)
		enc.WriteInt32(r.NodeID)
		enc.WriteString(r.Host)
		enc.WriteInt32(r.Port)

	case *JoinGroupResponse:
		enc.WriteInt32(r.Header.CorrelationID)
		enc.WriteInt16(r.ErrorCode)
		enc.WriteInt32(r.GenerationID)
		enc.WriteString(r.ProtocolName)
		enc.WriteString(r.Leader)
		enc.WriteString(r.MemberID)
		enc.WriteInt32(int32(len(r.Members)))
		for _, m := range r.Members {
			enc.WriteString(m.MemberID)
			enc.WriteBytes(m.Metadata)
		}

	case *SyncGroupResponse:
		enc.WriteInt32(r.Header.CorrelationID)
		enc.WriteInt16(r.ErrorCode)
		enc.WriteBytes(r.Assignment)

	case *HeartbeatResponse:
		enc.WriteInt32(r.Header.CorrelationID)
		enc.WriteInt16(r.ErrorCode)

	case *LeaveGroupResponse:
		enc.WriteInt32(r.Header.CorrelationID)
		enc.WriteInt16(r.ErrorCode)

	case *OffsetCommitResponse:
		enc.WriteInt32(r.Header.CorrelationID)
		enc.WriteInt32(int32(len(r.Topics)))
		for _, t := range r.Topics {
			enc.WriteString(t.Topic)
			enc.WriteInt32(int32(len(t.Partitions)))
			for _, p := range t.Partitions {
				enc.WriteInt32(p.Partition)
				enc.WriteInt16(p.ErrorCode)
			}
		}

	case *OffsetFetchResponse:
		enc.WriteInt32(r.Header.CorrelationID)
		enc.WriteInt32(int32(len(r.Topics)))
		for _, t := range r.Topics {
			enc.WriteString(t.Topic)
			enc.WriteInt32(int32(len(t.Partitions)))
			for _, p := range t.Partitions {
				enc.WriteInt32(p.Partition)
				enc.WriteInt64(p.CommittedOffset)
				enc.WriteNullableString(p.Metadata)
				enc.WriteInt16(p.ErrorCode)
			}
		}

	default:
		return nil, fmt.Errorf("unknown response type: %T", resp)
	}

	if enc.Err() != nil {
		return nil, enc.Err()
	}
	return buf.Bytes(), nil
}

// DecodeResponse decodes a response from bytes given the API key and version.
func DecodeResponse(apiKey int16, apiVersion int16, data []byte) (interface{}, error) {
	dec := NewDecoder(bytes.NewReader(data))

	correlationID := dec.ReadInt32()

	switch apiKey {
	case APIKeyProduce:
		resp := &ProduceResponse{Header: ResponseHeader{CorrelationID: correlationID}, Version: apiVersion}
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
				if apiVersion >= 2 {
					resp.Topics[i].Partitions[j].LogAppendTimeMs = dec.ReadInt64()
				}
			}
		}
		if apiVersion >= 1 {
			_ = dec.ReadInt32() // throttleTimeMs
		}
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return resp, nil

	case APIKeyFetch:
		resp := &FetchResponse{Header: ResponseHeader{CorrelationID: correlationID}, Version: apiVersion}
		if apiVersion >= 1 {
			_ = dec.ReadInt32() // throttleTimeMs
		}
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
		resp := &MetadataResponse{Header: ResponseHeader{CorrelationID: correlationID}, Version: apiVersion}
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
				// Replicas
				repCount := dec.ReadInt32()
				resp.Topics[i].Partitions[j].Replicas = make([]int32, repCount)
				for k := int32(0); k < repCount; k++ {
					resp.Topics[i].Partitions[j].Replicas[k] = dec.ReadInt32()
				}
				// ISR
				isrCount := dec.ReadInt32()
				resp.Topics[i].Partitions[j].ISR = make([]int32, isrCount)
				for k := int32(0); k < isrCount; k++ {
					resp.Topics[i].Partitions[j].ISR[k] = dec.ReadInt32()
				}
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

	case APIKeyApiVersions:
		resp := &ApiVersionsResponse{Header: ResponseHeader{CorrelationID: correlationID}, Version: apiVersion}
		resp.ErrorCode = dec.ReadInt16()
		apiCount := dec.ReadInt32()
		resp.ApiVersions = make([]ApiVersionRange, apiCount)
		for i := range resp.ApiVersions {
			resp.ApiVersions[i].APIKey = dec.ReadInt16()
			resp.ApiVersions[i].MinVersion = dec.ReadInt16()
			resp.ApiVersions[i].MaxVersion = dec.ReadInt16()
		}
		if apiVersion >= 1 {
			_ = dec.ReadInt32() // throttleTimeMs
		}
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return resp, nil

	case APIKeyListOffsets:
		resp := &ListOffsetsResponse{Header: ResponseHeader{CorrelationID: correlationID}}
		topicCount := dec.ReadInt32()
		resp.Topics = make([]ListOffsetsTopicResponse, topicCount)
		for i := range resp.Topics {
			resp.Topics[i].Topic = dec.ReadString()
			partCount := dec.ReadInt32()
			resp.Topics[i].Partitions = make([]ListOffsetsPartitionResponse, partCount)
			for j := range resp.Topics[i].Partitions {
				resp.Topics[i].Partitions[j].Partition = dec.ReadInt32()
				resp.Topics[i].Partitions[j].ErrorCode = dec.ReadInt16()
				resp.Topics[i].Partitions[j].Timestamp = dec.ReadInt64()
				resp.Topics[i].Partitions[j].Offset = dec.ReadInt64()
			}
		}
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return resp, nil

	case APIKeyFindCoordinator:
		resp := &FindCoordinatorResponse{Header: ResponseHeader{CorrelationID: correlationID}}
		resp.ErrorCode = dec.ReadInt16()
		resp.NodeID = dec.ReadInt32()
		resp.Host = dec.ReadString()
		resp.Port = dec.ReadInt32()
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return resp, nil

	case APIKeyJoinGroup:
		resp := &JoinGroupResponse{Header: ResponseHeader{CorrelationID: correlationID}}
		resp.ErrorCode = dec.ReadInt16()
		resp.GenerationID = dec.ReadInt32()
		resp.ProtocolName = dec.ReadString()
		resp.Leader = dec.ReadString()
		resp.MemberID = dec.ReadString()
		memberCount := dec.ReadInt32()
		resp.Members = make([]JoinGroupMember, memberCount)
		for i := range resp.Members {
			resp.Members[i].MemberID = dec.ReadString()
			resp.Members[i].Metadata = dec.ReadBytes()
		}
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return resp, nil

	case APIKeySyncGroup:
		resp := &SyncGroupResponse{Header: ResponseHeader{CorrelationID: correlationID}}
		resp.ErrorCode = dec.ReadInt16()
		resp.Assignment = dec.ReadBytes()
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return resp, nil

	case APIKeyHeartbeat:
		resp := &HeartbeatResponse{Header: ResponseHeader{CorrelationID: correlationID}}
		resp.ErrorCode = dec.ReadInt16()
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return resp, nil

	case APIKeyLeaveGroup:
		resp := &LeaveGroupResponse{Header: ResponseHeader{CorrelationID: correlationID}}
		resp.ErrorCode = dec.ReadInt16()
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return resp, nil

	case APIKeyOffsetCommit:
		resp := &OffsetCommitResponse{Header: ResponseHeader{CorrelationID: correlationID}}
		topicCount := dec.ReadInt32()
		resp.Topics = make([]OffsetCommitTopicResponse, topicCount)
		for i := range resp.Topics {
			resp.Topics[i].Topic = dec.ReadString()
			partCount := dec.ReadInt32()
			resp.Topics[i].Partitions = make([]OffsetCommitPartitionResponse, partCount)
			for j := range resp.Topics[i].Partitions {
				resp.Topics[i].Partitions[j].Partition = dec.ReadInt32()
				resp.Topics[i].Partitions[j].ErrorCode = dec.ReadInt16()
			}
		}
		if dec.Err() != nil {
			return nil, dec.Err()
		}
		return resp, nil

	case APIKeyOffsetFetch:
		resp := &OffsetFetchResponse{Header: ResponseHeader{CorrelationID: correlationID}}
		topicCount := dec.ReadInt32()
		resp.Topics = make([]OffsetFetchTopicResponse, topicCount)
		for i := range resp.Topics {
			resp.Topics[i].Topic = dec.ReadString()
			partCount := dec.ReadInt32()
			resp.Topics[i].Partitions = make([]OffsetFetchPartitionResponse, partCount)
			for j := range resp.Topics[i].Partitions {
				resp.Topics[i].Partitions[j].Partition = dec.ReadInt32()
				resp.Topics[i].Partitions[j].CommittedOffset = dec.ReadInt64()
				resp.Topics[i].Partitions[j].Metadata = dec.ReadNullableString()
				resp.Topics[i].Partitions[j].ErrorCode = dec.ReadInt16()
			}
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
