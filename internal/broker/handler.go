package broker

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/harshithgowda/streamq/internal/log"
	"github.com/harshithgowda/streamq/internal/protocol"
)

// HandleProduce processes a ProduceRequest.
// Accepts Kafka RecordBatch wire format (magic=2) and translates to internal format.
func (b *Broker) HandleProduce(req *protocol.ProduceRequest) *protocol.ProduceResponse {
	resp := &protocol.ProduceResponse{
		Header:  protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
		Version: req.Header.APIVersion,
	}

	for _, topicData := range req.Topics {
		topicResp := protocol.ProduceTopicResponse{Topic: topicData.Topic}

		for _, partData := range topicData.Partitions {
			partResp := protocol.ProducePartitionResponse{Partition: partData.Partition}

			// Auto-create topic if enabled
			partition, err := b.TopicManager.GetPartition(topicData.Topic, partData.Partition)
			if err != nil && b.Config.AutoCreateTopics {
				createErr := b.TopicManager.CreateTopic(topicData.Topic, b.Config.DefaultPartitions)
				if createErr == nil || strings.Contains(createErr.Error(), "already exists") {
					partition, err = b.TopicManager.GetPartition(topicData.Topic, partData.Partition)
				}
			}
			if err != nil {
				partResp.ErrorCode = protocol.ErrUnknownTopicOrPart
				topicResp.Partitions = append(topicResp.Partitions, partResp)
				continue
			}

			// Try Kafka RecordBatch format first, fall back to internal format
			var firstOffset int64 = -1
			batches, kafkaErr := protocol.KafkaRecordBatchToInternal(partData.Records)
			if kafkaErr == nil {
				// Kafka wire format
				for _, batch := range batches {
					baseOffset, appErr := partition.Log.Append(batch)
					if appErr != nil {
						partResp.ErrorCode = protocol.ErrUnknown
						break
					}
					if firstOffset < 0 {
						firstOffset = baseOffset
					}
				}
			} else {
				// Fallback: try internal format (for legacy custom clients)
				data := partData.Records
				for len(data) >= 12 {
					batchLen := int32(data[8])<<24 | int32(data[9])<<16 | int32(data[10])<<8 | int32(data[11])
					totalSize := 12 + int(batchLen)
					if totalSize > len(data) || batchLen < 0 {
						break
					}

					batch, decErr := log.DecodeRecordBatch(data[:totalSize])
					if decErr != nil {
						partResp.ErrorCode = protocol.ErrInvalidMessage
						break
					}

					baseOffset, appErr := partition.Log.Append(batch)
					if appErr != nil {
						partResp.ErrorCode = protocol.ErrUnknown
						break
					}

					if firstOffset < 0 {
						firstOffset = baseOffset
					}
					data = data[totalSize:]
				}
			}

			if partResp.ErrorCode == 0 {
				partResp.ErrorCode = protocol.ErrNone
				partResp.BaseOffset = firstOffset
				if firstOffset < 0 {
					partResp.BaseOffset = 0
				}
				if req.Header.APIVersion >= 2 {
					partResp.LogAppendTimeMs = time.Now().UnixMilli()
				}
			}
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp
}

// HandleFetch processes a FetchRequest.
// Returns Kafka RecordBatch wire format (magic=2).
func (b *Broker) HandleFetch(req *protocol.FetchRequest) *protocol.FetchResponse {
	return b.doFetch(req)
}

func (b *Broker) doFetch(req *protocol.FetchRequest) *protocol.FetchResponse {
	resp := &protocol.FetchResponse{
		Header:  protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
		Version: req.Header.APIVersion,
	}

	for _, topicData := range req.Topics {
		topicResp := protocol.FetchTopicResponse{Topic: topicData.Topic}

		for _, partData := range topicData.Partitions {
			partResp := protocol.FetchPartitionResponse{Partition: partData.Partition}

			partition, err := b.TopicManager.GetPartition(topicData.Topic, partData.Partition)
			if err != nil {
				partResp.ErrorCode = protocol.ErrUnknownTopicOrPart
				topicResp.Partitions = append(topicResp.Partitions, partResp)
				continue
			}

			maxBytes := int(partData.MaxBytes)
			if maxBytes <= 0 {
				maxBytes = 1024 * 1024
			}

			batches, err := partition.Log.Read(partData.FetchOffset, maxBytes)
			if err != nil {
				if errors.Is(err, log.ErrOffsetOutOfRange) {
					partResp.ErrorCode = protocol.ErrOffsetOutOfRange
				} else if errors.Is(err, log.ErrCorruptRecord) {
					partResp.ErrorCode = protocol.ErrCorruptMessage
				} else {
					partResp.ErrorCode = protocol.ErrUnknown
				}
				topicResp.Partitions = append(topicResp.Partitions, partResp)
				continue
			}

			// Encode all batches into Kafka RecordBatch wire format
			var raw []byte
			for _, batch := range batches {
				raw = append(raw, protocol.InternalToKafkaRecordBatch(batch)...)
			}

			partResp.ErrorCode = protocol.ErrNone
			partResp.HighWatermark = partition.Log.NextOffset()
			partResp.RecordBatches = raw
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp
}

// totalResponseBytes returns the total size of record batches in a FetchResponse.
func totalResponseBytes(resp *protocol.FetchResponse) int {
	total := 0
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			total += len(p.RecordBatches)
		}
	}
	return total
}

// HandleMetadata processes a MetadataRequest.
func (b *Broker) HandleMetadata(req *protocol.MetadataRequest) *protocol.MetadataResponse {
	resp := &protocol.MetadataResponse{
		Header:  protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
		Version: req.Header.APIVersion,
	}

	// Parse broker address for metadata
	host, portStr := parseAddr(b.Config.Addr)
	port, _ := strconv.Atoi(portStr)
	resp.Brokers = []protocol.BrokerInfo{
		{NodeID: 0, Host: host, Port: int32(port)},
	}

	// If no topics specified, return all
	topicNames := req.Topics
	if len(topicNames) == 0 {
		topicNames = b.TopicManager.ListTopics()
	}

	for _, name := range topicNames {
		topic := b.TopicManager.GetTopic(name)
		if topic == nil {
			resp.Topics = append(resp.Topics, protocol.TopicMetadata{
				ErrorCode: protocol.ErrUnknownTopicOrPart,
				Topic:     name,
			})
			continue
		}

		tm := protocol.TopicMetadata{
			ErrorCode: protocol.ErrNone,
			Topic:     name,
		}
		for _, p := range topic.Partitions {
			tm.Partitions = append(tm.Partitions, protocol.PartitionMetadata{
				ErrorCode: protocol.ErrNone,
				Partition: p.ID,
				Leader:    0,
				Replicas:  []int32{0},
				ISR:       []int32{0},
			})
		}
		resp.Topics = append(resp.Topics, tm)
	}

	return resp
}

// HandleCreateTopics processes a CreateTopicsRequest.
func (b *Broker) HandleCreateTopics(req *protocol.CreateTopicsRequest) *protocol.CreateTopicsResponse {
	resp := &protocol.CreateTopicsResponse{
		Header: protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
	}

	for _, topicReq := range req.Topics {
		result := protocol.CreateTopicResult{Topic: topicReq.Topic}

		numPartitions := topicReq.NumPartitions
		if numPartitions <= 0 {
			numPartitions = b.Config.DefaultPartitions
		}

		err := b.TopicManager.CreateTopic(topicReq.Topic, numPartitions)
		if err != nil {
			if strings.Contains(err.Error(), "already exists") {
				result.ErrorCode = protocol.ErrTopicAlreadyExists
			} else {
				result.ErrorCode = protocol.ErrUnknown
			}
		} else {
			result.ErrorCode = protocol.ErrNone
		}

		resp.Topics = append(resp.Topics, result)
	}

	return resp
}

// HandleApiVersions returns supported API version ranges.
func (b *Broker) HandleApiVersions(req *protocol.ApiVersionsRequest) *protocol.ApiVersionsResponse {
	return &protocol.ApiVersionsResponse{
		Header:  protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
		Version: req.Header.APIVersion,
		ErrorCode: protocol.ErrNone,
		ApiVersions: []protocol.ApiVersionRange{
			{APIKey: protocol.APIKeyProduce, MinVersion: 0, MaxVersion: 3},
			{APIKey: protocol.APIKeyFetch, MinVersion: 0, MaxVersion: 4},
			{APIKey: protocol.APIKeyListOffsets, MinVersion: 0, MaxVersion: 1},
			{APIKey: protocol.APIKeyMetadata, MinVersion: 0, MaxVersion: 1},
			{APIKey: protocol.APIKeyOffsetFetch, MinVersion: 0, MaxVersion: 0},
			{APIKey: protocol.APIKeyFindCoordinator, MinVersion: 0, MaxVersion: 0},
			{APIKey: protocol.APIKeyJoinGroup, MinVersion: 0, MaxVersion: 0},
			{APIKey: protocol.APIKeyApiVersions, MinVersion: 0, MaxVersion: 2},
			{APIKey: protocol.APIKeyCreateTopics, MinVersion: 0, MaxVersion: 0},
		},
	}
}

// HandleListOffsets returns earliest/latest offsets for partitions.
func (b *Broker) HandleListOffsets(req *protocol.ListOffsetsRequest) *protocol.ListOffsetsResponse {
	resp := &protocol.ListOffsetsResponse{
		Header: protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
	}

	for _, topicData := range req.Topics {
		topicResp := protocol.ListOffsetsTopicResponse{Topic: topicData.Topic}

		for _, partData := range topicData.Partitions {
			partResp := protocol.ListOffsetsPartitionResponse{
				Partition: partData.Partition,
			}

			partition, err := b.TopicManager.GetPartition(topicData.Topic, partData.Partition)
			if err != nil {
				partResp.ErrorCode = protocol.ErrUnknownTopicOrPart
				topicResp.Partitions = append(topicResp.Partitions, partResp)
				continue
			}

			partResp.ErrorCode = protocol.ErrNone
			switch partData.Timestamp {
			case -1: // latest
				partResp.Offset = partition.Log.NextOffset()
				partResp.Timestamp = -1
			case -2: // earliest
				partResp.Offset = partition.Log.EarliestOffset()
				partResp.Timestamp = -2
			default:
				// For any other timestamp, return the latest offset
				partResp.Offset = partition.Log.NextOffset()
				partResp.Timestamp = partData.Timestamp
			}

			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp
}

// HandleFindCoordinator returns self as coordinator.
func (b *Broker) HandleFindCoordinator(req *protocol.FindCoordinatorRequest) *protocol.FindCoordinatorResponse {
	host, portStr := parseAddr(b.Config.Addr)
	port, _ := strconv.Atoi(portStr)
	return &protocol.FindCoordinatorResponse{
		Header:    protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
		ErrorCode: protocol.ErrNone,
		NodeID:    0,
		Host:      host,
		Port:      int32(port),
	}
}

// HandleJoinGroup returns an error since we don't support consumer groups.
func (b *Broker) HandleJoinGroup(req *protocol.JoinGroupRequest) *protocol.JoinGroupResponse {
	return &protocol.JoinGroupResponse{
		Header:    protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
		ErrorCode: protocol.ErrGroupCoordinatorNotAvailable,
	}
}

// HandleOffsetFetch returns committed offset -1 for all partitions (no committed offsets).
func (b *Broker) HandleOffsetFetch(req *protocol.OffsetFetchRequest) *protocol.OffsetFetchResponse {
	resp := &protocol.OffsetFetchResponse{
		Header: protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
	}

	for _, t := range req.Topics {
		topicResp := protocol.OffsetFetchTopicResponse{Topic: t.Topic}
		for _, p := range t.Partitions {
			topicResp.Partitions = append(topicResp.Partitions, protocol.OffsetFetchPartitionResponse{
				Partition:       p,
				CommittedOffset: -1,
				ErrorCode:       protocol.ErrNone,
			})
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp
}

// Dispatch routes a decoded request to the appropriate handler.
func (b *Broker) Dispatch(req interface{}) (interface{}, error) {
	switch r := req.(type) {
	case *protocol.ProduceRequest:
		return b.HandleProduce(r), nil
	case *protocol.FetchRequest:
		return b.HandleFetch(r), nil
	case *protocol.MetadataRequest:
		return b.HandleMetadata(r), nil
	case *protocol.CreateTopicsRequest:
		return b.HandleCreateTopics(r), nil
	case *protocol.ApiVersionsRequest:
		return b.HandleApiVersions(r), nil
	case *protocol.ListOffsetsRequest:
		return b.HandleListOffsets(r), nil
	case *protocol.FindCoordinatorRequest:
		return b.HandleFindCoordinator(r), nil
	case *protocol.JoinGroupRequest:
		return b.HandleJoinGroup(r), nil
	case *protocol.OffsetFetchRequest:
		return b.HandleOffsetFetch(r), nil
	default:
		return nil, fmt.Errorf("unknown request type: %T", req)
	}
}

func parseAddr(addr string) (host, port string) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "localhost", "9092"
	}
	if host == "" {
		host = "localhost"
	}
	return host, port
}
