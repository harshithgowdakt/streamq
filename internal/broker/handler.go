package broker

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/harshithgowda/streamq/internal/log"
	"github.com/harshithgowda/streamq/internal/protocol"
)

// HandleProduce processes a ProduceRequest.
func (b *Broker) HandleProduce(req *protocol.ProduceRequest) *protocol.ProduceResponse {
	resp := &protocol.ProduceResponse{
		Header: protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
	}

	for _, topicData := range req.Topics {
		topicResp := protocol.ProduceTopicResponse{Topic: topicData.Topic}

		for _, partData := range topicData.Partitions {
			partResp := protocol.ProducePartitionResponse{Partition: partData.Partition}

			partition, err := b.TopicManager.GetPartition(topicData.Topic, partData.Partition)
			if err != nil {
				partResp.ErrorCode = protocol.ErrUnknownTopicOrPart
				topicResp.Partitions = append(topicResp.Partitions, partResp)
				continue
			}

			// Decode the record batch
			batch, err := log.DecodeRecordBatch(partData.Records)
			if err != nil {
				partResp.ErrorCode = protocol.ErrInvalidMessage
				topicResp.Partitions = append(topicResp.Partitions, partResp)
				continue
			}

			// Append to the partition's log
			baseOffset, err := partition.Log.Append(batch)
			if err != nil {
				partResp.ErrorCode = protocol.ErrUnknown
				topicResp.Partitions = append(topicResp.Partitions, partResp)
				continue
			}

			partResp.ErrorCode = protocol.ErrNone
			partResp.BaseOffset = baseOffset
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp
}

// HandleFetch processes a FetchRequest.
func (b *Broker) HandleFetch(req *protocol.FetchRequest) *protocol.FetchResponse {
	resp := &protocol.FetchResponse{
		Header: protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
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
				partResp.ErrorCode = protocol.ErrUnknown
				topicResp.Partitions = append(topicResp.Partitions, partResp)
				continue
			}

			// Encode all batches into raw bytes
			var raw []byte
			for _, batch := range batches {
				raw = append(raw, batch.Encode()...)
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

// HandleMetadata processes a MetadataRequest.
func (b *Broker) HandleMetadata(req *protocol.MetadataRequest) *protocol.MetadataResponse {
	resp := &protocol.MetadataResponse{
		Header: protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
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
