package broker

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/harshithgowda/streamq/internal/cluster"
	"github.com/harshithgowda/streamq/internal/log"
	"github.com/harshithgowda/streamq/internal/protocol"
)

// HandleProduce processes a ProduceRequest.
func (b *Broker) HandleProduce(req *protocol.ProduceRequest) *protocol.ProduceResponse {
	resp := &protocol.ProduceResponse{
		Header:  protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
		Version: req.Header.APIVersion,
	}

	for _, topicData := range req.Topics {
		topicResp := protocol.ProduceTopicResponse{Topic: topicData.Topic}

		for _, partData := range topicData.Partitions {
			partResp := protocol.ProducePartitionResponse{Partition: partData.Partition}

			// In cluster mode, reject if not leader
			if b.clusterMode {
				state := b.GetPartitionState(topicData.Topic, partData.Partition)
				if state == nil || state.GetRole() != RoleLeader {
					partResp.ErrorCode = protocol.ErrNotLeaderForPartition
					topicResp.Partitions = append(topicResp.Partitions, partResp)
					continue
				}
				// Epoch check
				if state.GetEpoch() < 0 {
					partResp.ErrorCode = protocol.ErrNotLeaderForPartition
					topicResp.Partitions = append(topicResp.Partitions, partResp)
					continue
				}
			}

			// Auto-create topic if enabled
			partition, err := b.TopicManager.GetPartition(topicData.Topic, partData.Partition)
			if err != nil && b.Config.AutoCreateTopics {
				if b.clusterMode && b.controllerClient != nil {
					_ = b.controllerClient.CreateTopic(topicData.Topic, b.Config.DefaultPartitions, 3)
					time.Sleep(100 * time.Millisecond) // wait for metadata propagation
					partition, err = b.TopicManager.GetPartition(topicData.Topic, partData.Partition)
				} else {
					createErr := b.TopicManager.CreateTopic(topicData.Topic, b.Config.DefaultPartitions)
					if createErr == nil || strings.Contains(createErr.Error(), "already exists") {
						partition, err = b.TopicManager.GetPartition(topicData.Topic, partData.Partition)
					}
				}
			}
			if err != nil {
				partResp.ErrorCode = protocol.ErrUnknownTopicOrPart
				topicResp.Partitions = append(topicResp.Partitions, partResp)
				continue
			}

			// Try Kafka RecordBatch format first, fall back to internal format
			var firstOffset int64 = -1
			var recordCount int
			batches, kafkaErr := protocol.KafkaRecordBatchToInternal(partData.Records)
			if kafkaErr == nil {
				for _, batch := range batches {
					baseOffset, appErr := partition.Log.Append(batch)
					if appErr != nil {
						partResp.ErrorCode = protocol.ErrUnknown
						break
					}
					if firstOffset < 0 {
						firstOffset = baseOffset
					}
					recordCount += len(batch.Records)
				}
			} else {
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
					recordCount += len(batch.Records)
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

				// For acks=-1, wait until HWM advances past our records
				if b.clusterMode && req.Acks == -1 && recordCount > 0 {
					state := b.GetPartitionState(topicData.Topic, partData.Partition)
					if state != nil {
						targetOffset := firstOffset + int64(recordCount)
						ok := state.WaitForHWM(targetOffset, time.Duration(req.TimeoutMs)*time.Millisecond)
						if !ok {
							// Check min ISR
							state.mu.RLock()
							isrLen := len(state.ISR)
							state.mu.RUnlock()
							if isrLen < 2 { // min.isr = 2
								partResp.ErrorCode = protocol.ErrNotEnoughReplicas
							} else {
								partResp.ErrorCode = protocol.ErrRequestTimedOut
							}
						}
					}
				}
			}
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp
}

// HandleFetch processes a FetchRequest.
func (b *Broker) HandleFetch(req *protocol.FetchRequest) *protocol.FetchResponse {
	return b.doFetch(req)
}

func (b *Broker) doFetch(req *protocol.FetchRequest) *protocol.FetchResponse {
	resp := &protocol.FetchResponse{
		Header:  protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
		Version: req.Header.APIVersion,
	}

	isReplicaFetch := req.ReplicaID >= 0

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

			var raw []byte
			for _, batch := range batches {
				raw = append(raw, protocol.InternalToKafkaRecordBatch(batch)...)
			}

			partResp.ErrorCode = protocol.ErrNone

			if b.clusterMode {
				state := b.GetPartitionState(topicData.Topic, partData.Partition)
				if state != nil {
					if isReplicaFetch {
						// Replica fetch: return data up to LEO, update replica tracking
						partResp.HighWatermark = partition.Log.NextOffset()

						followerID := cluster.BrokerID(req.ReplicaID)
						fetchEnd := partData.FetchOffset
						if len(batches) > 0 {
							last := batches[len(batches)-1]
							fetchEnd = last.BaseOffset + int64(len(last.Records))
						}
						state.UpdateReplicaLEO(followerID, fetchEnd)
						state.UpdateHWM(partition.Log.NextOffset())
					} else {
						// Consumer fetch: return data up to HWM
						hwm := state.GetHWM()
						partResp.HighWatermark = hwm
						// Filter batches to only include up to HWM
						raw = nil
						for _, batch := range batches {
							if batch.BaseOffset < hwm {
								raw = append(raw, protocol.InternalToKafkaRecordBatch(batch)...)
							}
						}
					}
				} else {
					partResp.HighWatermark = partition.Log.NextOffset()
				}
			} else {
				partResp.HighWatermark = partition.Log.NextOffset()
			}

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

	if b.clusterMode {
		meta := b.GetClusterMetadata()
		if meta != nil {
			// Build broker list from cluster metadata
			for _, bi := range meta.Brokers {
				resp.Brokers = append(resp.Brokers, protocol.BrokerInfo{
					NodeID: int32(bi.ID),
					Host:   bi.Host,
					Port:   bi.Port,
				})
			}

			// Build topic metadata
			topicNames := req.Topics
			if len(topicNames) == 0 {
				for topic := range meta.Assignments {
					topicNames = append(topicNames, topic)
				}
			}

			for _, name := range topicNames {
				parts, ok := meta.Assignments[name]
				if !ok {
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
				for _, pa := range parts {
					replicas := make([]int32, len(pa.Replicas))
					for i, r := range pa.Replicas {
						replicas[i] = int32(r)
					}
					isr := make([]int32, len(pa.ISR))
					for i, r := range pa.ISR {
						isr[i] = int32(r)
					}
					tm.Partitions = append(tm.Partitions, protocol.PartitionMetadata{
						ErrorCode: protocol.ErrNone,
						Partition: pa.Partition,
						Leader:    int32(pa.Leader),
						Replicas:  replicas,
						ISR:       isr,
					})
				}
				resp.Topics = append(resp.Topics, tm)
			}

			return resp
		}
	}

	// Single-node mode
	host, portStr := parseAddr(b.Config.Addr)
	port, _ := strconv.Atoi(portStr)
	resp.Brokers = []protocol.BrokerInfo{
		{NodeID: b.GetNodeID(), Host: host, Port: int32(port)},
	}

	topicNames := req.Topics
	if len(topicNames) == 0 {
		topicNames = b.TopicManager.ListTopics()
	}

	nodeID := b.GetNodeID()
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
				Leader:    nodeID,
				Replicas:  []int32{nodeID},
				ISR:       []int32{nodeID},
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

		if b.clusterMode && b.controllerClient != nil {
			err := b.controllerClient.CreateTopic(topicReq.Topic, numPartitions, topicReq.ReplicationFactor)
			if err != nil {
				if strings.Contains(err.Error(), "already exists") {
					result.ErrorCode = protocol.ErrTopicAlreadyExists
				} else {
					result.ErrorCode = protocol.ErrUnknown
				}
			} else {
				result.ErrorCode = protocol.ErrNone
			}
		} else {
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
			{APIKey: protocol.APIKeyOffsetCommit, MinVersion: 0, MaxVersion: 2},
			{APIKey: protocol.APIKeyOffsetFetch, MinVersion: 0, MaxVersion: 0},
			{APIKey: protocol.APIKeyFindCoordinator, MinVersion: 0, MaxVersion: 0},
			{APIKey: protocol.APIKeyJoinGroup, MinVersion: 0, MaxVersion: 1},
			{APIKey: protocol.APIKeyHeartbeat, MinVersion: 0, MaxVersion: 0},
			{APIKey: protocol.APIKeyLeaveGroup, MinVersion: 0, MaxVersion: 0},
			{APIKey: protocol.APIKeySyncGroup, MinVersion: 0, MaxVersion: 0},
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

	if b.clusterMode {
		meta := b.GetClusterMetadata()
		if meta != nil {
			for _, bi := range meta.Brokers {
				return &protocol.FindCoordinatorResponse{
					Header:    protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
					ErrorCode: protocol.ErrNone,
					NodeID:    int32(bi.ID),
					Host:      bi.Host,
					Port:      bi.Port,
				}
			}
		}
	}

	return &protocol.FindCoordinatorResponse{
		Header:    protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
		ErrorCode: protocol.ErrNone,
		NodeID:    b.GetNodeID(),
		Host:      host,
		Port:      int32(port),
	}
}

// HandleJoinGroup delegates to the group coordinator.
func (b *Broker) HandleJoinGroup(req *protocol.JoinGroupRequest) *protocol.JoinGroupResponse {
	return b.groupCoordinator.HandleJoinGroup(req)
}

// HandleSyncGroup delegates to the group coordinator.
func (b *Broker) HandleSyncGroup(req *protocol.SyncGroupRequest) *protocol.SyncGroupResponse {
	return b.groupCoordinator.HandleSyncGroup(req)
}

// HandleHeartbeat delegates to the group coordinator.
func (b *Broker) HandleHeartbeat(req *protocol.HeartbeatRequest) *protocol.HeartbeatResponse {
	return b.groupCoordinator.HandleHeartbeat(req)
}

// HandleLeaveGroup delegates to the group coordinator.
func (b *Broker) HandleLeaveGroup(req *protocol.LeaveGroupRequest) *protocol.LeaveGroupResponse {
	return b.groupCoordinator.HandleLeaveGroup(req)
}

// HandleOffsetCommit delegates to the group coordinator.
func (b *Broker) HandleOffsetCommit(req *protocol.OffsetCommitRequest) *protocol.OffsetCommitResponse {
	return b.groupCoordinator.HandleOffsetCommit(req)
}

// HandleOffsetFetch delegates to the group coordinator.
func (b *Broker) HandleOffsetFetch(req *protocol.OffsetFetchRequest) *protocol.OffsetFetchResponse {
	return b.groupCoordinator.HandleOffsetFetch(req)
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
	case *protocol.SyncGroupRequest:
		return b.HandleSyncGroup(r), nil
	case *protocol.HeartbeatRequest:
		return b.HandleHeartbeat(r), nil
	case *protocol.LeaveGroupRequest:
		return b.HandleLeaveGroup(r), nil
	case *protocol.OffsetCommitRequest:
		return b.HandleOffsetCommit(r), nil
	case *protocol.OffsetFetchRequest:
		return b.HandleOffsetFetch(r), nil
	default:
		return nil, fmt.Errorf("unknown request type: %T", req)
	}
}
