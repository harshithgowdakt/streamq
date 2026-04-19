package consumer

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/harshithgowda/streamq/internal/protocol"
)

// GroupConfig extends Config with consumer group settings.
type GroupConfig struct {
	BrokerAddr         string
	ClientID           string
	MaxBytes           int32
	GroupID            string
	SessionTimeoutMs   int32
	RebalanceTimeoutMs int32
	AutoCommitInterval time.Duration
	AssignmentStrategy string // "range" (default) or "roundrobin"
}

// DefaultGroupConfig returns sensible defaults for group consumer.
func DefaultGroupConfig() GroupConfig {
	return GroupConfig{
		ClientID:           "streamq-group-consumer",
		MaxBytes:           1024 * 1024,
		SessionTimeoutMs:   10000,
		RebalanceTimeoutMs: 60000,
		AutoCommitInterval: 5 * time.Second,
		AssignmentStrategy: "range",
	}
}

// GroupConsumer is a consumer that participates in a consumer group.
type GroupConsumer struct {
	config       GroupConfig
	conn         net.Conn
	sendMu       sync.Mutex // serializes wire protocol use
	mu           sync.Mutex // guards member state
	memberID     string
	generationID int32
	assignments  map[string][]int32         // topic -> partitions
	offsets      map[string]map[int32]int64 // topic -> partition -> next fetch offset
	topics       []string
	corrID       int32

	closed      chan struct{}
	closeOnce   sync.Once
	heartbeatWg sync.WaitGroup
	commitWg    sync.WaitGroup
}

// NewGroupConsumer creates a group consumer, connects to broker, and joins the group.
func NewGroupConsumer(config GroupConfig) (*GroupConsumer, error) {
	conn, err := net.DialTimeout("tcp", config.BrokerAddr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	gc := &GroupConsumer{
		config:      config,
		conn:        conn,
		assignments: make(map[string][]int32),
		offsets:     make(map[string]map[int32]int64),
		closed:      make(chan struct{}),
	}

	return gc, nil
}

// CurrentOffsets returns a snapshot of the next-fetch offsets.
func (gc *GroupConsumer) CurrentOffsets() map[string]map[int32]int64 {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	out := make(map[string]map[int32]int64, len(gc.offsets))
	for t, parts := range gc.offsets {
		cp := make(map[int32]int64, len(parts))
		for p, o := range parts {
			cp[p] = o
		}
		out[t] = cp
	}
	return out
}

// Assignments returns a snapshot of the current partition assignment.
func (gc *GroupConsumer) Assignments() map[string][]int32 {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	out := make(map[string][]int32, len(gc.assignments))
	for t, parts := range gc.assignments {
		cp := make([]int32, len(parts))
		copy(cp, parts)
		out[t] = cp
	}
	return out
}

// Subscribe sets the topics and joins the consumer group.
func (gc *GroupConsumer) Subscribe(topics ...string) error {
	gc.mu.Lock()
	gc.topics = topics
	gc.mu.Unlock()

	if err := gc.joinGroup(); err != nil {
		return err
	}

	// Start heartbeat goroutine
	gc.heartbeatWg.Add(1)
	go gc.heartbeatLoop()

	// Start auto-commit goroutine if enabled
	if gc.config.AutoCommitInterval > 0 {
		gc.commitWg.Add(1)
		go gc.autoCommitLoop()
	}
	return nil
}

// joinGroupRetry retries joinGroup up to a few times.
func (gc *GroupConsumer) joinGroupRetry(attempt int) error {
	if attempt > 5 {
		return fmt.Errorf("join group: exceeded max retries")
	}
	return gc.joinGroup()
}

func (gc *GroupConsumer) joinGroup() error {
	gc.mu.Lock()
	topics := gc.topics
	strategy := gc.config.AssignmentStrategy
	if strategy == "" {
		strategy = "range"
	}
	gc.mu.Unlock()

	// Build subscription metadata
	sub := &protocol.ConsumerProtocolSubscription{
		Version: 0,
		Topics:  topics,
	}
	metadata := protocol.EncodeConsumerSubscription(sub)

	// JoinGroup
	joinReq := &protocol.JoinGroupRequest{
		Header: protocol.RequestHeader{
			APIKey:        protocol.APIKeyJoinGroup,
			APIVersion:    1,
			CorrelationID: gc.nextCorrID(),
			ClientID:      gc.config.ClientID,
		},
		GroupID:            gc.config.GroupID,
		SessionTimeoutMs:   gc.config.SessionTimeoutMs,
		RebalanceTimeoutMs: gc.config.RebalanceTimeoutMs,
		MemberID:           gc.memberID,
		ProtocolType:       "consumer",
		Protocols: []protocol.JoinGroupProtocol{
			{Name: strategy, Metadata: metadata},
		},
	}

	joinResp, err := gc.sendRequest(joinReq)
	if err != nil {
		return fmt.Errorf("join group: %w", err)
	}

	jr := joinResp.(*protocol.JoinGroupResponse)
	if jr.ErrorCode != protocol.ErrNone {
		return fmt.Errorf("join group error: %d", jr.ErrorCode)
	}

	gc.mu.Lock()
	gc.memberID = jr.MemberID
	gc.generationID = jr.GenerationID
	gc.mu.Unlock()

	// SyncGroup
	var syncAssignments []protocol.SyncGroupAssignment
	if jr.MemberID == jr.Leader {
		// This consumer is the leader — run assignment
		syncAssignments, err = gc.performAssignment(jr.Members, topics, strategy)
		if err != nil {
			return fmt.Errorf("assignment: %w", err)
		}
	}

	syncReq := &protocol.SyncGroupRequest{
		Header: protocol.RequestHeader{
			APIKey:        protocol.APIKeySyncGroup,
			APIVersion:    0,
			CorrelationID: gc.nextCorrID(),
			ClientID:      gc.config.ClientID,
		},
		GroupID:      gc.config.GroupID,
		GenerationID: jr.GenerationID,
		MemberID:     jr.MemberID,
		Assignments:  syncAssignments,
	}

	syncResp, err := gc.sendRequest(syncReq)
	if err != nil {
		return fmt.Errorf("sync group: %w", err)
	}

	sr := syncResp.(*protocol.SyncGroupResponse)
	if sr.ErrorCode == protocol.ErrRebalanceInProgress || sr.ErrorCode == protocol.ErrIllegalGeneration {
		// Another rebalance started while we were syncing. Re-join.
		time.Sleep(200 * time.Millisecond)
		return gc.joinGroupRetry(0)
	}
	if sr.ErrorCode != protocol.ErrNone {
		return fmt.Errorf("sync group error: %d", sr.ErrorCode)
	}

	// Parse assignment
	assignment, err := protocol.DecodeConsumerAssignment(sr.Assignment)
	if err != nil {
		return fmt.Errorf("decode assignment: %w", err)
	}

	gc.mu.Lock()
	gc.assignments = assignment.PartitionsByTopic
	gc.offsets = make(map[string]map[int32]int64)
	gc.mu.Unlock()

	// Fetch committed offsets
	if err := gc.fetchCommittedOffsets(); err != nil {
		return fmt.Errorf("fetch offsets: %w", err)
	}

	return nil
}

func (gc *GroupConsumer) performAssignment(members []protocol.JoinGroupMember, topics []string, strategy string) ([]protocol.SyncGroupAssignment, error) {
	// Decode member subscriptions
	memberSubs := make(map[string]*protocol.ConsumerProtocolSubscription)
	for _, m := range members {
		sub, err := protocol.DecodeConsumerSubscription(m.Metadata)
		if err != nil {
			return nil, err
		}
		memberSubs[m.MemberID] = sub
	}

	// Get partition info from metadata
	partitions, err := gc.getTopicPartitions(topics)
	if err != nil {
		return nil, err
	}

	// Run assignor
	var assignments map[string]*protocol.ConsumerProtocolAssignment
	switch strategy {
	case "roundrobin":
		// Simple round-robin: collect all topic-partitions, distribute evenly
		assignments = gc.roundRobinAssign(memberSubs, partitions)
	default: // "range"
		assignments = gc.rangeAssign(memberSubs, partitions)
	}

	// Encode assignments
	var result []protocol.SyncGroupAssignment
	for memberID, assign := range assignments {
		result = append(result, protocol.SyncGroupAssignment{
			MemberID:   memberID,
			Assignment: protocol.EncodeConsumerAssignment(assign),
		})
	}

	return result, nil
}

func (gc *GroupConsumer) rangeAssign(members map[string]*protocol.ConsumerProtocolSubscription, partitions map[string][]int32) map[string]*protocol.ConsumerProtocolAssignment {
	result := make(map[string]*protocol.ConsumerProtocolAssignment, len(members))
	for id := range members {
		result[id] = &protocol.ConsumerProtocolAssignment{
			PartitionsByTopic: make(map[string][]int32),
		}
	}

	allTopics := make(map[string]bool)
	for _, sub := range members {
		for _, t := range sub.Topics {
			allTopics[t] = true
		}
	}

	for topic := range allTopics {
		parts, ok := partitions[topic]
		if !ok {
			continue
		}

		var subscribedMembers []string
		for id, sub := range members {
			for _, t := range sub.Topics {
				if t == topic {
					subscribedMembers = append(subscribedMembers, id)
					break
				}
			}
		}
		sort.Strings(subscribedMembers)

		sorted := make([]int32, len(parts))
		copy(sorted, parts)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

		n := len(sorted)
		m := len(subscribedMembers)
		if m == 0 {
			continue
		}

		rangeSize := n / m
		extra := n % m
		idx := 0
		for i, memberID := range subscribedMembers {
			count := rangeSize
			if i < extra {
				count++
			}
			if count > 0 {
				result[memberID].PartitionsByTopic[topic] = sorted[idx : idx+count]
				idx += count
			}
		}
	}

	return result
}

func (gc *GroupConsumer) roundRobinAssign(members map[string]*protocol.ConsumerProtocolSubscription, partitions map[string][]int32) map[string]*protocol.ConsumerProtocolAssignment {
	result := make(map[string]*protocol.ConsumerProtocolAssignment, len(members))
	sortedMembers := make([]string, 0, len(members))
	for id := range members {
		result[id] = &protocol.ConsumerProtocolAssignment{
			PartitionsByTopic: make(map[string][]int32),
		}
		sortedMembers = append(sortedMembers, id)
	}
	sort.Strings(sortedMembers)

	type tp struct {
		topic     string
		partition int32
	}
	var allTP []tp

	topics := make([]string, 0, len(partitions))
	for t := range partitions {
		topics = append(topics, t)
	}
	sort.Strings(topics)

	for _, t := range topics {
		parts := make([]int32, len(partitions[t]))
		copy(parts, partitions[t])
		sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
		for _, p := range parts {
			allTP = append(allTP, tp{t, p})
		}
	}

	for i, tp := range allTP {
		memberID := sortedMembers[i%len(sortedMembers)]
		result[memberID].PartitionsByTopic[tp.topic] = append(
			result[memberID].PartitionsByTopic[tp.topic],
			tp.partition,
		)
	}

	return result
}

func (gc *GroupConsumer) getTopicPartitions(topics []string) (map[string][]int32, error) {
	metaReq := &protocol.MetadataRequest{
		Header: protocol.RequestHeader{
			APIKey:        protocol.APIKeyMetadata,
			APIVersion:    1,
			CorrelationID: gc.nextCorrID(),
			ClientID:      gc.config.ClientID,
		},
		Topics: topics,
	}

	resp, err := gc.sendRequest(metaReq)
	if err != nil {
		return nil, err
	}

	metaResp := resp.(*protocol.MetadataResponse)
	result := make(map[string][]int32)
	for _, t := range metaResp.Topics {
		if t.ErrorCode != protocol.ErrNone {
			continue
		}
		for _, p := range t.Partitions {
			result[t.Topic] = append(result[t.Topic], p.Partition)
		}
	}

	return result, nil
}

func (gc *GroupConsumer) fetchCommittedOffsets() error {
	gc.mu.Lock()
	var fetchTopics []protocol.OffsetFetchTopic
	for topic, parts := range gc.assignments {
		fetchTopics = append(fetchTopics, protocol.OffsetFetchTopic{
			Topic:      topic,
			Partitions: parts,
		})
	}
	gc.mu.Unlock()

	if len(fetchTopics) == 0 {
		return nil
	}

	req := &protocol.OffsetFetchRequest{
		Header: protocol.RequestHeader{
			APIKey:        protocol.APIKeyOffsetFetch,
			APIVersion:    0,
			CorrelationID: gc.nextCorrID(),
			ClientID:      gc.config.ClientID,
		},
		GroupID: gc.config.GroupID,
		Topics:  fetchTopics,
	}

	resp, err := gc.sendRequest(req)
	if err != nil {
		return err
	}

	ofr := resp.(*protocol.OffsetFetchResponse)
	gc.mu.Lock()
	for _, t := range ofr.Topics {
		for _, p := range t.Partitions {
			if gc.offsets[t.Topic] == nil {
				gc.offsets[t.Topic] = make(map[int32]int64)
			}
			if p.CommittedOffset >= 0 {
				gc.offsets[t.Topic][p.Partition] = p.CommittedOffset
			} else {
				gc.offsets[t.Topic][p.Partition] = 0
			}
		}
	}
	gc.mu.Unlock()

	return nil
}

// Poll fetches messages from assigned partitions.
func (gc *GroupConsumer) Poll(timeout time.Duration) ([]Message, error) {
	gc.mu.Lock()
	if len(gc.assignments) == 0 {
		gc.mu.Unlock()
		return nil, nil
	}

	// Build fetch request
	req := &protocol.FetchRequest{
		Header: protocol.RequestHeader{
			APIKey:        protocol.APIKeyFetch,
			APIVersion:    4,
			CorrelationID: gc.nextCorrID(),
			ClientID:      gc.config.ClientID,
		},
		ReplicaID: -1,
		MaxWaitMs: int32(timeout.Milliseconds()),
		MinBytes:  1,
		MaxBytes:  gc.config.MaxBytes,
	}

	for topic, parts := range gc.assignments {
		td := protocol.FetchTopicData{Topic: topic}
		for _, p := range parts {
			offset := gc.offsets[topic][p]
			td.Partitions = append(td.Partitions, protocol.FetchPartitionData{
				Partition:   p,
				FetchOffset: offset,
				MaxBytes:    gc.config.MaxBytes,
			})
		}
		req.Topics = append(req.Topics, td)
	}
	gc.mu.Unlock()

	resp, err := gc.sendRequest(req)
	if err != nil {
		return nil, err
	}

	fetchResp := resp.(*protocol.FetchResponse)
	var messages []Message

	gc.mu.Lock()
	defer gc.mu.Unlock()

	for _, topicResp := range fetchResp.Topics {
		for _, partResp := range topicResp.Partitions {
			if partResp.ErrorCode != protocol.ErrNone || len(partResp.RecordBatches) == 0 {
				continue
			}

			batches, err := protocol.KafkaRecordBatchToInternal(partResp.RecordBatches)
			if err != nil {
				continue
			}

			for _, batch := range batches {
				for i, rec := range batch.Records {
					messages = append(messages, Message{
						Topic:     topicResp.Topic,
						Partition: partResp.Partition,
						Offset:    batch.BaseOffset + int64(i),
						Key:       rec.Key,
						Value:     rec.Value,
					})
				}

				// Advance offset
				newOffset := batch.BaseOffset + int64(len(batch.Records))
				if gc.offsets[topicResp.Topic] == nil {
					gc.offsets[topicResp.Topic] = make(map[int32]int64)
				}
				gc.offsets[topicResp.Topic][partResp.Partition] = newOffset
			}
		}
	}

	return messages, nil
}

// CommitSync commits current offsets synchronously.
func (gc *GroupConsumer) CommitSync() error {
	gc.mu.Lock()
	if len(gc.offsets) == 0 {
		gc.mu.Unlock()
		return nil
	}

	req := &protocol.OffsetCommitRequest{
		Header: protocol.RequestHeader{
			APIKey:        protocol.APIKeyOffsetCommit,
			APIVersion:    1,
			CorrelationID: gc.nextCorrID(),
			ClientID:      gc.config.ClientID,
		},
		GroupID:      gc.config.GroupID,
		GenerationID: gc.generationID,
		MemberID:     gc.memberID,
	}

	for topic, parts := range gc.offsets {
		ct := protocol.OffsetCommitTopic{Topic: topic}
		for partition, offset := range parts {
			ct.Partitions = append(ct.Partitions, protocol.OffsetCommitPartition{
				Partition: partition,
				Offset:    offset,
			})
		}
		req.Topics = append(req.Topics, ct)
	}
	gc.mu.Unlock()

	resp, err := gc.sendRequest(req)
	if err != nil {
		return err
	}

	cr := resp.(*protocol.OffsetCommitResponse)
	for _, t := range cr.Topics {
		for _, p := range t.Partitions {
			if p.ErrorCode != protocol.ErrNone {
				return fmt.Errorf("commit error for %s-%d: %d", t.Topic, p.Partition, p.ErrorCode)
			}
		}
	}

	return nil
}

func (gc *GroupConsumer) heartbeatLoop() {
	defer gc.heartbeatWg.Done()
	interval := time.Duration(gc.config.SessionTimeoutMs/3) * time.Millisecond
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-gc.closed:
			return
		case <-ticker.C:
			gc.mu.Lock()
			req := &protocol.HeartbeatRequest{
				Header: protocol.RequestHeader{
					APIKey:        protocol.APIKeyHeartbeat,
					APIVersion:    0,
					CorrelationID: gc.nextCorrID(),
					ClientID:      gc.config.ClientID,
				},
				GroupID:      gc.config.GroupID,
				GenerationID: gc.generationID,
				MemberID:     gc.memberID,
			}
			gc.mu.Unlock()

			resp, err := gc.sendRequest(req)
			if err != nil {
				continue
			}

			hr := resp.(*protocol.HeartbeatResponse)
			if hr.ErrorCode == protocol.ErrRebalanceInProgress {
				// Rejoin in place — heartbeat loop keeps running
				gc.joinGroup()
			}
		}
	}
}

func (gc *GroupConsumer) autoCommitLoop() {
	defer gc.commitWg.Done()
	ticker := time.NewTicker(gc.config.AutoCommitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-gc.closed:
			return
		case <-ticker.C:
			gc.CommitSync()
		}
	}
}

// Close leaves the group and closes the connection.
func (gc *GroupConsumer) Close() error {
	gc.closeOnce.Do(func() {
		close(gc.closed)
	})

	// Send LeaveGroup before stopping goroutines so the coordinator learns quickly.
	gc.mu.Lock()
	memberID := gc.memberID
	gc.mu.Unlock()
	if memberID != "" {
		leaveReq := &protocol.LeaveGroupRequest{
			Header: protocol.RequestHeader{
				APIKey:        protocol.APIKeyLeaveGroup,
				APIVersion:    0,
				CorrelationID: gc.nextCorrID(),
				ClientID:      gc.config.ClientID,
			},
			GroupID:  gc.config.GroupID,
			MemberID: memberID,
		}
		gc.sendRequest(leaveReq)
	}

	// Close connection — unblocks any in-flight reads in heartbeat/commit loops.
	gc.conn.Close()

	gc.heartbeatWg.Wait()
	gc.commitWg.Wait()
	return nil
}

func (gc *GroupConsumer) sendRequest(req interface{}) (interface{}, error) {
	data, err := protocol.EncodeRequest(req)
	if err != nil {
		return nil, err
	}

	frame := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(frame, uint32(len(data)))
	copy(frame[4:], data)

	gc.sendMu.Lock()
	defer gc.sendMu.Unlock()
	conn := gc.conn

	if _, err := conn.Write(frame); err != nil {
		return nil, err
	}

	var respLenBuf [4]byte
	if _, err := io.ReadFull(conn, respLenBuf[:]); err != nil {
		return nil, err
	}
	respLen := binary.BigEndian.Uint32(respLenBuf[:])
	respBody := make([]byte, respLen)
	if _, err := io.ReadFull(conn, respBody); err != nil {
		return nil, err
	}

	var apiKey int16
	var apiVersion int16
	switch r := req.(type) {
	case *protocol.FetchRequest:
		apiKey = protocol.APIKeyFetch
		apiVersion = r.Header.APIVersion
	case *protocol.MetadataRequest:
		apiKey = protocol.APIKeyMetadata
		apiVersion = r.Header.APIVersion
	case *protocol.JoinGroupRequest:
		apiKey = protocol.APIKeyJoinGroup
		apiVersion = r.Header.APIVersion
	case *protocol.SyncGroupRequest:
		apiKey = protocol.APIKeySyncGroup
		apiVersion = r.Header.APIVersion
	case *protocol.HeartbeatRequest:
		apiKey = protocol.APIKeyHeartbeat
		apiVersion = r.Header.APIVersion
	case *protocol.LeaveGroupRequest:
		apiKey = protocol.APIKeyLeaveGroup
		apiVersion = r.Header.APIVersion
	case *protocol.OffsetCommitRequest:
		apiKey = protocol.APIKeyOffsetCommit
		apiVersion = r.Header.APIVersion
	case *protocol.OffsetFetchRequest:
		apiKey = protocol.APIKeyOffsetFetch
		apiVersion = r.Header.APIVersion
	case *protocol.FindCoordinatorRequest:
		apiKey = protocol.APIKeyFindCoordinator
		apiVersion = r.Header.APIVersion
	}

	return protocol.DecodeResponse(apiKey, apiVersion, respBody)
}

func (gc *GroupConsumer) nextCorrID() int32 {
	return atomic.AddInt32(&gc.corrID, 1)
}
