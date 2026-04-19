package broker

import (
	"crypto/rand"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/harshithgowda/streamq/internal/protocol"
)

// GroupState represents the state of a consumer group.
type GroupState int

const (
	GroupStateEmpty               GroupState = 0
	GroupStatePreparingRebalance  GroupState = 1
	GroupStateCompletingRebalance GroupState = 2
	GroupStateStable              GroupState = 3
	GroupStateDead                GroupState = 4
)

// MemberMetadata holds state for a consumer group member.
type MemberMetadata struct {
	MemberID           string
	ClientID           string
	SessionTimeoutMs   int32
	RebalanceTimeoutMs int32
	Protocols          []protocol.JoinGroupProtocol
	Assignment         []byte
	sessionTimer       *time.Timer
}

// ConsumerGroup holds the state of a consumer group.
type ConsumerGroup struct {
	mu             sync.Mutex
	GroupID        string
	State          GroupState
	ProtocolType   string
	ProtocolName   string
	GenerationID   int32
	LeaderID       string
	Members        map[string]*MemberMetadata
	PendingMembers map[string]chan *protocol.JoinGroupResponse
	syncBarrier    map[string]chan []byte
	Offsets        map[string]map[int32]OffsetAndMetadata
}

// GroupCoordinator manages consumer groups. All offset state is durably
// stored in the __consumer_offsets topic; the in-memory map is just a cache
// rebuilt from that log at startup.
type GroupCoordinator struct {
	mu     sync.RWMutex
	groups map[string]*ConsumerGroup
	broker *Broker
}

// NewGroupCoordinator creates a new GroupCoordinator. The __consumer_offsets
// topic is bootstrapped lazily via Start(), after topic management is ready.
func NewGroupCoordinator(broker *Broker) *GroupCoordinator {
	return &GroupCoordinator{
		groups: make(map[string]*ConsumerGroup),
		broker: broker,
	}
}

// Start ensures the __consumer_offsets topic exists and replays committed
// offsets into memory. Must be called after TopicManager is ready and, in
// cluster mode, after the broker has joined the cluster.
func (gc *GroupCoordinator) Start() {
	if err := gc.ensureOffsetsTopic(); err != nil {
		log.Printf("group coordinator: ensure offsets topic: %v", err)
	}
	gc.loadOffsetsFromLog()
}

// Close stops the coordinator. Offsets are already durable in
// __consumer_offsets, so there's nothing to flush here.
func (gc *GroupCoordinator) Close() {}

func (gc *GroupCoordinator) getOrCreateGroup(groupID string) *ConsumerGroup {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	g, ok := gc.groups[groupID]
	if !ok {
		g = &ConsumerGroup{
			GroupID:        groupID,
			State:          GroupStateEmpty,
			Members:        make(map[string]*MemberMetadata),
			PendingMembers: make(map[string]chan *protocol.JoinGroupResponse),
			syncBarrier:    make(map[string]chan []byte),
			Offsets:        make(map[string]map[int32]OffsetAndMetadata),
		}
		gc.groups[groupID] = g
	}
	return g
}

func generateMemberID(clientID string) string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%s-%x", clientID, b)
}

// HandleJoinGroup handles a JoinGroup request.
func (gc *GroupCoordinator) HandleJoinGroup(req *protocol.JoinGroupRequest) *protocol.JoinGroupResponse {
	group := gc.getOrCreateGroup(req.GroupID)
	group.mu.Lock()

	// Generate member ID if empty (first join)
	memberID := req.MemberID
	if memberID == "" {
		memberID = generateMemberID(req.Header.ClientID)
		// Return MEMBER_ID_REQUIRED-style: send back the ID, client re-joins
		// Actually in Kafka protocol, empty memberID on first join is normal.
		// The broker generates one and includes it in the response.
	}

	// Check protocol type consistency
	if group.ProtocolType != "" && group.ProtocolType != req.ProtocolType {
		group.mu.Unlock()
		return &protocol.JoinGroupResponse{
			Header:    protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
			ErrorCode: protocol.ErrInconsistentGroupProtocol,
			MemberID:  memberID,
		}
	}

	// Check that at least one protocol matches existing members
	if len(group.Members) > 0 {
		hasCommon := false
		for _, p := range req.Protocols {
			if gc.protocolSupported(group, p.Name) {
				hasCommon = true
				break
			}
		}
		if !hasCommon && len(req.Protocols) > 0 {
			group.mu.Unlock()
			return &protocol.JoinGroupResponse{
				Header:    protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
				ErrorCode: protocol.ErrInconsistentGroupProtocol,
				MemberID:  memberID,
			}
		}
	}

	// Add or update member
	member, exists := group.Members[memberID]
	if !exists {
		member = &MemberMetadata{
			MemberID: memberID,
			ClientID: req.Header.ClientID,
		}
		group.Members[memberID] = member
	}
	member.SessionTimeoutMs = req.SessionTimeoutMs
	member.RebalanceTimeoutMs = req.RebalanceTimeoutMs
	member.Protocols = req.Protocols

	if group.ProtocolType == "" {
		group.ProtocolType = req.ProtocolType
	}

	// Create a channel for this member to wait on
	ch := make(chan *protocol.JoinGroupResponse, 1)
	group.PendingMembers[memberID] = ch

	// Trigger rebalance
	wasEmpty := group.State == GroupStateEmpty
	if group.State == GroupStateStable || group.State == GroupStateEmpty || group.State == GroupStateCompletingRebalance {
		group.State = GroupStatePreparingRebalance

		// Schedule a rebalance completion timer. On first rebalance from Empty, wait
		// a short initial delay to absorb simultaneous joiners (like Kafka's
		// group.initial.rebalance.delay.ms). Otherwise use the rebalance timeout.
		var delay time.Duration
		if wasEmpty {
			delay = 500 * time.Millisecond
		} else {
			rebalanceTimeout := req.RebalanceTimeoutMs
			if rebalanceTimeout <= 0 {
				rebalanceTimeout = 10000
			}
			delay = time.Duration(rebalanceTimeout) * time.Millisecond
		}
		time.AfterFunc(delay, func() {
			gc.tryCompleteJoin(group)
		})
	}

	// Don't auto-complete from Empty — always wait for initial rebalance delay so
	// late joiners can participate in the same generation. For subsequent
	// rebalances, complete immediately if everyone has rejoined.
	if !wasEmpty && gc.allMembersJoined(group) {
		gc.doCompleteJoin(group)
		group.mu.Unlock()
	} else {
		group.mu.Unlock()
	}

	// Block waiting for join response (with timeout)
	timeout := time.Duration(req.RebalanceTimeoutMs+req.SessionTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	select {
	case resp := <-ch:
		resp.Header.CorrelationID = req.Header.CorrelationID
		// Start session timer for this member
		gc.resetSessionTimer(group, memberID)
		return resp
	case <-time.After(timeout):
		// Timeout — remove member
		group.mu.Lock()
		delete(group.PendingMembers, memberID)
		delete(group.Members, memberID)
		if len(group.Members) == 0 {
			group.State = GroupStateEmpty
		}
		group.mu.Unlock()
		return &protocol.JoinGroupResponse{
			Header:    protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
			ErrorCode: protocol.ErrRebalanceInProgress,
			MemberID:  memberID,
		}
	}
}

func (gc *GroupCoordinator) protocolSupported(group *ConsumerGroup, name string) bool {
	for _, m := range group.Members {
		found := false
		for _, p := range m.Protocols {
			if p.Name == name {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (gc *GroupCoordinator) allMembersJoined(group *ConsumerGroup) bool {
	if len(group.Members) == 0 {
		return false
	}
	for memberID := range group.Members {
		if _, ok := group.PendingMembers[memberID]; !ok {
			return false
		}
	}
	return true
}

func (gc *GroupCoordinator) tryCompleteJoin(group *ConsumerGroup) {
	group.mu.Lock()
	defer group.mu.Unlock()

	if group.State != GroupStatePreparingRebalance {
		return
	}
	gc.doCompleteJoin(group)
}

// doCompleteJoin must be called with group.mu held.
func (gc *GroupCoordinator) doCompleteJoin(group *ConsumerGroup) {
	if group.State != GroupStatePreparingRebalance {
		return
	}

	// Remove members that didn't rejoin
	for memberID := range group.Members {
		if _, ok := group.PendingMembers[memberID]; !ok {
			if m := group.Members[memberID]; m != nil && m.sessionTimer != nil {
				m.sessionTimer.Stop()
			}
			delete(group.Members, memberID)
		}
	}

	if len(group.Members) == 0 {
		group.State = GroupStateEmpty
		return
	}

	// Increment generation
	group.GenerationID++

	// Pick protocol (first protocol supported by all members)
	group.ProtocolName = gc.selectProtocol(group)

	// Pick leader (first member in map iteration — deterministic enough for single node)
	var leaderID string
	for id := range group.Members {
		leaderID = id
		break
	}
	group.LeaderID = leaderID

	// Build member metadata for leader
	var members []protocol.JoinGroupMember
	for _, m := range group.Members {
		// Find metadata for selected protocol
		var metadata []byte
		for _, p := range m.Protocols {
			if p.Name == group.ProtocolName {
				metadata = p.Metadata
				break
			}
		}
		members = append(members, protocol.JoinGroupMember{
			MemberID: m.MemberID,
			Metadata: metadata,
		})
	}

	group.State = GroupStateCompletingRebalance

	// Initialize sync barrier for all members
	group.syncBarrier = make(map[string]chan []byte)
	for memberID := range group.Members {
		group.syncBarrier[memberID] = make(chan []byte, 1)
	}

	// Send responses to all pending members
	for memberID, ch := range group.PendingMembers {
		resp := &protocol.JoinGroupResponse{
			ErrorCode:    protocol.ErrNone,
			GenerationID: group.GenerationID,
			ProtocolName: group.ProtocolName,
			Leader:       leaderID,
			MemberID:     memberID,
		}
		// Only leader gets member list
		if memberID == leaderID {
			resp.Members = members
		}
		ch <- resp
	}

	// Clear pending
	group.PendingMembers = make(map[string]chan *protocol.JoinGroupResponse)
}

func (gc *GroupCoordinator) selectProtocol(group *ConsumerGroup) string {
	// Count votes for each protocol
	votes := make(map[string]int)
	for _, m := range group.Members {
		for _, p := range m.Protocols {
			votes[p.Name]++
		}
	}

	// Pick protocol supported by all members with most votes
	numMembers := len(group.Members)
	bestName := ""
	bestVotes := 0
	for name, count := range votes {
		if count == numMembers && count > bestVotes {
			bestName = name
			bestVotes = count
		}
	}

	if bestName == "" {
		// Fallback: first protocol of first member
		for _, m := range group.Members {
			if len(m.Protocols) > 0 {
				return m.Protocols[0].Name
			}
		}
	}
	return bestName
}

// HandleSyncGroup handles a SyncGroup request.
func (gc *GroupCoordinator) HandleSyncGroup(req *protocol.SyncGroupRequest) *protocol.SyncGroupResponse {
	group := gc.getOrCreateGroup(req.GroupID)
	group.mu.Lock()

	member, ok := group.Members[req.MemberID]
	if !ok {
		group.mu.Unlock()
		return &protocol.SyncGroupResponse{
			Header:    protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
			ErrorCode: protocol.ErrUnknownMemberID,
		}
	}
	_ = member

	if req.GenerationID != group.GenerationID {
		group.mu.Unlock()
		return &protocol.SyncGroupResponse{
			Header:    protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
			ErrorCode: protocol.ErrIllegalGeneration,
		}
	}

	if group.State != GroupStateCompletingRebalance {
		// If already stable, return stored assignment
		if group.State == GroupStateStable {
			m := group.Members[req.MemberID]
			assignment := m.Assignment
			group.mu.Unlock()
			return &protocol.SyncGroupResponse{
				Header:     protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
				ErrorCode:  protocol.ErrNone,
				Assignment: assignment,
			}
		}
		group.mu.Unlock()
		return &protocol.SyncGroupResponse{
			Header:    protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
			ErrorCode: protocol.ErrRebalanceInProgress,
		}
	}

	// Get sync barrier channel for this member
	barrierCh, hasCh := group.syncBarrier[req.MemberID]

	// If this is the leader, distribute assignments
	if req.MemberID == group.LeaderID {
		for _, a := range req.Assignments {
			if m, ok := group.Members[a.MemberID]; ok {
				m.Assignment = a.Assignment
			}
			if ch, ok := group.syncBarrier[a.MemberID]; ok {
				ch <- a.Assignment
			}
		}
		group.State = GroupStateStable
		group.mu.Unlock()

		// Leader gets its own assignment
		if hasCh {
			select {
			case assignment := <-barrierCh:
				return &protocol.SyncGroupResponse{
					Header:     protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
					ErrorCode:  protocol.ErrNone,
					Assignment: assignment,
				}
			case <-time.After(30 * time.Second):
				return &protocol.SyncGroupResponse{
					Header:    protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
					ErrorCode: protocol.ErrRebalanceInProgress,
				}
			}
		}
		return &protocol.SyncGroupResponse{
			Header:    protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
			ErrorCode: protocol.ErrNone,
		}
	}

	// Follower: wait for leader to send assignments
	group.mu.Unlock()

	if !hasCh {
		return &protocol.SyncGroupResponse{
			Header:    protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
			ErrorCode: protocol.ErrUnknownMemberID,
		}
	}

	select {
	case assignment := <-barrierCh:
		return &protocol.SyncGroupResponse{
			Header:     protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
			ErrorCode:  protocol.ErrNone,
			Assignment: assignment,
		}
	case <-time.After(30 * time.Second):
		return &protocol.SyncGroupResponse{
			Header:    protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
			ErrorCode: protocol.ErrRebalanceInProgress,
		}
	}
}

// HandleHeartbeat handles a Heartbeat request.
func (gc *GroupCoordinator) HandleHeartbeat(req *protocol.HeartbeatRequest) *protocol.HeartbeatResponse {
	group := gc.getOrCreateGroup(req.GroupID)
	group.mu.Lock()
	defer group.mu.Unlock()

	_, ok := group.Members[req.MemberID]
	if !ok {
		return &protocol.HeartbeatResponse{
			Header:    protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
			ErrorCode: protocol.ErrUnknownMemberID,
		}
	}

	if req.GenerationID != group.GenerationID {
		return &protocol.HeartbeatResponse{
			Header:    protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
			ErrorCode: protocol.ErrIllegalGeneration,
		}
	}

	if group.State == GroupStatePreparingRebalance || group.State == GroupStateCompletingRebalance {
		return &protocol.HeartbeatResponse{
			Header:    protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
			ErrorCode: protocol.ErrRebalanceInProgress,
		}
	}

	gc.resetSessionTimerLocked(group, req.MemberID)

	return &protocol.HeartbeatResponse{
		Header:    protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
		ErrorCode: protocol.ErrNone,
	}
}

func (gc *GroupCoordinator) resetSessionTimer(group *ConsumerGroup, memberID string) {
	group.mu.Lock()
	defer group.mu.Unlock()
	gc.resetSessionTimerLocked(group, memberID)
}

// resetSessionTimerLocked must be called with group.mu held.
func (gc *GroupCoordinator) resetSessionTimerLocked(group *ConsumerGroup, memberID string) {
	member, ok := group.Members[memberID]
	if !ok {
		return
	}

	if member.sessionTimer != nil {
		member.sessionTimer.Stop()
	}

	timeout := time.Duration(member.SessionTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	member.sessionTimer = time.AfterFunc(timeout, func() {
		gc.expireMember(group.GroupID, memberID)
	})
}

func (gc *GroupCoordinator) expireMember(groupID, memberID string) {
	gc.mu.RLock()
	group, ok := gc.groups[groupID]
	gc.mu.RUnlock()
	if !ok {
		return
	}

	group.mu.Lock()
	member, ok := group.Members[memberID]
	if !ok {
		group.mu.Unlock()
		return
	}

	log.Printf("consumer group %s: member %s session expired", groupID, memberID)

	if member.sessionTimer != nil {
		member.sessionTimer.Stop()
	}
	delete(group.Members, memberID)
	delete(group.PendingMembers, memberID)
	delete(group.syncBarrier, memberID)

	if len(group.Members) == 0 {
		group.State = GroupStateEmpty
		group.mu.Unlock()
		return
	}

	// Trigger rebalance
	group.State = GroupStatePreparingRebalance
	group.mu.Unlock()

	// If all remaining members have pending joins, complete immediately
	group.mu.Lock()
	if gc.allMembersJoined(group) {
		gc.doCompleteJoin(group)
	}
	group.mu.Unlock()
}

// HandleLeaveGroup handles a LeaveGroup request.
func (gc *GroupCoordinator) HandleLeaveGroup(req *protocol.LeaveGroupRequest) *protocol.LeaveGroupResponse {
	group := gc.getOrCreateGroup(req.GroupID)
	group.mu.Lock()

	member, ok := group.Members[req.MemberID]
	if !ok {
		group.mu.Unlock()
		return &protocol.LeaveGroupResponse{
			Header:    protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
			ErrorCode: protocol.ErrUnknownMemberID,
		}
	}

	if member.sessionTimer != nil {
		member.sessionTimer.Stop()
	}
	delete(group.Members, req.MemberID)
	delete(group.PendingMembers, req.MemberID)
	delete(group.syncBarrier, req.MemberID)

	if len(group.Members) == 0 {
		group.State = GroupStateEmpty
		group.mu.Unlock()
	} else {
		// Trigger rebalance
		group.State = GroupStatePreparingRebalance
		if gc.allMembersJoined(group) {
			gc.doCompleteJoin(group)
		}
		group.mu.Unlock()
	}

	return &protocol.LeaveGroupResponse{
		Header:    protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
		ErrorCode: protocol.ErrNone,
	}
}

// HandleOffsetCommit handles an OffsetCommit request.
func (gc *GroupCoordinator) HandleOffsetCommit(req *protocol.OffsetCommitRequest) *protocol.OffsetCommitResponse {
	group := gc.getOrCreateGroup(req.GroupID)
	group.mu.Lock()
	defer group.mu.Unlock()

	// Validate member if generation > 0
	if req.GenerationID > 0 {
		if _, ok := group.Members[req.MemberID]; !ok {
			resp := &protocol.OffsetCommitResponse{
				Header: protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
			}
			for _, t := range req.Topics {
				tr := protocol.OffsetCommitTopicResponse{Topic: t.Topic}
				for _, p := range t.Partitions {
					tr.Partitions = append(tr.Partitions, protocol.OffsetCommitPartitionResponse{
						Partition: p.Partition,
						ErrorCode: protocol.ErrUnknownMemberID,
					})
				}
				resp.Topics = append(resp.Topics, tr)
			}
			return resp
		}
		if req.GenerationID != group.GenerationID {
			resp := &protocol.OffsetCommitResponse{
				Header: protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
			}
			for _, t := range req.Topics {
				tr := protocol.OffsetCommitTopicResponse{Topic: t.Topic}
				for _, p := range t.Partitions {
					tr.Partitions = append(tr.Partitions, protocol.OffsetCommitPartitionResponse{
						Partition: p.Partition,
						ErrorCode: protocol.ErrIllegalGeneration,
					})
				}
				resp.Topics = append(resp.Topics, tr)
			}
			return resp
		}
	}

	resp := &protocol.OffsetCommitResponse{
		Header: protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
	}

	now := time.Now().UnixMilli()
	for _, t := range req.Topics {
		tr := protocol.OffsetCommitTopicResponse{Topic: t.Topic}
		for _, p := range t.Partitions {
			metadata := ""
			if p.Metadata != nil {
				metadata = *p.Metadata
			}

			// Write to __consumer_offsets first. Only acknowledge success to
			// the client after the record is durable. If this fails, report
			// an error and leave the in-memory map untouched so the client
			// retries — we must never tell a client we committed when we
			// didn't.
			key := encodeOffsetKey(offsetCommitKey{
				GroupID:   req.GroupID,
				Topic:     t.Topic,
				Partition: p.Partition,
			})
			val := encodeOffsetValue(offsetCommitValue{
				Offset:    p.Offset,
				Metadata:  metadata,
				Timestamp: now,
			})

			// Release group.mu while we hit the log so we don't hold the
			// group lock during disk I/O.
			group.mu.Unlock()
			appendErr := gc.appendOffsetRecord(req.GroupID, key, val)
			group.mu.Lock()

			if appendErr != nil {
				log.Printf("offset commit for %s/%s-%d failed: %v",
					req.GroupID, t.Topic, p.Partition, appendErr)
				tr.Partitions = append(tr.Partitions, protocol.OffsetCommitPartitionResponse{
					Partition: p.Partition,
					ErrorCode: protocol.ErrUnknown,
				})
				continue
			}

			if group.Offsets[t.Topic] == nil {
				group.Offsets[t.Topic] = make(map[int32]OffsetAndMetadata)
			}
			group.Offsets[t.Topic][p.Partition] = OffsetAndMetadata{
				Offset:   p.Offset,
				Metadata: metadata,
			}
			tr.Partitions = append(tr.Partitions, protocol.OffsetCommitPartitionResponse{
				Partition: p.Partition,
				ErrorCode: protocol.ErrNone,
			})
		}
		resp.Topics = append(resp.Topics, tr)
	}

	return resp
}

// HandleOffsetFetch handles an OffsetFetch request.
func (gc *GroupCoordinator) HandleOffsetFetch(req *protocol.OffsetFetchRequest) *protocol.OffsetFetchResponse {
	group := gc.getOrCreateGroup(req.GroupID)
	group.mu.Lock()
	defer group.mu.Unlock()

	resp := &protocol.OffsetFetchResponse{
		Header: protocol.ResponseHeader{CorrelationID: req.Header.CorrelationID},
	}

	for _, t := range req.Topics {
		topicResp := protocol.OffsetFetchTopicResponse{Topic: t.Topic}
		for _, p := range t.Partitions {
			partResp := protocol.OffsetFetchPartitionResponse{
				Partition:       p,
				CommittedOffset: -1,
				ErrorCode:       protocol.ErrNone,
			}
			if topicOffsets, ok := group.Offsets[t.Topic]; ok {
				if om, ok := topicOffsets[p]; ok {
					partResp.CommittedOffset = om.Offset
					if om.Metadata != "" {
						partResp.Metadata = &om.Metadata
					}
				}
			}
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp
}
