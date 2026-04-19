package broker

import (
	"sync"
	"time"

	"github.com/harshithgowda/streamq/internal/cluster"
)

// PartitionRole indicates the role of this broker for a partition.
type PartitionRole int

const (
	RoleNone     PartitionRole = 0
	RoleLeader   PartitionRole = 1
	RoleFollower PartitionRole = 2
)

// PartitionState tracks replication state for a single partition.
type PartitionState struct {
	mu            sync.RWMutex
	Role          PartitionRole
	Epoch         int32
	LEO           int64                      // = CommitLog.NextOffset
	HWM           int64                      // = min(LEO across ISR)
	ISR           []cluster.BrokerID
	ReplicaLEOs   map[cluster.BrokerID]int64 // leader tracks follower LEOs
	ReplicaLastMs map[cluster.BrokerID]int64 // last fetch time per follower

	hwmCond *sync.Cond // signaled when HWM advances
}

// NewPartitionState creates a new PartitionState.
func NewPartitionState() *PartitionState {
	ps := &PartitionState{
		ReplicaLEOs:   make(map[cluster.BrokerID]int64),
		ReplicaLastMs: make(map[cluster.BrokerID]int64),
	}
	ps.hwmCond = sync.NewCond(&ps.mu)
	return ps
}

// GetRole returns the current role.
func (ps *PartitionState) GetRole() PartitionRole {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.Role
}

// GetEpoch returns the current epoch.
func (ps *PartitionState) GetEpoch() int32 {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.Epoch
}

// GetHWM returns the current high water mark.
func (ps *PartitionState) GetHWM() int64 {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.HWM
}

// UpdateReplicaLEO updates the tracked LEO for a follower and records fetch time.
func (ps *PartitionState) UpdateReplicaLEO(followerID cluster.BrokerID, leo int64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.ReplicaLEOs[followerID] = leo
	ps.ReplicaLastMs[followerID] = time.Now().UnixMilli()
}

// UpdateHWM recalculates the HWM as min(LEO) across all ISR members.
// leaderLEO is the leader's current LEO.
func (ps *PartitionState) UpdateHWM(leaderLEO int64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.LEO = leaderLEO

	if len(ps.ISR) == 0 {
		return
	}

	// HWM = min LEO across all ISR members
	minLEO := leaderLEO
	for _, id := range ps.ISR {
		if leo, ok := ps.ReplicaLEOs[id]; ok {
			if leo < minLEO {
				minLEO = leo
			}
		} else {
			// If a follower in ISR hasn't reported yet, use 0
			// (but the leader's own LEO should always be included)
			// The leader doesn't track itself in ReplicaLEOs
		}
	}

	if minLEO > ps.HWM {
		ps.HWM = minLEO
		ps.hwmCond.Broadcast()
	}
}

// WaitForHWM blocks until HWM >= targetOffset or timeout expires.
// Returns true if HWM reached the target.
func (ps *PartitionState) WaitForHWM(targetOffset int64, timeout time.Duration) bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.HWM >= targetOffset {
		return true
	}

	deadline := time.Now().Add(timeout)
	for ps.HWM < targetOffset {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}

		// Use a timer-based wait to avoid blocking forever
		done := make(chan struct{})
		go func() {
			time.Sleep(remaining)
			ps.hwmCond.Broadcast()
			close(done)
		}()

		ps.hwmCond.Wait()

		select {
		case <-done:
		default:
		}

		if ps.HWM >= targetOffset {
			return true
		}
	}

	return ps.HWM >= targetOffset
}
