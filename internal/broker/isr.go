package broker

import (
	"log"
	"sync"
	"time"

	"github.com/harshithgowda/streamq/internal/cluster"
)

// ISRManager periodically checks and manages ISR membership for leader partitions.
type ISRManager struct {
	broker   *Broker
	lagMaxMs int64 // max allowed lag in milliseconds before shrinking ISR

	quit chan struct{}
	wg   sync.WaitGroup
}

// NewISRManager creates a new ISRManager.
func NewISRManager(b *Broker, lagMaxMs int64) *ISRManager {
	return &ISRManager{
		broker:   b,
		lagMaxMs: lagMaxMs,
		quit:     make(chan struct{}),
	}
}

// Start begins the ISR check loop.
func (m *ISRManager) Start() {
	m.wg.Add(1)
	go m.loop()
}

func (m *ISRManager) loop() {
	defer m.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.check()
		case <-m.quit:
			return
		}
	}
}

func (m *ISRManager) check() {
	meta := m.broker.GetClusterMetadata()
	if meta == nil {
		return
	}

	nowMs := time.Now().UnixMilli()

	for topic, assignments := range meta.Assignments {
		for _, pa := range assignments {
			if pa.Leader != m.broker.nodeID {
				continue
			}

			state := m.broker.GetPartitionState(topic, pa.Partition)
			if state == nil {
				continue
			}

			state.mu.Lock()

			changed := false
			var newISR []cluster.BrokerID

			// Check each ISR member
			for _, id := range state.ISR {
				if id == m.broker.nodeID {
					// Leader is always in ISR
					newISR = append(newISR, id)
					continue
				}

				lastMs, ok := state.ReplicaLastMs[id]
				if !ok || (nowMs-lastMs) > m.lagMaxMs {
					// Follower is lagging, shrink ISR
					log.Printf("ISR shrink: removing broker %d from %s-%d (lag: %dms)",
						id, topic, pa.Partition, nowMs-lastMs)
					changed = true
					continue
				}

				newISR = append(newISR, id)
			}

			// Check for ISR expansion: out-of-ISR replicas that have caught up
			for _, replicaID := range pa.Replicas {
				if replicaID == m.broker.nodeID {
					continue
				}

				inISR := false
				for _, id := range newISR {
					if id == replicaID {
						inISR = true
						break
					}
				}
				if inISR {
					continue
				}

				// Check if caught up
				replicaLEO, ok := state.ReplicaLEOs[replicaID]
				if ok && replicaLEO >= state.LEO {
					log.Printf("ISR expand: adding broker %d to %s-%d (LEO: %d)",
						replicaID, topic, pa.Partition, replicaLEO)
					newISR = append(newISR, replicaID)
					changed = true
				}
			}

			if changed {
				state.ISR = newISR
			}

			state.mu.Unlock()

			if changed && m.broker.controllerClient != nil {
				if err := m.broker.controllerClient.ReportISRChange(topic, pa.Partition, newISR); err != nil {
					log.Printf("report ISR change: %v", err)
				}
			}
		}
	}
}

// Stop stops the ISR manager.
func (m *ISRManager) Stop() {
	close(m.quit)
	m.wg.Wait()
}
