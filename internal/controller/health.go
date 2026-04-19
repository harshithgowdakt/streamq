package controller

import (
	"log"
	"sync"
	"time"

	"github.com/harshithgowda/streamq/internal/cluster"
)

// HealthChecker monitors broker liveness and proposes timeout commands.
type HealthChecker struct {
	controller *Controller
	timeout    time.Duration

	quit chan struct{}
	wg   sync.WaitGroup
}

// NewHealthChecker creates a new HealthChecker.
func NewHealthChecker(ctrl *Controller, timeout time.Duration) *HealthChecker {
	return &HealthChecker{
		controller: ctrl,
		timeout:    timeout,
		quit:       make(chan struct{}),
	}
}

// Start begins the health check loop.
func (h *HealthChecker) Start() {
	h.wg.Add(1)
	go h.loop()
}

func (h *HealthChecker) loop() {
	defer h.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.check()
		case <-h.quit:
			return
		}
	}
}

func (h *HealthChecker) check() {
	if !h.controller.IsLeader() {
		return
	}

	meta := h.controller.GetMetadata()
	now := time.Now()

	for id, broker := range meta.Brokers {
		if now.Sub(broker.LastSeen) > h.timeout {
			log.Printf("broker %d timed out (last seen: %v)", id, broker.LastSeen)
			if err := h.controller.TimeoutBroker(id); err != nil {
				log.Printf("timeout broker %d: %v", id, err)
			}
		}
	}
}

// Stop stops the health checker.
func (h *HealthChecker) Stop() {
	close(h.quit)
	h.wg.Wait()
}

// GetBrokerLastSeen returns the last seen times for all brokers.
func (h *HealthChecker) GetBrokerLastSeen() map[cluster.BrokerID]time.Time {
	meta := h.controller.GetMetadata()
	result := make(map[cluster.BrokerID]time.Time, len(meta.Brokers))
	for id, b := range meta.Brokers {
		result[id] = b.LastSeen
	}
	return result
}
