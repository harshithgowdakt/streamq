package controller

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/harshithgowda/streamq/internal/cluster"
)

// Command types for the Raft FSM.
const (
	CmdRegisterBroker = "register_broker"
	CmdCreateTopic    = "create_topic"
	CmdUpdateISR      = "update_isr"
	CmdBrokerTimeout  = "broker_timeout"
	CmdHeartbeat      = "heartbeat"
)

// FSMCommand is the Raft log entry payload.
type FSMCommand struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// RegisterBrokerData is the payload for CmdRegisterBroker.
type RegisterBrokerData struct {
	Host string `json:"host"`
	Port int32  `json:"port"`
}

// CreateTopicData is the payload for CmdCreateTopic.
type CreateTopicData struct {
	Topic             string `json:"topic"`
	NumPartitions     int32  `json:"num_partitions"`
	ReplicationFactor int16  `json:"replication_factor"`
}

// UpdateISRData is the payload for CmdUpdateISR.
type UpdateISRData struct {
	Topic     string           `json:"topic"`
	Partition int32            `json:"partition"`
	NewISR    []cluster.BrokerID `json:"new_isr"`
}

// BrokerTimeoutData is the payload for CmdBrokerTimeout.
type BrokerTimeoutData struct {
	BrokerID cluster.BrokerID `json:"broker_id"`
}

// HeartbeatData is the payload for CmdHeartbeat.
type HeartbeatData struct {
	BrokerID cluster.BrokerID `json:"broker_id"`
	Time     time.Time        `json:"time"`
}

// FSM implements raft.FSM for cluster metadata.
type FSM struct {
	mu       sync.RWMutex
	metadata *cluster.ClusterMetadata

	// onApply is called after each successful Apply with the new metadata.
	onApply func(*cluster.ClusterMetadata)
}

// NewFSM creates a new FSM.
func NewFSM() *FSM {
	return &FSM{
		metadata: cluster.NewClusterMetadata(),
	}
}

// SetOnApply sets a callback invoked after each FSM apply.
func (f *FSM) SetOnApply(fn func(*cluster.ClusterMetadata)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onApply = fn
}

// GetMetadata returns a clone of the current metadata.
func (f *FSM) GetMetadata() *cluster.ClusterMetadata {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.metadata.Clone()
}

// Apply applies a Raft log entry to the FSM.
func (f *FSM) Apply(l *raft.Log) interface{} {
	var cmd FSMCommand
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return fmt.Errorf("unmarshal command: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	var result interface{}
	switch cmd.Type {
	case CmdRegisterBroker:
		result = f.applyRegisterBroker(cmd.Data)
	case CmdCreateTopic:
		result = f.applyCreateTopic(cmd.Data)
	case CmdUpdateISR:
		result = f.applyUpdateISR(cmd.Data)
	case CmdBrokerTimeout:
		result = f.applyBrokerTimeout(cmd.Data)
	case CmdHeartbeat:
		result = f.applyHeartbeat(cmd.Data)
	default:
		result = fmt.Errorf("unknown command type: %s", cmd.Type)
	}

	if f.onApply != nil {
		f.onApply(f.metadata.Clone())
	}

	return result
}

func (f *FSM) applyRegisterBroker(data json.RawMessage) interface{} {
	var d RegisterBrokerData
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}

	id := f.metadata.NextBrokerID
	f.metadata.Brokers[id] = cluster.BrokerInfo{
		ID:       id,
		Host:     d.Host,
		Port:     d.Port,
		LastSeen: time.Now(),
	}
	f.metadata.NextBrokerID++
	return id
}

func (f *FSM) applyCreateTopic(data json.RawMessage) interface{} {
	var d CreateTopicData
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}

	if _, exists := f.metadata.Assignments[d.Topic]; exists {
		return fmt.Errorf("topic %q already exists", d.Topic)
	}

	// Collect live broker IDs
	brokerIDs := make([]cluster.BrokerID, 0, len(f.metadata.Brokers))
	for id := range f.metadata.Brokers {
		brokerIDs = append(brokerIDs, id)
	}

	if len(brokerIDs) == 0 {
		return fmt.Errorf("no brokers available")
	}

	rf := int(d.ReplicationFactor)
	if rf <= 0 {
		rf = len(brokerIDs)
		if rf > 3 {
			rf = 3
		}
	}
	if rf > len(brokerIDs) {
		rf = len(brokerIDs)
	}

	// Sort broker IDs for deterministic placement
	sortBrokerIDs(brokerIDs)

	assignments := make([]cluster.PartitionAssignment, d.NumPartitions)
	for i := int32(0); i < d.NumPartitions; i++ {
		replicas := make([]cluster.BrokerID, rf)
		for j := 0; j < rf; j++ {
			idx := (int(i) + j) % len(brokerIDs)
			replicas[j] = brokerIDs[idx]
		}
		assignments[i] = cluster.PartitionAssignment{
			Topic:     d.Topic,
			Partition: i,
			Leader:    replicas[0],
			Replicas:  replicas,
			ISR:       append([]cluster.BrokerID(nil), replicas...),
			Epoch:     1,
		}
	}

	f.metadata.Assignments[d.Topic] = assignments
	return nil
}

func (f *FSM) applyUpdateISR(data json.RawMessage) interface{} {
	var d UpdateISRData
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}

	parts, ok := f.metadata.Assignments[d.Topic]
	if !ok {
		return fmt.Errorf("topic %q not found", d.Topic)
	}
	if int(d.Partition) >= len(parts) {
		return fmt.Errorf("partition %d out of range", d.Partition)
	}

	p := &parts[d.Partition]
	p.ISR = append([]cluster.BrokerID(nil), d.NewISR...)

	// If current leader is not in ISR, elect new leader
	leaderInISR := false
	for _, id := range p.ISR {
		if id == p.Leader {
			leaderInISR = true
			break
		}
	}
	if !leaderInISR && len(p.ISR) > 0 {
		p.Leader = p.ISR[0]
		p.Epoch++
	}

	return nil
}

func (f *FSM) applyBrokerTimeout(data json.RawMessage) interface{} {
	var d BrokerTimeoutData
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}

	delete(f.metadata.Brokers, d.BrokerID)

	// Remove broker from all ISRs, elect new leaders if needed
	for topic, parts := range f.metadata.Assignments {
		for i := range parts {
			p := &f.metadata.Assignments[topic][i]
			newISR := make([]cluster.BrokerID, 0, len(p.ISR))
			for _, id := range p.ISR {
				if id != d.BrokerID {
					newISR = append(newISR, id)
				}
			}
			p.ISR = newISR

			if p.Leader == d.BrokerID {
				if len(p.ISR) > 0 {
					p.Leader = p.ISR[0]
				} else {
					p.Leader = -1 // no leader
				}
				p.Epoch++
			}
		}
	}

	return nil
}

func (f *FSM) applyHeartbeat(data json.RawMessage) interface{} {
	var d HeartbeatData
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}

	if b, ok := f.metadata.Brokers[d.BrokerID]; ok {
		b.LastSeen = d.Time
		f.metadata.Brokers[d.BrokerID] = b
	}

	return nil
}

// Snapshot returns a snapshot of the FSM state.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	data, err := json.Marshal(f.metadata)
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{data: data}, nil
}

// Restore restores the FSM from a snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var metadata cluster.ClusterMetadata
	if err := json.NewDecoder(rc).Decode(&metadata); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.metadata = &metadata
	return nil
}

type fsmSnapshot struct {
	data []byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

// sortBrokerIDs sorts a slice of BrokerIDs in ascending order.
func sortBrokerIDs(ids []cluster.BrokerID) {
	for i := 1; i < len(ids); i++ {
		key := ids[i]
		j := i - 1
		for j >= 0 && ids[j] > key {
			ids[j+1] = ids[j]
			j--
		}
		ids[j+1] = key
	}
}
