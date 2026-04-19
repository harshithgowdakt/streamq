package cluster

import "time"

// BrokerID uniquely identifies a broker in the cluster.
type BrokerID int32

// PartitionAssignment describes the replica placement for a single partition.
type PartitionAssignment struct {
	Topic     string     `json:"topic"`
	Partition int32      `json:"partition"`
	Leader    BrokerID   `json:"leader"`
	Replicas  []BrokerID `json:"replicas"`
	ISR       []BrokerID `json:"isr"`
	Epoch     int32      `json:"epoch"`
}

// BrokerInfo describes a broker in the cluster.
type BrokerInfo struct {
	ID       BrokerID  `json:"id"`
	Host     string    `json:"host"`
	Port     int32     `json:"port"`
	LastSeen time.Time `json:"last_seen"`
}

// ClusterMetadata is the full cluster state managed by the controller via Raft.
type ClusterMetadata struct {
	Brokers      map[BrokerID]BrokerInfo          `json:"brokers"`
	Assignments  map[string][]PartitionAssignment  `json:"assignments"` // topic -> partitions
	NextBrokerID BrokerID                          `json:"next_broker_id"`
}

// NewClusterMetadata creates an empty ClusterMetadata.
func NewClusterMetadata() *ClusterMetadata {
	return &ClusterMetadata{
		Brokers:      make(map[BrokerID]BrokerInfo),
		Assignments:  make(map[string][]PartitionAssignment),
		NextBrokerID: 0,
	}
}

// Clone returns a deep copy of ClusterMetadata.
func (cm *ClusterMetadata) Clone() *ClusterMetadata {
	c := &ClusterMetadata{
		Brokers:      make(map[BrokerID]BrokerInfo, len(cm.Brokers)),
		Assignments:  make(map[string][]PartitionAssignment, len(cm.Assignments)),
		NextBrokerID: cm.NextBrokerID,
	}
	for id, b := range cm.Brokers {
		c.Brokers[id] = b
	}
	for topic, parts := range cm.Assignments {
		cp := make([]PartitionAssignment, len(parts))
		for i, p := range parts {
			cp[i] = PartitionAssignment{
				Topic:     p.Topic,
				Partition: p.Partition,
				Leader:    p.Leader,
				Epoch:     p.Epoch,
				Replicas:  append([]BrokerID(nil), p.Replicas...),
				ISR:       append([]BrokerID(nil), p.ISR...),
			}
		}
		c.Assignments[topic] = cp
	}
	return c
}
