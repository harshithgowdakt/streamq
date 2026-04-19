package cluster

// RPC message type IDs for the controller-broker protocol.
const (
	MsgRegisterBroker    byte = 1
	MsgRegisterBrokerResp byte = 2
	MsgMetadataUpdate    byte = 3
	MsgISRChange         byte = 4
	MsgBrokerHeartbeat   byte = 5
	MsgHeartbeatResp     byte = 6
	MsgCreateTopicReq    byte = 7
	MsgCreateTopicResp   byte = 8
)

// RegisterBrokerReq is sent by a broker to register itself with the controller.
type RegisterBrokerReq struct {
	Host string `json:"host"`
	Port int32  `json:"port"`
}

// RegisterBrokerResp is the controller's response to a broker registration.
type RegisterBrokerResp struct {
	BrokerID BrokerID         `json:"broker_id"`
	Metadata *ClusterMetadata `json:"metadata"`
	Error    string           `json:"error,omitempty"`
}

// MetadataUpdateMsg is pushed by the controller to all brokers on metadata change.
type MetadataUpdateMsg struct {
	Metadata *ClusterMetadata `json:"metadata"`
}

// ISRChangeReq is sent by a leader broker to report ISR changes.
type ISRChangeReq struct {
	Topic     string     `json:"topic"`
	Partition int32      `json:"partition"`
	NewISR    []BrokerID `json:"new_isr"`
}

// BrokerHeartbeatReq is sent periodically by brokers.
type BrokerHeartbeatReq struct {
	BrokerID BrokerID `json:"broker_id"`
}

// HeartbeatResp is the controller's response to a heartbeat.
type HeartbeatResp struct {
	OK bool `json:"ok"`
}

// CreateTopicRPCReq is forwarded from broker to controller for topic creation.
type CreateTopicRPCReq struct {
	Topic             string `json:"topic"`
	NumPartitions     int32  `json:"num_partitions"`
	ReplicationFactor int16  `json:"replication_factor"`
}

// CreateTopicRPCResp is the controller's response to a topic creation request.
type CreateTopicRPCResp struct {
	Error string `json:"error,omitempty"`
}
