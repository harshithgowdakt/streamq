package consumer

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/harshithgowda/streamq/internal/log"
	"github.com/harshithgowda/streamq/internal/protocol"
)

// Message is a consumed message.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

// Config holds consumer configuration.
type Config struct {
	BrokerAddr string
	ClientID   string
	MaxBytes   int32
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		ClientID: "streamq-consumer",
		MaxBytes: 1024 * 1024,
	}
}

// Subscription tracks a topic-partition subscription.
type Subscription struct {
	Topic     string
	Partition int32
	Offset    int64
}

// Consumer fetches messages from the broker.
type Consumer struct {
	config        Config
	conn          net.Conn
	subscriptions []Subscription
	corrID        int32
}

// NewConsumer creates and connects a new Consumer.
func NewConsumer(config Config) (*Consumer, error) {
	conn, err := net.DialTimeout("tcp", config.BrokerAddr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	return &Consumer{
		config: config,
		conn:   conn,
	}, nil
}

// Subscribe adds a topic-partition subscription starting at the given offset.
func (c *Consumer) Subscribe(topic string, partition int32, offset int64) {
	c.subscriptions = append(c.subscriptions, Subscription{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
	})
}

// Poll sends fetch requests for all subscriptions and returns messages.
func (c *Consumer) Poll() ([]Message, error) {
	if len(c.subscriptions) == 0 {
		return nil, nil
	}

	// Build fetch request grouping subscriptions by topic
	topicMap := make(map[string][]protocol.FetchPartitionData)
	subIdx := make(map[string]map[int32]int) // topic -> partition -> subscription index

	for i, sub := range c.subscriptions {
		topicMap[sub.Topic] = append(topicMap[sub.Topic], protocol.FetchPartitionData{
			Partition:   sub.Partition,
			FetchOffset: sub.Offset,
			MaxBytes:    c.config.MaxBytes,
		})
		if subIdx[sub.Topic] == nil {
			subIdx[sub.Topic] = make(map[int32]int)
		}
		subIdx[sub.Topic][sub.Partition] = i
	}

	req := &protocol.FetchRequest{
		Header: protocol.RequestHeader{
			APIKey:        protocol.APIKeyFetch,
			CorrelationID: c.nextCorrID(),
			ClientID:      c.config.ClientID,
		},
		MaxBytes: c.config.MaxBytes,
	}

	for topic, partitions := range topicMap {
		req.Topics = append(req.Topics, protocol.FetchTopicData{
			Topic:      topic,
			Partitions: partitions,
		})
	}

	// Send and receive
	resp, err := c.sendRequest(req)
	if err != nil {
		return nil, err
	}

	fetchResp := resp.(*protocol.FetchResponse)

	var messages []Message

	for _, topicResp := range fetchResp.Topics {
		for _, partResp := range topicResp.Partitions {
			if partResp.ErrorCode != protocol.ErrNone {
				continue
			}

			if len(partResp.RecordBatches) == 0 {
				continue
			}

			// Decode all batches from the raw bytes
			data := partResp.RecordBatches
			for len(data) >= 12 { // minimum: BaseOffset(8) + BatchLength(4)
				batchLen := int32(binary.BigEndian.Uint32(data[8:12]))
				totalSize := 12 + int(batchLen)
				if totalSize > len(data) {
					break
				}

				batch, err := log.DecodeRecordBatch(data[:totalSize])
				if err != nil {
					break
				}

				for i, rec := range batch.Records {
					messages = append(messages, Message{
						Topic:     topicResp.Topic,
						Partition: partResp.Partition,
						Offset:    batch.BaseOffset + int64(i),
						Key:       rec.Key,
						Value:     rec.Value,
					})
				}

				// Advance offset for this subscription
				if idx, ok := subIdx[topicResp.Topic][partResp.Partition]; ok {
					c.subscriptions[idx].Offset = batch.BaseOffset + int64(len(batch.Records))
				}

				data = data[totalSize:]
			}
		}
	}

	return messages, nil
}

func (c *Consumer) sendRequest(req interface{}) (interface{}, error) {
	data, err := protocol.EncodeRequest(req)
	if err != nil {
		return nil, err
	}

	frame := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(frame, uint32(len(data)))
	copy(frame[4:], data)
	if _, err := c.conn.Write(frame); err != nil {
		return nil, err
	}

	var respLenBuf [4]byte
	if _, err := io.ReadFull(c.conn, respLenBuf[:]); err != nil {
		return nil, err
	}
	respLen := binary.BigEndian.Uint32(respLenBuf[:])
	respBody := make([]byte, respLen)
	if _, err := io.ReadFull(c.conn, respBody); err != nil {
		return nil, err
	}

	var apiKey int16
	switch req.(type) {
	case *protocol.FetchRequest:
		apiKey = protocol.APIKeyFetch
	case *protocol.MetadataRequest:
		apiKey = protocol.APIKeyMetadata
	}

	return protocol.DecodeResponse(apiKey, respBody)
}

func (c *Consumer) nextCorrID() int32 {
	return atomic.AddInt32(&c.corrID, 1)
}

// Close closes the consumer connection.
func (c *Consumer) Close() error {
	return c.conn.Close()
}
