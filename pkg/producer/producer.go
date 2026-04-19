package producer

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/harshithgowda/streamq/internal/log"
	"github.com/harshithgowda/streamq/internal/protocol"
)

// Config holds producer configuration.
type Config struct {
	BrokerAddr   string
	BatchSize    int           // max records per batch before flush
	LingerTime   time.Duration // max time to wait before flush
	ClientID     string
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		BatchSize:  100,
		LingerTime: 10 * time.Millisecond,
		ClientID:   "streamq-producer",
	}
}

// Message is a message to be sent.
type Message struct {
	Topic     string
	Partition int32
	Key       []byte
	Value     []byte
}

// Result is the result of a produce operation.
type Result struct {
	Offset int64
	Err    error
}

type pendingRecord struct {
	msg    Message
	result chan Result
}

// partitionKey identifies a topic-partition pair.
type partitionKey struct {
	topic     string
	partition int32
}

// Producer batches messages and sends them to the broker.
type Producer struct {
	config  Config
	conn    net.Conn
	mu      sync.Mutex
	closed  atomic.Bool
	corrID  int32

	// Accumulator: per topic-partition buffer
	accMu   sync.Mutex
	acc     map[partitionKey][]pendingRecord
	flushCh chan struct{}
	wg      sync.WaitGroup
}

// NewProducer creates and connects a new Producer.
func NewProducer(config Config) (*Producer, error) {
	conn, err := net.DialTimeout("tcp", config.BrokerAddr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	p := &Producer{
		config:  config,
		conn:    conn,
		acc:     make(map[partitionKey][]pendingRecord),
		flushCh: make(chan struct{}, 1),
	}

	// Start background flusher
	p.wg.Add(1)
	go p.flusher()

	return p, nil
}

// Send sends a message and blocks until acknowledged.
func (p *Producer) Send(msg Message) (int64, error) {
	if p.closed.Load() {
		return 0, fmt.Errorf("producer is closed")
	}

	result := make(chan Result, 1)
	key := partitionKey{msg.Topic, msg.Partition}

	p.accMu.Lock()
	p.acc[key] = append(p.acc[key], pendingRecord{msg: msg, result: result})
	shouldFlush := len(p.acc[key]) >= p.config.BatchSize
	p.accMu.Unlock()

	if shouldFlush {
		select {
		case p.flushCh <- struct{}{}:
		default:
		}
	}

	r := <-result
	return r.Offset, r.Err
}

func (p *Producer) flusher() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.LingerTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.flush()
		case <-p.flushCh:
			p.flush()
		}

		if p.closed.Load() {
			p.flush() // final flush
			return
		}
	}
}

func (p *Producer) flush() {
	p.accMu.Lock()
	if len(p.acc) == 0 {
		p.accMu.Unlock()
		return
	}
	// Swap out the accumulator
	acc := p.acc
	p.acc = make(map[partitionKey][]pendingRecord)
	p.accMu.Unlock()

	// Group by topic for the produce request
	type partitionBatch struct {
		records []pendingRecord
		batch   *log.RecordBatch
	}
	topicMap := make(map[string]map[int32]*partitionBatch)

	for key, records := range acc {
		if _, ok := topicMap[key.topic]; !ok {
			topicMap[key.topic] = make(map[int32]*partitionBatch)
		}

		batch := &log.RecordBatch{
			Records: make([]log.Record, len(records)),
		}
		for i, r := range records {
			batch.Records[i] = log.Record{Key: r.msg.Key, Value: r.msg.Value}
		}

		topicMap[key.topic][key.partition] = &partitionBatch{
			records: records,
			batch:   batch,
		}
	}

	// Build produce request
	req := &protocol.ProduceRequest{
		Header: protocol.RequestHeader{
			APIKey:        protocol.APIKeyProduce,
			CorrelationID: p.nextCorrID(),
			ClientID:      p.config.ClientID,
		},
		TimeoutMs: 5000,
	}

	for topic, partitions := range topicMap {
		topicData := protocol.ProduceTopicData{Topic: topic}
		for partID, pb := range partitions {
			topicData.Partitions = append(topicData.Partitions, protocol.ProducePartitionData{
				Partition: partID,
				Records:   pb.batch.Encode(),
			})
		}
		req.Topics = append(req.Topics, topicData)
	}

	// Send request and get response
	resp, err := p.sendRequest(req)
	if err != nil {
		// Notify all pending records of the error
		for _, records := range acc {
			for _, r := range records {
				r.result <- Result{Err: err}
			}
		}
		return
	}

	produceResp := resp.(*protocol.ProduceResponse)

	// Distribute results
	for _, topicResp := range produceResp.Topics {
		for _, partResp := range topicResp.Partitions {
			key := partitionKey{topicResp.Topic, partResp.Partition}
			pb := topicMap[topicResp.Topic][partResp.Partition]
			if pb == nil {
				continue
			}

			if partResp.ErrorCode != protocol.ErrNone {
				for _, r := range pb.records {
					r.result <- Result{Err: fmt.Errorf("produce error: %d", partResp.ErrorCode)}
				}
			} else {
				for i, r := range pb.records {
					r.result <- Result{Offset: partResp.BaseOffset + int64(i)}
				}
			}
			delete(acc, key)
		}
	}

	// Any remaining records without a response
	for _, records := range acc {
		for _, r := range records {
			r.result <- Result{Err: fmt.Errorf("no response for partition")}
		}
	}
}

func (p *Producer) sendRequest(req interface{}) (interface{}, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := protocol.EncodeRequest(req)
	if err != nil {
		return nil, err
	}

	// Write frame
	frame := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(frame, uint32(len(data)))
	copy(frame[4:], data)
	if _, err := p.conn.Write(frame); err != nil {
		return nil, err
	}

	// Read response
	var respLenBuf [4]byte
	if _, err := io.ReadFull(p.conn, respLenBuf[:]); err != nil {
		return nil, err
	}
	respLen := binary.BigEndian.Uint32(respLenBuf[:])
	respBody := make([]byte, respLen)
	if _, err := io.ReadFull(p.conn, respBody); err != nil {
		return nil, err
	}

	// Determine API key from the request
	var apiKey int16
	switch req.(type) {
	case *protocol.ProduceRequest:
		apiKey = protocol.APIKeyProduce
	case *protocol.FetchRequest:
		apiKey = protocol.APIKeyFetch
	case *protocol.MetadataRequest:
		apiKey = protocol.APIKeyMetadata
	case *protocol.CreateTopicsRequest:
		apiKey = protocol.APIKeyCreateTopics
	}

	return protocol.DecodeResponse(apiKey, respBody)
}

func (p *Producer) nextCorrID() int32 {
	return atomic.AddInt32(&p.corrID, 1)
}

// Close flushes pending messages and closes the connection.
func (p *Producer) Close() error {
	p.closed.Store(true)
	select {
	case p.flushCh <- struct{}{}:
	default:
	}
	p.wg.Wait()
	return p.conn.Close()
}
