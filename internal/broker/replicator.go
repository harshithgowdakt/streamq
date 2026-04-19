package broker

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/harshithgowda/streamq/internal/cluster"
	"github.com/harshithgowda/streamq/internal/protocol"
)

// ReplicaFetcher fetches data from a leader broker for follower partitions.
type ReplicaFetcher struct {
	broker     *Broker
	leaderAddr string
	leaderID   cluster.BrokerID

	mu         sync.Mutex
	partitions map[string]*fetchPartition // "topic-partID" -> partition info
	conn       net.Conn
	stop       chan struct{}
	wg         sync.WaitGroup
	started    bool
}

type fetchPartition struct {
	topic     string
	partition *Partition
}

// NewReplicaFetcher creates a new ReplicaFetcher.
func NewReplicaFetcher(b *Broker, leaderAddr string, leaderID cluster.BrokerID) *ReplicaFetcher {
	return &ReplicaFetcher{
		broker:     b,
		leaderAddr: leaderAddr,
		leaderID:   leaderID,
		partitions: make(map[string]*fetchPartition),
		stop:       make(chan struct{}),
	}
}

// AddPartition adds a partition to fetch.
func (rf *ReplicaFetcher) AddPartition(part *Partition, topic string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	key := fmt.Sprintf("%s-%d", topic, part.ID)
	rf.partitions[key] = &fetchPartition{
		topic:     topic,
		partition: part,
	}
}

// Start begins the fetch loop.
func (rf *ReplicaFetcher) Start() {
	rf.mu.Lock()
	if rf.started {
		rf.mu.Unlock()
		return
	}
	rf.started = true
	rf.mu.Unlock()

	rf.wg.Add(1)
	go rf.fetchLoop()
}

// Stop stops the fetch loop.
func (rf *ReplicaFetcher) Stop() {
	select {
	case <-rf.stop:
		return // already stopped
	default:
	}
	close(rf.stop)
	if rf.conn != nil {
		rf.conn.Close()
	}
	rf.wg.Wait()
}

func (rf *ReplicaFetcher) fetchLoop() {
	defer rf.wg.Done()

	for {
		select {
		case <-rf.stop:
			return
		default:
		}

		if err := rf.doFetch(); err != nil {
			select {
			case <-rf.stop:
				return
			default:
				log.Printf("replica fetch from %s failed: %v", rf.leaderAddr, err)
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func (rf *ReplicaFetcher) doFetch() error {
	if rf.conn == nil {
		conn, err := net.DialTimeout("tcp", rf.leaderAddr, 5*time.Second)
		if err != nil {
			return fmt.Errorf("connect to leader: %w", err)
		}
		rf.conn = conn
	}

	rf.mu.Lock()
	parts := make(map[string]*fetchPartition, len(rf.partitions))
	for k, v := range rf.partitions {
		parts[k] = v
	}
	rf.mu.Unlock()

	if len(parts) == 0 {
		time.Sleep(500 * time.Millisecond)
		return nil
	}

	// Build FetchRequest
	topicMap := make(map[string][]protocol.FetchPartitionData)
	for _, fp := range parts {
		topicMap[fp.topic] = append(topicMap[fp.topic], protocol.FetchPartitionData{
			Partition:   fp.partition.ID,
			FetchOffset: fp.partition.Log.NextOffset(),
			MaxBytes:    1024 * 1024, // 1MB
		})
	}

	var topics []protocol.FetchTopicData
	for topic, partitions := range topicMap {
		topics = append(topics, protocol.FetchTopicData{
			Topic:      topic,
			Partitions: partitions,
		})
	}

	fetchReq := &protocol.FetchRequest{
		Header: protocol.RequestHeader{
			APIKey:        protocol.APIKeyFetch,
			APIVersion:    4,
			CorrelationID: int32(time.Now().UnixNano() & 0x7FFFFFFF),
			ClientID:      fmt.Sprintf("replica-%d", rf.broker.GetNodeID()),
		},
		ReplicaID: rf.broker.GetNodeID(),
		MaxWaitMs: 500,
		MinBytes:  1,
		MaxBytes:  10 * 1024 * 1024,
		Topics:    topics,
	}

	// Encode and send
	reqBytes, err := protocol.EncodeRequest(fetchReq)
	if err != nil {
		rf.conn.Close()
		rf.conn = nil
		return fmt.Errorf("encode fetch: %w", err)
	}

	frame := make([]byte, 4+len(reqBytes))
	binary.BigEndian.PutUint32(frame, uint32(len(reqBytes)))
	copy(frame[4:], reqBytes)

	rf.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := rf.conn.Write(frame); err != nil {
		rf.conn.Close()
		rf.conn = nil
		return fmt.Errorf("send fetch: %w", err)
	}

	// Read response
	rf.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	var respLenBuf [4]byte
	if _, err := io.ReadFull(rf.conn, respLenBuf[:]); err != nil {
		rf.conn.Close()
		rf.conn = nil
		return fmt.Errorf("read resp len: %w", err)
	}

	respLen := binary.BigEndian.Uint32(respLenBuf[:])
	if respLen > 100*1024*1024 {
		rf.conn.Close()
		rf.conn = nil
		return fmt.Errorf("response too large: %d", respLen)
	}

	respBody := make([]byte, respLen)
	if _, err := io.ReadFull(rf.conn, respBody); err != nil {
		rf.conn.Close()
		rf.conn = nil
		return fmt.Errorf("read resp body: %w", err)
	}

	respI, err := protocol.DecodeResponse(protocol.APIKeyFetch, 4, respBody)
	if err != nil {
		return fmt.Errorf("decode fetch resp: %w", err)
	}

	fetchResp, ok := respI.(*protocol.FetchResponse)
	if !ok {
		return fmt.Errorf("unexpected response type: %T", respI)
	}

	// Apply fetched data
	for _, topicResp := range fetchResp.Topics {
		for _, partResp := range topicResp.Partitions {
			if partResp.ErrorCode != protocol.ErrNone {
				continue
			}
			if len(partResp.RecordBatches) == 0 {
				continue
			}

			partition, err := rf.broker.TopicManager.GetPartition(topicResp.Topic, partResp.Partition)
			if err != nil {
				continue
			}

			// Parse Kafka RecordBatch and append as replica
			batches, err := protocol.KafkaRecordBatchToInternal(partResp.RecordBatches)
			if err != nil {
				log.Printf("replica decode batches for %s-%d: %v", topicResp.Topic, partResp.Partition, err)
				continue
			}

			for _, batch := range batches {
				if _, err := partition.Log.AppendReplica(batch); err != nil {
					log.Printf("replica append %s-%d: %v", topicResp.Topic, partResp.Partition, err)
					break
				}
			}
		}
	}

	return nil
}
