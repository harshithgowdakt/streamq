package streamq_test

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/harshithgowda/streamq/internal/broker"
	"github.com/harshithgowda/streamq/internal/protocol"
	"github.com/harshithgowda/streamq/internal/server"
	"github.com/harshithgowda/streamq/pkg/consumer"
	"github.com/harshithgowda/streamq/pkg/producer"
)

func sendRawRequest(conn net.Conn, req interface{}) ([]byte, error) {
	data, err := protocol.EncodeRequest(req)
	if err != nil {
		return nil, err
	}
	frame := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(frame, uint32(len(data)))
	copy(frame[4:], data)
	if _, err := conn.Write(frame); err != nil {
		return nil, err
	}
	var respLenBuf [4]byte
	if _, err := io.ReadFull(conn, respLenBuf[:]); err != nil {
		return nil, err
	}
	respLen := binary.BigEndian.Uint32(respLenBuf[:])
	respBody := make([]byte, respLen)
	if _, err := io.ReadFull(conn, respBody); err != nil {
		return nil, err
	}
	return respBody, nil
}

func TestEndToEnd(t *testing.T) {
	cfg := broker.Config{
		DataDir:           t.TempDir(),
		DefaultPartitions: 1,
		MaxSegmentBytes:   1024 * 1024,
		Addr:              ":0",
	}
	b := broker.NewBroker(cfg)
	srv := server.NewServer(b)
	if err := srv.Start(":0"); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()
	defer b.Close()

	addr := srv.Addr().String()

	// Step 1: Create topic via raw protocol
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}

	respBytes, err := sendRawRequest(conn, &protocol.CreateTopicsRequest{
		Header: protocol.RequestHeader{
			APIKey:        protocol.APIKeyCreateTopics,
			CorrelationID: 1,
			ClientID:      "integration-test",
		},
		Topics: []protocol.CreateTopicRequest{
			{Topic: "e2e-topic", NumPartitions: 2},
		},
		TimeoutMs: 5000,
	})
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	createResp, err := protocol.DecodeResponse(protocol.APIKeyCreateTopics, 0, respBytes)
	if err != nil {
		t.Fatal(err)
	}
	cr := createResp.(*protocol.CreateTopicsResponse)
	if cr.Topics[0].ErrorCode != protocol.ErrNone {
		t.Fatalf("create topic error: %d", cr.Topics[0].ErrorCode)
	}

	// Step 2: Produce messages using producer client
	p, err := producer.NewProducer(producer.Config{
		BrokerAddr:     addr,
		BatchSize:      100,
		LingerTime:     10 * time.Millisecond,
		RequestTimeout: 5 * time.Second,
		ClientID:       "e2e-producer",
	})
	if err != nil {
		t.Fatal(err)
	}

	numMessages := 10
	for i := 0; i < numMessages; i++ {
		offset, err := p.Send(producer.Message{
			Topic:     "e2e-topic",
			Partition: 0,
			Key:       []byte(fmt.Sprintf("key-%d", i)),
			Value:     []byte(fmt.Sprintf("value-%d", i)),
		})
		if err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		if offset < 0 {
			t.Fatalf("invalid offset %d for message %d", offset, i)
		}
	}
	p.Close()

	// Step 3: Consume messages using consumer client
	c, err := consumer.NewConsumer(consumer.Config{
		BrokerAddr: addr,
		ClientID:   "e2e-consumer",
		MaxBytes:   1024 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.Subscribe("e2e-topic", 0, 0)

	messages, err := c.Poll()
	if err != nil {
		t.Fatal(err)
	}

	if len(messages) != numMessages {
		t.Fatalf("expected %d messages, got %d", numMessages, len(messages))
	}

	for i, msg := range messages {
		expectedKey := fmt.Sprintf("key-%d", i)
		expectedValue := fmt.Sprintf("value-%d", i)
		if string(msg.Key) != expectedKey {
			t.Errorf("message %d key: got %q, want %q", i, msg.Key, expectedKey)
		}
		if string(msg.Value) != expectedValue {
			t.Errorf("message %d value: got %q, want %q", i, msg.Value, expectedValue)
		}
		if msg.Offset != int64(i) {
			t.Errorf("message %d offset: got %d, want %d", i, msg.Offset, i)
		}
	}

	// Step 4: Metadata check
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	metaBytes, err := sendRawRequest(conn2, &protocol.MetadataRequest{
		Header: protocol.RequestHeader{
			APIKey:        protocol.APIKeyMetadata,
			CorrelationID: 100,
			ClientID:      "e2e-test",
		},
		Topics: []string{"e2e-topic"},
	})
	if err != nil {
		t.Fatal(err)
	}

	metaResp, err := protocol.DecodeResponse(protocol.APIKeyMetadata, 0, metaBytes)
	if err != nil {
		t.Fatal(err)
	}
	mr := metaResp.(*protocol.MetadataResponse)
	if len(mr.Topics) != 1 {
		t.Fatalf("expected 1 topic in metadata, got %d", len(mr.Topics))
	}
	if mr.Topics[0].Topic != "e2e-topic" {
		t.Errorf("topic name: %q", mr.Topics[0].Topic)
	}
	if len(mr.Topics[0].Partitions) != 2 {
		t.Errorf("expected 2 partitions, got %d", len(mr.Topics[0].Partitions))
	}

	t.Logf("E2E test passed: %d messages produced and consumed successfully", numMessages)
}

func TestEndToEndMultiPartition(t *testing.T) {
	cfg := broker.Config{
		DataDir:           t.TempDir(),
		DefaultPartitions: 1,
		MaxSegmentBytes:   1024 * 1024,
		Addr:              ":0",
	}
	b := broker.NewBroker(cfg)
	srv := server.NewServer(b)
	if err := srv.Start(":0"); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()
	defer b.Close()

	addr := srv.Addr().String()

	// Create topic with 3 partitions
	b.TopicManager.CreateTopic("multi", 3)

	// Produce to all 3 partitions
	p, err := producer.NewProducer(producer.Config{
		BrokerAddr:     addr,
		BatchSize:      100,
		LingerTime:     10 * time.Millisecond,
		RequestTimeout: 5 * time.Second,
		ClientID:       "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	for part := int32(0); part < 3; part++ {
		for i := 0; i < 5; i++ {
			_, err := p.Send(producer.Message{
				Topic:     "multi",
				Partition: part,
				Value:     []byte(fmt.Sprintf("p%d-msg%d", part, i)),
			})
			if err != nil {
				t.Fatalf("send p%d msg%d: %v", part, i, err)
			}
		}
	}
	p.Close()

	// Consume from each partition independently
	for part := int32(0); part < 3; part++ {
		c, err := consumer.NewConsumer(consumer.Config{
			BrokerAddr: addr,
			ClientID:   "test",
			MaxBytes:   1024 * 1024,
		})
		if err != nil {
			t.Fatal(err)
		}

		c.Subscribe("multi", part, 0)
		messages, err := c.Poll()
		if err != nil {
			t.Fatal(err)
		}
		c.Close()

		if len(messages) != 5 {
			t.Errorf("partition %d: expected 5 messages, got %d", part, len(messages))
		}
	}
}

func TestEndToEndAutoCreateTopic(t *testing.T) {
	cfg := broker.Config{
		DataDir:           t.TempDir(),
		DefaultPartitions: 2,
		MaxSegmentBytes:   1024 * 1024,
		AutoCreateTopics:  true,
		Addr:              ":0",
	}
	b := broker.NewBroker(cfg)
	srv := server.NewServer(b)
	if err := srv.Start(":0"); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()
	defer b.Close()

	addr := srv.Addr().String()

	// Produce to a topic that doesn't exist yet — should auto-create
	p, err := producer.NewProducer(producer.Config{
		BrokerAddr:     addr,
		BatchSize:      100,
		LingerTime:     10 * time.Millisecond,
		RequestTimeout: 5 * time.Second,
		ClientID:       "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	offset, err := p.Send(producer.Message{
		Topic:     "auto-topic",
		Partition: 0,
		Value:     []byte("auto-created-message"),
	})
	if err != nil {
		t.Fatalf("send to auto-created topic: %v", err)
	}
	if offset != 0 {
		t.Errorf("expected offset 0, got %d", offset)
	}
	p.Close()

	// Consume it back
	c, err := consumer.NewConsumer(consumer.Config{
		BrokerAddr: addr,
		ClientID:   "test",
		MaxBytes:   1024 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.Subscribe("auto-topic", 0, 0)
	messages, err := c.Poll()
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}
	if string(messages[0].Value) != "auto-created-message" {
		t.Errorf("value: %q", messages[0].Value)
	}
}
