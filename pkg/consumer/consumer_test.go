package consumer

import (
	"testing"
	"time"

	"github.com/harshithgowda/streamq/internal/broker"
	"github.com/harshithgowda/streamq/internal/server"
	"github.com/harshithgowda/streamq/pkg/producer"
)

func startTestServer(t *testing.T) string {
	t.Helper()

	cfg := broker.Config{
		DataDir:           t.TempDir(),
		DefaultPartitions: 1,
		MaxSegmentBytes:   1024 * 1024,
		AutoCreateTopics:  true,
		Addr:              ":0",
	}
	b := broker.NewBroker(cfg)
	b.TopicManager.CreateTopic("test", 1)

	srv := server.NewServer(b)
	if err := srv.Start(":0"); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		srv.Stop()
		b.Close()
	})

	return srv.Addr().String()
}

func TestConsumerPoll(t *testing.T) {
	addr := startTestServer(t)

	// Produce some messages first
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

	for i := 0; i < 3; i++ {
		_, err := p.Send(producer.Message{
			Topic:     "test",
			Partition: 0,
			Key:       []byte("key"),
			Value:     []byte("message"),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	p.Close()

	// Now consume
	c, err := NewConsumer(Config{
		BrokerAddr: addr,
		ClientID:   "test-consumer",
		MaxBytes:   1024 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.Subscribe("test", 0, 0)

	messages, err := c.Poll()
	if err != nil {
		t.Fatal(err)
	}

	if len(messages) < 3 {
		t.Fatalf("expected at least 3 messages, got %d", len(messages))
	}

	if string(messages[0].Value) != "message" {
		t.Errorf("message 0 value: %q", messages[0].Value)
	}
	if messages[0].Offset != 0 {
		t.Errorf("message 0 offset: %d", messages[0].Offset)
	}

	// Poll again should return no new messages
	messages2, err := c.Poll()
	if err != nil {
		t.Fatal(err)
	}
	if len(messages2) != 0 {
		t.Errorf("expected 0 messages on second poll, got %d", len(messages2))
	}
}

func TestConsumerOffsetOutOfRange(t *testing.T) {
	addr := startTestServer(t)

	// Produce a message
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
	p.Send(producer.Message{Topic: "test", Partition: 0, Value: []byte("msg")})
	p.Close()

	// Consume with invalid offset
	c, err := NewConsumer(Config{
		BrokerAddr: addr,
		ClientID:   "test",
		MaxBytes:   1024 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.Subscribe("test", 0, -1) // before earliest
	_, err = c.Poll()
	if err == nil {
		t.Fatal("expected offset out of range error")
	}
}
