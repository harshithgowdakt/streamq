package producer

import (
	"testing"
	"time"

	"github.com/harshithgowda/streamq/internal/broker"
	"github.com/harshithgowda/streamq/internal/server"
)

func startTestServer(t *testing.T) string {
	t.Helper()

	cfg := broker.Config{
		DataDir:           t.TempDir(),
		DefaultPartitions: 1,
		MaxSegmentBytes:   1024 * 1024,
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

func TestProducerSend(t *testing.T) {
	addr := startTestServer(t)

	cfg := Config{
		BrokerAddr:     addr,
		BatchSize:      10,
		LingerTime:     50 * time.Millisecond,
		RequestTimeout: 5 * time.Second,
		ClientID:       "test-producer",
	}

	p, err := NewProducer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	offset, err := p.Send(Message{
		Topic:     "test",
		Partition: 0,
		Value:     []byte("hello producer"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if offset != 0 {
		t.Errorf("expected offset 0, got %d", offset)
	}

	// Send another
	offset2, err := p.Send(Message{
		Topic:     "test",
		Partition: 0,
		Value:     []byte("second message"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if offset2 <= offset {
		t.Errorf("expected offset > %d, got %d", offset, offset2)
	}
}

func TestProducerBatching(t *testing.T) {
	addr := startTestServer(t)

	cfg := Config{
		BrokerAddr:     addr,
		BatchSize:      5,
		LingerTime:     500 * time.Millisecond,
		RequestTimeout: 5 * time.Second,
		ClientID:       "test-producer",
	}

	p, err := NewProducer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Send 5 messages concurrently (should trigger batch flush)
	results := make(chan Result, 5)
	for i := 0; i < 5; i++ {
		go func(i int) {
			off, err := p.Send(Message{
				Topic:     "test",
				Partition: 0,
				Value:     []byte("batch msg"),
			})
			results <- Result{Offset: off, Err: err}
		}(i)
	}

	for i := 0; i < 5; i++ {
		r := <-results
		if r.Err != nil {
			t.Errorf("send %d: %v", i, r.Err)
		}
	}
}
