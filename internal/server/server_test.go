package server

import (
	"encoding/binary"
	"io"
	"net"
	"testing"

	"github.com/harshithgowda/streamq/internal/broker"
	"github.com/harshithgowda/streamq/internal/log"
	"github.com/harshithgowda/streamq/internal/protocol"
)

func testServer(t *testing.T) (*Server, string) {
	t.Helper()

	cfg := broker.Config{
		DataDir:           t.TempDir(),
		DefaultPartitions: 1,
		MaxSegmentBytes:   1024 * 1024,
		Addr:              ":0",
	}
	b := broker.NewBroker(cfg)

	// Pre-create a topic
	b.TopicManager.CreateTopic("test", 1)

	srv := NewServer(b)
	if err := srv.Start(":0"); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		srv.Stop()
		b.Close()
	})

	return srv, srv.Addr().String()
}

func sendRequest(conn net.Conn, req interface{}) ([]byte, error) {
	data, err := protocol.EncodeRequest(req)
	if err != nil {
		return nil, err
	}

	// Write frame
	frame := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(frame, uint32(len(data)))
	copy(frame[4:], data)
	if _, err := conn.Write(frame); err != nil {
		return nil, err
	}

	// Read response frame
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

func TestServerProduceAndFetch(t *testing.T) {
	_, addr := testServer(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Produce
	batch := &log.RecordBatch{
		Records: []log.Record{
			{Key: []byte("key"), Value: []byte("hello from server test")},
		},
	}

	respBytes, err := sendRequest(conn, &protocol.ProduceRequest{
		Header: protocol.RequestHeader{
			APIKey:        protocol.APIKeyProduce,
			CorrelationID: 1,
			ClientID:      "test",
		},
		Topics: []protocol.ProduceTopicData{
			{
				Topic: "test",
				Partitions: []protocol.ProducePartitionData{
					{Partition: 0, Records: batch.Encode()},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	produceResp, err := protocol.DecodeResponse(protocol.APIKeyProduce, respBytes)
	if err != nil {
		t.Fatal(err)
	}
	pr := produceResp.(*protocol.ProduceResponse)
	if pr.Topics[0].Partitions[0].ErrorCode != protocol.ErrNone {
		t.Fatalf("produce error: %d", pr.Topics[0].Partitions[0].ErrorCode)
	}

	// Fetch
	respBytes, err = sendRequest(conn, &protocol.FetchRequest{
		Header: protocol.RequestHeader{
			APIKey:        protocol.APIKeyFetch,
			CorrelationID: 2,
			ClientID:      "test",
		},
		MaxBytes: 1024 * 1024,
		Topics: []protocol.FetchTopicData{
			{
				Topic: "test",
				Partitions: []protocol.FetchPartitionData{
					{Partition: 0, FetchOffset: 0, MaxBytes: 65536},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	fetchResp, err := protocol.DecodeResponse(protocol.APIKeyFetch, respBytes)
	if err != nil {
		t.Fatal(err)
	}
	fr := fetchResp.(*protocol.FetchResponse)
	if fr.Topics[0].Partitions[0].ErrorCode != protocol.ErrNone {
		t.Fatalf("fetch error: %d", fr.Topics[0].Partitions[0].ErrorCode)
	}

	rawBatches := fr.Topics[0].Partitions[0].RecordBatches
	decoded, err := log.DecodeRecordBatch(rawBatches)
	if err != nil {
		t.Fatal(err)
	}
	if string(decoded.Records[0].Value) != "hello from server test" {
		t.Errorf("got value %q", decoded.Records[0].Value)
	}
}
