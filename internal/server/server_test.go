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

	// Produce using Kafka RecordBatch format
	batch := &log.RecordBatch{
		Records: []log.Record{
			{Key: []byte("key"), Value: []byte("hello from server test")},
		},
	}

	respBytes, err := sendRequest(conn, &protocol.ProduceRequest{
		Header: protocol.RequestHeader{
			APIKey:        protocol.APIKeyProduce,
			APIVersion:    3,
			CorrelationID: 1,
			ClientID:      "test",
		},
		Acks: -1,
		Topics: []protocol.ProduceTopicData{
			{
				Topic: "test",
				Partitions: []protocol.ProducePartitionData{
					{Partition: 0, Records: protocol.InternalToKafkaRecordBatch(batch)},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	produceResp, err := protocol.DecodeResponse(protocol.APIKeyProduce, 3, respBytes)
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
			APIVersion:    4,
			CorrelationID: 2,
			ClientID:      "test",
		},
		ReplicaID: -1,
		MaxBytes:  1024 * 1024,
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

	fetchResp, err := protocol.DecodeResponse(protocol.APIKeyFetch, 4, respBytes)
	if err != nil {
		t.Fatal(err)
	}
	fr := fetchResp.(*protocol.FetchResponse)
	if fr.Topics[0].Partitions[0].ErrorCode != protocol.ErrNone {
		t.Fatalf("fetch error: %d", fr.Topics[0].Partitions[0].ErrorCode)
	}

	rawBatches := fr.Topics[0].Partitions[0].RecordBatches
	batches, err := protocol.KafkaRecordBatchToInternal(rawBatches)
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) == 0 || len(batches[0].Records) == 0 {
		t.Fatal("no records in response")
	}
	if string(batches[0].Records[0].Value) != "hello from server test" {
		t.Errorf("got value %q", batches[0].Records[0].Value)
	}
}

func TestServerApiVersions(t *testing.T) {
	_, addr := testServer(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	respBytes, err := sendRequest(conn, &protocol.ApiVersionsRequest{
		Header: protocol.RequestHeader{
			APIKey:        protocol.APIKeyApiVersions,
			APIVersion:    0,
			CorrelationID: 1,
			ClientID:      "test",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := protocol.DecodeResponse(protocol.APIKeyApiVersions, 0, respBytes)
	if err != nil {
		t.Fatal(err)
	}

	avResp := resp.(*protocol.ApiVersionsResponse)
	if avResp.ErrorCode != protocol.ErrNone {
		t.Fatalf("error code: %d", avResp.ErrorCode)
	}
	if len(avResp.ApiVersions) == 0 {
		t.Fatal("expected api versions in response")
	}
}
