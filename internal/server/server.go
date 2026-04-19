package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/harshithgowda/streamq/internal/broker"
	"github.com/harshithgowda/streamq/internal/protocol"
)

// Server is a TCP server that accepts connections and dispatches requests to the broker.
type Server struct {
	broker   *broker.Broker
	listener net.Listener
	wg       sync.WaitGroup
	quit     chan struct{}
}

// NewServer creates a new Server.
func NewServer(b *broker.Broker) *Server {
	return &Server{
		broker: b,
		quit:   make(chan struct{}),
	}
}

// Start begins listening and accepting connections.
func (s *Server) Start(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	s.listener = ln
	log.Printf("streamq broker listening on %s", addr)

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				log.Printf("accept error: %v", err)
				continue
			}
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	for {
		// Read frame length (4 bytes)
		var frameLenBuf [4]byte
		if _, err := io.ReadFull(conn, frameLenBuf[:]); err != nil {
			if err != io.EOF {
				select {
				case <-s.quit:
				default:
					log.Printf("read frame length: %v", err)
				}
			}
			return
		}

		frameLen := int32(binary.BigEndian.Uint32(frameLenBuf[:]))
		if frameLen <= 0 || frameLen > 100*1024*1024 {
			log.Printf("invalid frame length: %d", frameLen)
			return
		}

		// Read frame body
		body := make([]byte, frameLen)
		if _, err := io.ReadFull(conn, body); err != nil {
			log.Printf("read frame body: %v", err)
			return
		}

		// Decode request
		req, err := protocol.DecodeRequest(body)
		if err != nil {
			log.Printf("decode request: %v", err)
			return
		}

		// Handle fetch with MinBytes/MaxWaitMs polling
		var resp interface{}
		if fetchReq, ok := req.(*protocol.FetchRequest); ok {
			resp = s.handleFetchWithWait(fetchReq)
		} else {
			resp, err = s.broker.Dispatch(req)
			if err != nil {
				log.Printf("dispatch: %v", err)
				return
			}
		}

		// Encode response
		respBytes, err := protocol.EncodeResponse(resp)
		if err != nil {
			log.Printf("encode response: %v", err)
			return
		}

		// Write frame: length prefix + response
		frame := make([]byte, 4+len(respBytes))
		binary.BigEndian.PutUint32(frame, uint32(len(respBytes)))
		copy(frame[4:], respBytes)

		if _, err := conn.Write(frame); err != nil {
			log.Printf("write response: %v", err)
			return
		}
	}
}

// handleFetchWithWait implements Kafka's MinBytes/MaxWaitMs semantics:
// it polls the broker until MinBytes of data is available or MaxWaitMs expires.
func (s *Server) handleFetchWithWait(req *protocol.FetchRequest) *protocol.FetchResponse {
	resp := s.broker.HandleFetch(req)

	minBytes := int(req.MinBytes)
	maxWaitMs := int(req.MaxWaitMs)

	// If we already have enough data or no wait requested, return immediately
	if minBytes <= 0 || maxWaitMs <= 0 || totalFetchBytes(resp) >= minBytes {
		return resp
	}

	// Poll until MinBytes satisfied or MaxWaitMs elapsed
	deadline := time.Now().Add(time.Duration(maxWaitMs) * time.Millisecond)
	pollInterval := 10 * time.Millisecond

	for totalFetchBytes(resp) < minBytes {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}

		wait := pollInterval
		if wait > remaining {
			wait = remaining
		}

		select {
		case <-time.After(wait):
		case <-s.quit:
			return resp
		}

		resp = s.broker.HandleFetch(req)
	}

	return resp
}

func totalFetchBytes(resp *protocol.FetchResponse) int {
	total := 0
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			total += len(p.RecordBatches)
		}
	}
	return total
}

// Stop gracefully shuts down the server.
func (s *Server) Stop() {
	close(s.quit)
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
}

// Addr returns the listener's address (useful for tests with :0 port).
func (s *Server) Addr() net.Addr {
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}
