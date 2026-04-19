package controller

import (
	"encoding/json"
	"log"
	"net"
	"sync"

	"github.com/harshithgowda/streamq/internal/cluster"
)

// RPCServer accepts broker connections and handles controller RPCs.
type RPCServer struct {
	listener   net.Listener
	controller *Controller

	mu    sync.RWMutex
	conns map[cluster.BrokerID]net.Conn // active broker connections

	quit chan struct{}
	wg   sync.WaitGroup
}

// NewRPCServer creates and starts an RPC server.
func NewRPCServer(addr string, ctrl *Controller) (*RPCServer, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	s := &RPCServer{
		listener:   ln,
		controller: ctrl,
		conns:      make(map[cluster.BrokerID]net.Conn),
		quit:       make(chan struct{}),
	}

	s.wg.Add(1)
	go s.acceptLoop()

	log.Printf("controller RPC listening on %s", addr)
	return s, nil
}

func (s *RPCServer) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				log.Printf("rpc accept error: %v", err)
				continue
			}
		}

		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *RPCServer) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	var brokerID cluster.BrokerID = -1

	for {
		msgType, payload, err := cluster.ReadMsg(conn)
		if err != nil {
			select {
			case <-s.quit:
			default:
				if brokerID >= 0 {
					log.Printf("broker %d disconnected: %v", brokerID, err)
					s.removeBrokerConn(brokerID)
				}
			}
			return
		}

		switch msgType {
		case cluster.MsgRegisterBroker:
			var req cluster.RegisterBrokerReq
			if err := json.Unmarshal(payload, &req); err != nil {
				log.Printf("bad register request: %v", err)
				return
			}

			id, meta, err := s.controller.RegisterBroker(req.Host, req.Port)
			resp := cluster.RegisterBrokerResp{}
			if err != nil {
				resp.Error = err.Error()
			} else {
				resp.BrokerID = id
				resp.Metadata = meta
				brokerID = id

				s.mu.Lock()
				s.conns[id] = conn
				s.mu.Unlock()

				log.Printf("registered broker %d (%s:%d)", id, req.Host, req.Port)
			}

			if err := cluster.WriteMsg(conn, cluster.MsgRegisterBrokerResp, resp); err != nil {
				log.Printf("write register resp: %v", err)
				return
			}

		case cluster.MsgBrokerHeartbeat:
			var req cluster.BrokerHeartbeatReq
			if err := json.Unmarshal(payload, &req); err != nil {
				log.Printf("bad heartbeat: %v", err)
				continue
			}

			_ = s.controller.RecordHeartbeat(req.BrokerID)

			if err := cluster.WriteMsg(conn, cluster.MsgHeartbeatResp, cluster.HeartbeatResp{OK: true}); err != nil {
				log.Printf("write heartbeat resp: %v", err)
				return
			}

		case cluster.MsgISRChange:
			var req cluster.ISRChangeReq
			if err := json.Unmarshal(payload, &req); err != nil {
				log.Printf("bad ISR change: %v", err)
				continue
			}

			if err := s.controller.UpdateISR(req.Topic, req.Partition, req.NewISR); err != nil {
				log.Printf("ISR update failed: %v", err)
			}

		case cluster.MsgCreateTopicReq:
			var req cluster.CreateTopicRPCReq
			if err := json.Unmarshal(payload, &req); err != nil {
				log.Printf("bad create topic: %v", err)
				continue
			}

			resp := cluster.CreateTopicRPCResp{}
			if err := s.controller.CreateTopic(req.Topic, req.NumPartitions, req.ReplicationFactor); err != nil {
				resp.Error = err.Error()
			}

			if err := cluster.WriteMsg(conn, cluster.MsgCreateTopicResp, resp); err != nil {
				log.Printf("write create topic resp: %v", err)
				return
			}

		default:
			log.Printf("unknown RPC message type: %d", msgType)
		}
	}
}

func (s *RPCServer) removeBrokerConn(id cluster.BrokerID) {
	s.mu.Lock()
	delete(s.conns, id)
	s.mu.Unlock()
}

// BroadcastMetadata sends a MetadataUpdate to all connected brokers.
func (s *RPCServer) BroadcastMetadata(meta *cluster.ClusterMetadata) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	msg := cluster.MetadataUpdateMsg{Metadata: meta}
	for id, conn := range s.conns {
		if err := cluster.WriteMsg(conn, cluster.MsgMetadataUpdate, msg); err != nil {
			log.Printf("push metadata to broker %d: %v", id, err)
		}
	}
}

// Addr returns the listener address.
func (s *RPCServer) Addr() net.Addr {
	return s.listener.Addr()
}

// Stop shuts down the RPC server.
func (s *RPCServer) Stop() {
	close(s.quit)
	s.listener.Close()
	s.mu.Lock()
	for _, conn := range s.conns {
		conn.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}
