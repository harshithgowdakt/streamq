package cluster

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// Wire format: [4-byte length][1-byte type][JSON payload]
// Length covers type byte + JSON payload.

// WriteMsg writes a typed JSON message to w.
func WriteMsg(w io.Writer, msgType byte, msg interface{}) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	length := uint32(1 + len(payload)) // type byte + payload
	var header [5]byte
	binary.BigEndian.PutUint32(header[0:4], length)
	header[4] = msgType

	if _, err := w.Write(header[:]); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}
	return nil
}

// ReadMsg reads a typed JSON message from r.
// Returns the message type and the raw JSON payload.
func ReadMsg(r io.Reader) (byte, []byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return 0, nil, err
	}

	length := binary.BigEndian.Uint32(lenBuf[:])
	if length == 0 || length > 10*1024*1024 { // max 10MB
		return 0, nil, fmt.Errorf("invalid message length: %d", length)
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, nil, fmt.Errorf("read body: %w", err)
	}

	msgType := buf[0]
	payload := buf[1:]
	return msgType, payload, nil
}

// DecodeMsg is a helper that reads a message and unmarshals the JSON payload
// into the appropriate type based on the message type byte.
func DecodeMsg(msgType byte, payload []byte) (interface{}, error) {
	var msg interface{}
	switch msgType {
	case MsgRegisterBroker:
		msg = &RegisterBrokerReq{}
	case MsgRegisterBrokerResp:
		msg = &RegisterBrokerResp{}
	case MsgMetadataUpdate:
		msg = &MetadataUpdateMsg{}
	case MsgISRChange:
		msg = &ISRChangeReq{}
	case MsgBrokerHeartbeat:
		msg = &BrokerHeartbeatReq{}
	case MsgHeartbeatResp:
		msg = &HeartbeatResp{}
	case MsgCreateTopicReq:
		msg = &CreateTopicRPCReq{}
	case MsgCreateTopicResp:
		msg = &CreateTopicRPCResp{}
	default:
		return nil, fmt.Errorf("unknown message type: %d", msgType)
	}

	if err := json.Unmarshal(payload, msg); err != nil {
		return nil, fmt.Errorf("unmarshal type %d: %w", msgType, err)
	}
	return msg, nil
}
