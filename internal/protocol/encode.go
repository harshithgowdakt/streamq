package protocol

import (
	"encoding/binary"
	"io"
	"math/bits"
)

// Encoder writes big-endian fixed-width values to a writer.
type Encoder struct {
	w   io.Writer
	buf [8]byte
	err error
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w}
}

func (e *Encoder) Err() error { return e.err }

func (e *Encoder) write(b []byte) {
	if e.err != nil {
		return
	}
	_, e.err = e.w.Write(b)
}

func (e *Encoder) WriteInt8(v int8) {
	e.buf[0] = byte(v)
	e.write(e.buf[:1])
}

func (e *Encoder) WriteInt16(v int16) {
	binary.BigEndian.PutUint16(e.buf[:], uint16(v))
	e.write(e.buf[:2])
}

func (e *Encoder) WriteInt32(v int32) {
	binary.BigEndian.PutUint32(e.buf[:], uint32(v))
	e.write(e.buf[:4])
}

func (e *Encoder) WriteInt64(v int64) {
	binary.BigEndian.PutUint64(e.buf[:], uint64(v))
	e.write(e.buf[:8])
}

// WriteString writes a length-prefixed string (int16 length + bytes).
func (e *Encoder) WriteString(s string) {
	e.WriteInt16(int16(len(s)))
	e.write([]byte(s))
}

// WriteNullableString writes a nullable string (-1 length for nil).
func (e *Encoder) WriteNullableString(s *string) {
	if s == nil {
		e.WriteInt16(-1)
		return
	}
	e.WriteString(*s)
}

// WriteBytes writes a length-prefixed byte slice (int32 length + bytes). -1 for nil.
func (e *Encoder) WriteBytes(b []byte) {
	if b == nil {
		e.WriteInt32(-1)
		return
	}
	e.WriteInt32(int32(len(b)))
	e.write(b)
}

// WriteUint32 writes an unsigned 32-bit integer.
func (e *Encoder) WriteUint32(v uint32) {
	binary.BigEndian.PutUint32(e.buf[:], v)
	e.write(e.buf[:4])
}

// WriteVarint writes a zigzag-encoded signed varint.
func (e *Encoder) WriteVarint(v int64) {
	var buf [binary.MaxVarintLen64]byte
	z := uint64((v << 1) ^ (v >> 63)) // zigzag encode
	n := binary.PutUvarint(buf[:], z)
	e.write(buf[:n])
}

// WriteUvarint writes an unsigned varint.
func (e *Encoder) WriteUvarint(v uint64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	e.write(buf[:n])
}

// WriteCompactString writes a compact string (uvarint(len+1) + bytes).
func (e *Encoder) WriteCompactString(s string) {
	e.WriteUvarint(uint64(len(s)) + 1)
	e.write([]byte(s))
}

// WriteCompactNullableString writes a compact nullable string.
func (e *Encoder) WriteCompactNullableString(s *string) {
	if s == nil {
		e.WriteUvarint(0)
		return
	}
	e.WriteCompactString(*s)
}

// WriteTaggedFields writes an empty tagged field section (uvarint 0).
func (e *Encoder) WriteTaggedFields() {
	e.WriteUvarint(0)
}

// WriteVarintBytes writes varint-length-prefixed bytes (for Kafka records).
func (e *Encoder) WriteVarintBytes(b []byte) {
	if b == nil {
		e.WriteVarint(-1)
		return
	}
	e.WriteVarint(int64(len(b)))
	e.write(b)
}

// WriteRaw writes raw bytes directly.
func (e *Encoder) WriteRaw(b []byte) {
	e.write(b)
}

// VarintLen returns the number of bytes needed to encode v as a zigzag varint.
func VarintLen(v int64) int {
	z := uint64((v << 1) ^ (v >> 63))
	if z == 0 {
		return 1
	}
	return (bits.Len64(z) + 6) / 7
}

// UvarintLen returns the number of bytes needed to encode v as a uvarint.
func UvarintLen(v uint64) int {
	if v == 0 {
		return 1
	}
	return (bits.Len64(v) + 6) / 7
}
