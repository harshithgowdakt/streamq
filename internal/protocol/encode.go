package protocol

import (
	"encoding/binary"
	"io"
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

// WriteRaw writes raw bytes directly.
func (e *Encoder) WriteRaw(b []byte) {
	e.write(b)
}
