package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Decoder reads big-endian fixed-width values from a reader.
type Decoder struct {
	r   io.Reader
	buf [8]byte
	err error
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

func (d *Decoder) Err() error { return d.err }

func (d *Decoder) read(n int) []byte {
	if d.err != nil {
		return d.buf[:n]
	}
	_, d.err = io.ReadFull(d.r, d.buf[:n])
	return d.buf[:n]
}

func (d *Decoder) ReadInt8() int8 {
	b := d.read(1)
	return int8(b[0])
}

func (d *Decoder) ReadInt16() int16 {
	b := d.read(2)
	return int16(binary.BigEndian.Uint16(b))
}

func (d *Decoder) ReadInt32() int32 {
	b := d.read(4)
	return int32(binary.BigEndian.Uint32(b))
}

func (d *Decoder) ReadInt64() int64 {
	b := d.read(8)
	return int64(binary.BigEndian.Uint64(b))
}

// ReadString reads a length-prefixed string (int16 length + bytes).
func (d *Decoder) ReadString() string {
	length := d.ReadInt16()
	if d.err != nil || length < 0 {
		return ""
	}
	buf := make([]byte, length)
	if d.err == nil {
		_, d.err = io.ReadFull(d.r, buf)
	}
	return string(buf)
}

// ReadNullableString reads a nullable string. Returns nil for -1 length.
func (d *Decoder) ReadNullableString() *string {
	length := d.ReadInt16()
	if d.err != nil || length < 0 {
		return nil
	}
	buf := make([]byte, length)
	if d.err == nil {
		_, d.err = io.ReadFull(d.r, buf)
	}
	s := string(buf)
	return &s
}

// ReadBytes reads a length-prefixed byte slice (int32 length + bytes). Returns nil for -1 length.
func (d *Decoder) ReadBytes() []byte {
	length := d.ReadInt32()
	if d.err != nil || length < 0 {
		return nil
	}
	if length > 100*1024*1024 { // 100MB sanity check
		d.err = fmt.Errorf("byte slice too large: %d", length)
		return nil
	}
	buf := make([]byte, length)
	if d.err == nil {
		_, d.err = io.ReadFull(d.r, buf)
	}
	return buf
}

// ReadRaw reads exactly n bytes.
func (d *Decoder) ReadRaw(n int) []byte {
	if d.err != nil {
		return nil
	}
	buf := make([]byte, n)
	_, d.err = io.ReadFull(d.r, buf)
	return buf
}
