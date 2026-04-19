package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// byteReaderWrapper wraps an io.Reader to satisfy io.ByteReader for varint decoding.
type byteReaderWrapper struct {
	r   io.Reader
	buf [1]byte
}

func (b *byteReaderWrapper) ReadByte() (byte, error) {
	_, err := io.ReadFull(b.r, b.buf[:])
	return b.buf[0], err
}

// Decoder reads big-endian fixed-width values from a reader.
type Decoder struct {
	r   io.Reader
	br  *byteReaderWrapper
	buf [8]byte
	err error
}

func NewDecoder(r io.Reader) *Decoder {
	br := &byteReaderWrapper{r: r}
	return &Decoder{r: r, br: br}
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

// ReadUint32 reads an unsigned 32-bit integer.
func (d *Decoder) ReadUint32() uint32 {
	b := d.read(4)
	return binary.BigEndian.Uint32(b)
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

// ReadUvarint reads an unsigned varint.
func (d *Decoder) ReadUvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, err := binary.ReadUvarint(d.br)
	if err != nil {
		d.err = fmt.Errorf("read uvarint: %w", err)
		return 0
	}
	return v
}

// ReadVarint reads a zigzag-encoded signed varint.
func (d *Decoder) ReadVarint() int64 {
	uv := d.ReadUvarint()
	// zigzag decode
	return int64((uv >> 1) ^ -(uv & 1))
}

// ReadCompactString reads a compact string (uvarint(len+1) + bytes).
func (d *Decoder) ReadCompactString() string {
	n := d.ReadUvarint()
	if d.err != nil || n == 0 {
		return ""
	}
	length := int(n - 1)
	if length == 0 {
		return ""
	}
	buf := make([]byte, length)
	if d.err == nil {
		_, d.err = io.ReadFull(d.r, buf)
	}
	return string(buf)
}

// ReadCompactNullableString reads a compact nullable string.
func (d *Decoder) ReadCompactNullableString() *string {
	n := d.ReadUvarint()
	if d.err != nil || n == 0 {
		return nil
	}
	length := int(n - 1)
	buf := make([]byte, length)
	if d.err == nil {
		_, d.err = io.ReadFull(d.r, buf)
	}
	s := string(buf)
	return &s
}

// ReadTaggedFields reads and skips tagged fields.
func (d *Decoder) ReadTaggedFields() {
	numTags := d.ReadUvarint()
	for i := uint64(0); i < numTags && d.err == nil; i++ {
		_ = d.ReadUvarint() // tag
		size := d.ReadUvarint()
		if d.err == nil && size > 0 {
			_ = d.ReadRaw(int(size))
		}
	}
}

// ReadVarintBytes reads varint-length-prefixed bytes (for Kafka records).
func (d *Decoder) ReadVarintBytes() []byte {
	length := d.ReadVarint()
	if d.err != nil || length < 0 {
		return nil
	}
	if length == 0 {
		return []byte{}
	}
	buf := make([]byte, length)
	if d.err == nil {
		_, d.err = io.ReadFull(d.r, buf)
	}
	return buf
}
