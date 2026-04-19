package log

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Record is a single key-value message.
type Record struct {
	Key   []byte
	Value []byte
}

// RecordBatch is a group of records written atomically.
// Wire format (big-endian, fixed-width):
//
//	BaseOffset    int64   (8)
//	BatchLength   int32   (4) — bytes after this field
//	CRC           uint32  (4) — CRC-32C of everything after CRC field
//	Attributes    int16   (2)
//	RecordCount   int32   (4)
//	[ for each record ]
//	  KeyLen      int32   (4)  — -1 means nil
//	  Key         []byte
//	  ValueLen    int32   (4)  — -1 means nil
//	  Value       []byte
type RecordBatch struct {
	BaseOffset int64
	Attributes int16
	Records    []Record
}

const (
	batchHeaderSize = 8 + 4 + 4 + 2 + 4 // BaseOffset + BatchLength + CRC + Attributes + RecordCount
)

// Encode serializes a RecordBatch to bytes.
func (b *RecordBatch) Encode() []byte {
	// Calculate body size (after CRC field): Attributes(2) + RecordCount(4) + records
	bodySize := 2 + 4
	for _, r := range b.Records {
		bodySize += 4 + len(r.Key) + 4 + len(r.Value)
	}

	batchLength := 4 + bodySize      // CRC(4) + body
	totalSize := 8 + 4 + batchLength // BaseOffset(8) + BatchLength(4) + batchLength

	buf := make([]byte, totalSize)
	offset := 0

	// BaseOffset
	binary.BigEndian.PutUint64(buf[offset:], uint64(b.BaseOffset))
	offset += 8

	// BatchLength
	binary.BigEndian.PutUint32(buf[offset:], uint32(batchLength))
	offset += 4

	// Skip CRC for now (will fill after encoding body)
	crcPos := offset
	offset += 4

	// Attributes
	binary.BigEndian.PutUint16(buf[offset:], uint16(b.Attributes))
	offset += 2

	// RecordCount
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(b.Records)))
	offset += 4

	// Records
	for _, r := range b.Records {
		if r.Key == nil {
			binary.BigEndian.PutUint32(buf[offset:], uint32(0xFFFFFFFF)) // -1
			offset += 4
		} else {
			binary.BigEndian.PutUint32(buf[offset:], uint32(len(r.Key)))
			offset += 4
			copy(buf[offset:], r.Key)
			offset += len(r.Key)
		}

		if r.Value == nil {
			binary.BigEndian.PutUint32(buf[offset:], uint32(0xFFFFFFFF)) // -1
			offset += 4
		} else {
			binary.BigEndian.PutUint32(buf[offset:], uint32(len(r.Value)))
			offset += 4
			copy(buf[offset:], r.Value)
			offset += len(r.Value)
		}
	}

	// Compute CRC over everything after the CRC field
	crc := crc32.Checksum(buf[crcPos+4:offset], crcTable)
	binary.BigEndian.PutUint32(buf[crcPos:], crc)

	return buf
}

// DecodeRecordBatch deserializes a RecordBatch from bytes.
func DecodeRecordBatch(data []byte) (*RecordBatch, error) {
	if len(data) < batchHeaderSize {
		return nil, fmt.Errorf("data too short for batch header: %d", len(data))
	}

	offset := 0

	baseOffset := int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	batchLength := int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	if int(batchLength)+12 > len(data) {
		return nil, fmt.Errorf("batch length %d exceeds available data %d", batchLength, len(data)-12)
	}

	storedCRC := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	// Validate CRC: covers everything after the CRC field, for batchLength-4 bytes
	computedCRC := crc32.Checksum(data[offset:12+batchLength], crcTable)
	if storedCRC != computedCRC {
		return nil, fmt.Errorf("CRC mismatch: stored=%08x computed=%08x", storedCRC, computedCRC)
	}

	attributes := int16(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	recordCount := int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	records := make([]Record, 0, recordCount)
	for i := int32(0); i < recordCount; i++ {
		if offset+4 > len(data) {
			return nil, fmt.Errorf("unexpected end of data reading key length for record %d", i)
		}
		keyLen := int32(binary.BigEndian.Uint32(data[offset:]))
		offset += 4

		var key []byte
		if keyLen >= 0 {
			if offset+int(keyLen) > len(data) {
				return nil, fmt.Errorf("unexpected end of data reading key for record %d", i)
			}
			key = make([]byte, keyLen)
			copy(key, data[offset:offset+int(keyLen)])
			offset += int(keyLen)
		}

		if offset+4 > len(data) {
			return nil, fmt.Errorf("unexpected end of data reading value length for record %d", i)
		}
		valueLen := int32(binary.BigEndian.Uint32(data[offset:]))
		offset += 4

		var value []byte
		if valueLen >= 0 {
			if offset+int(valueLen) > len(data) {
				return nil, fmt.Errorf("unexpected end of data reading value for record %d", i)
			}
			value = make([]byte, valueLen)
			copy(value, data[offset:offset+int(valueLen)])
			offset += int(valueLen)
		}

		records = append(records, Record{Key: key, Value: value})
	}

	return &RecordBatch{
		BaseOffset: baseOffset,
		Attributes: attributes,
		Records:    records,
	}, nil
}

// BatchSize returns the total encoded size of the batch.
func (b *RecordBatch) BatchSize() int {
	bodySize := 2 + 4
	for _, r := range b.Records {
		bodySize += 4 + len(r.Key) + 4 + len(r.Value)
	}
	return 8 + 4 + 4 + bodySize // BaseOffset + BatchLength + CRC + body
}
