package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/harshithgowda/streamq/internal/log"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Kafka RecordBatch header layout (magic=2):
//
//	BaseOffset           int64   (8)
//	BatchLength          int32   (4)  — bytes after this field
//	PartitionLeaderEpoch int32   (4)
//	Magic                int8    (1)  = 2
//	CRC                  uint32  (4)  — CRC-32C of attributes through end of batch
//	Attributes           int16   (2)
//	LastOffsetDelta      int32   (4)
//	FirstTimestamp       int64   (8)
//	MaxTimestamp         int64   (8)
//	ProducerID           int64   (8)
//	ProducerEpoch        int16   (2)
//	BaseSequence         int32   (4)
//	NumRecords           int32   (4)
//	--- total header: 61 bytes ---

const kafkaBatchHeaderSize = 61 // 8+4+4+1+4+2+4+8+8+8+2+4+4

// KafkaRecordBatchToInternal parses Kafka RecordBatch bytes (magic=2) and converts
// to internal log.RecordBatch format. Supports multiple concatenated batches.
func KafkaRecordBatchToInternal(data []byte) ([]*log.RecordBatch, error) {
	var batches []*log.RecordBatch

	for len(data) >= 12 { // minimum: BaseOffset(8) + BatchLength(4)
		if len(data) < kafkaBatchHeaderSize {
			break
		}

		baseOffset := int64(binary.BigEndian.Uint64(data[0:8]))
		batchLength := int32(binary.BigEndian.Uint32(data[8:12]))
		totalSize := 12 + int(batchLength)
		if batchLength < 0 || totalSize > len(data) {
			return nil, fmt.Errorf("batch length %d exceeds available data %d", batchLength, len(data)-12)
		}

		batchData := data[:totalSize]

		// PartitionLeaderEpoch at offset 12
		// Magic at offset 16
		magic := int8(batchData[16])
		if magic != 2 {
			return nil, fmt.Errorf("unsupported magic: %d (expected 2)", magic)
		}

		// CRC at offset 17
		storedCRC := binary.BigEndian.Uint32(batchData[17:21])

		// Validate CRC-32C: covers from attributes (offset 21) through end of batch
		computedCRC := crc32.Checksum(batchData[21:totalSize], crcTable)
		if storedCRC != computedCRC {
			return nil, fmt.Errorf("CRC mismatch: stored=%08x computed=%08x", storedCRC, computedCRC)
		}

		// attributes at offset 21
		// lastOffsetDelta at offset 23
		// firstTimestamp at offset 27
		// maxTimestamp at offset 35
		// producerId at offset 43
		// producerEpoch at offset 51
		// baseSequence at offset 53
		numRecords := int32(binary.BigEndian.Uint32(batchData[57:61]))

		// Decode varint-encoded records starting at offset 61
		recordReader := bytes.NewReader(batchData[61:totalSize])
		dec := NewDecoder(recordReader)

		records := make([]log.Record, 0, numRecords)
		for i := int32(0); i < numRecords; i++ {
			_ = dec.ReadVarint() // record length
			_ = dec.ReadInt8()   // attributes
			_ = dec.ReadVarint() // timestampDelta
			_ = dec.ReadVarint() // offsetDelta

			key := dec.ReadVarintBytes()
			value := dec.ReadVarintBytes()

			// headers
			numHeaders := dec.ReadVarint()
			for h := int64(0); h < numHeaders && dec.Err() == nil; h++ {
				_ = dec.ReadVarintBytes() // header key
				_ = dec.ReadVarintBytes() // header value
			}

			if dec.Err() != nil {
				return nil, fmt.Errorf("decode record %d: %w", i, dec.Err())
			}

			records = append(records, log.Record{Key: key, Value: value})
		}

		batches = append(batches, &log.RecordBatch{
			BaseOffset: baseOffset,
			Records:    records,
		})

		data = data[totalSize:]
	}

	if len(batches) == 0 {
		return nil, fmt.Errorf("no valid record batches found")
	}

	return batches, nil
}

// InternalToKafkaRecordBatch converts an internal log.RecordBatch to Kafka wire
// format (magic=2 RecordBatch).
func InternalToKafkaRecordBatch(batch *log.RecordBatch) []byte {
	now := time.Now().UnixMilli()

	// Encode records into varint format first
	var recordsBuf bytes.Buffer
	recEnc := NewEncoder(&recordsBuf)
	for i, rec := range batch.Records {
		// Calculate record body size
		bodySize := 1 + // attributes (int8)
			VarintLen(0) + // timestampDelta
			VarintLen(int64(i)) + // offsetDelta
			varintBytesLen(rec.Key) +
			varintBytesLen(rec.Value) +
			VarintLen(0) // num headers

		recEnc.WriteVarint(int64(bodySize))
		recEnc.WriteInt8(0)              // attributes
		recEnc.WriteVarint(0)            // timestampDelta
		recEnc.WriteVarint(int64(i))     // offsetDelta
		recEnc.WriteVarintBytes(rec.Key) // key
		recEnc.WriteVarintBytes(rec.Value)
		recEnc.WriteVarint(0) // num headers
	}
	recordsBytes := recordsBuf.Bytes()

	// Build the portion from attributes through end (what CRC covers)
	// attributes(2) + lastOffsetDelta(4) + firstTimestamp(8) + maxTimestamp(8) +
	// producerId(8) + producerEpoch(2) + baseSequence(4) + numRecords(4) + records
	crcBodySize := 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + len(recordsBytes)
	crcBody := make([]byte, crcBodySize)
	off := 0

	// Attributes
	binary.BigEndian.PutUint16(crcBody[off:], 0)
	off += 2

	// LastOffsetDelta
	lastDelta := int32(0)
	if len(batch.Records) > 0 {
		lastDelta = int32(len(batch.Records) - 1)
	}
	binary.BigEndian.PutUint32(crcBody[off:], uint32(lastDelta))
	off += 4

	// FirstTimestamp
	binary.BigEndian.PutUint64(crcBody[off:], uint64(now))
	off += 8

	// MaxTimestamp
	binary.BigEndian.PutUint64(crcBody[off:], uint64(now))
	off += 8

	// ProducerID = -1
	binary.BigEndian.PutUint64(crcBody[off:], 0xFFFFFFFFFFFFFFFF)
	off += 8

	// ProducerEpoch = -1
	binary.BigEndian.PutUint16(crcBody[off:], 0xFFFF)
	off += 2

	// BaseSequence = -1
	binary.BigEndian.PutUint32(crcBody[off:], 0xFFFFFFFF)
	off += 4

	// NumRecords
	binary.BigEndian.PutUint32(crcBody[off:], uint32(len(batch.Records)))
	off += 4

	// Records
	copy(crcBody[off:], recordsBytes)

	// Compute CRC
	crc := crc32.Checksum(crcBody, crcTable)

	// BatchLength = partitionLeaderEpoch(4) + magic(1) + crc(4) + crcBody
	batchLength := 4 + 1 + 4 + len(crcBody)

	// Total = BaseOffset(8) + BatchLength(4) + batchLength
	total := 8 + 4 + batchLength
	buf := make([]byte, total)
	pos := 0

	// BaseOffset
	binary.BigEndian.PutUint64(buf[pos:], uint64(batch.BaseOffset))
	pos += 8

	// BatchLength
	binary.BigEndian.PutUint32(buf[pos:], uint32(batchLength))
	pos += 4

	// PartitionLeaderEpoch
	binary.BigEndian.PutUint32(buf[pos:], 0)
	pos += 4

	// Magic
	buf[pos] = 2
	pos += 1

	// CRC
	binary.BigEndian.PutUint32(buf[pos:], crc)
	pos += 4

	// CRC body (attributes through end)
	copy(buf[pos:], crcBody)

	return buf
}

func varintBytesLen(b []byte) int {
	if b == nil {
		return VarintLen(-1)
	}
	return VarintLen(int64(len(b))) + len(b)
}
