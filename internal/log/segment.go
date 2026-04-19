package log

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	sparseIndexInterval = 4096 // write an index entry every 4KB of log data
)

// Segment is a single .log + .index pair.
type Segment struct {
	mu         sync.RWMutex
	logFile    *os.File
	index      *Index
	baseOffset int64
	nextOffset int64
	position   int64 // current write position in the log file
	dir        string

	// Tracks bytes since last index entry
	bytesSinceIndex int64
}

// NewSegment creates or opens a segment with the given base offset.
func NewSegment(dir string, baseOffset int64) (*Segment, error) {
	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	idxPath := filepath.Join(dir, fmt.Sprintf("%020d.index", baseOffset))

	logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log: %w", err)
	}

	fi, err := logFile.Stat()
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("stat log: %w", err)
	}

	idx, err := NewIndex(idxPath)
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("open index: %w", err)
	}

	s := &Segment{
		logFile:    logFile,
		index:      idx,
		baseOffset: baseOffset,
		nextOffset: baseOffset,
		position:   fi.Size(),
		dir:        dir,
	}

	// Recover nextOffset by scanning existing data
	if fi.Size() > 0 {
		nextOff, err := s.recover()
		if err != nil {
			logFile.Close()
			idx.Close()
			return nil, fmt.Errorf("recover segment: %w", err)
		}
		s.nextOffset = nextOff
	}

	return s, nil
}

// recover scans the log file to find the next valid offset and rebuilds the index.
func (s *Segment) recover() (int64, error) {
	if _, err := s.logFile.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	// Reset index for rebuild
	s.index.SanitizeForRecovery(0)

	var pos int64
	nextOffset := s.baseOffset
	var bytesSinceIdx int64

	for {
		// Read enough for the batch header
		header := make([]byte, batchHeaderSize)
		n, err := io.ReadFull(s.logFile, header)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// Truncate any partial write
				s.logFile.Truncate(pos)
				s.position = pos
				return nextOffset, nil
			}
			return 0, err
		}
		if n < batchHeaderSize {
			s.logFile.Truncate(pos)
			s.position = pos
			return nextOffset, nil
		}

		batchLen := int64(int32(beUint32(header[8:])))
		totalSize := 12 + batchLen // BaseOffset(8) + BatchLength(4) + body

		// Read the full batch
		batchData := make([]byte, totalSize)
		copy(batchData, header)
		remaining := int(totalSize) - batchHeaderSize
		if remaining > 0 {
			n, err = io.ReadFull(s.logFile, batchData[batchHeaderSize:])
			if err != nil || n < remaining {
				// Partial batch — truncate
				s.logFile.Truncate(pos)
				s.position = pos
				return nextOffset, nil
			}
		}

		// Validate CRC
		batch, err := DecodeRecordBatch(batchData)
		if err != nil {
			// Corrupt batch — truncate
			s.logFile.Truncate(pos)
			s.position = pos
			return nextOffset, nil
		}

		// Write sparse index entry if needed
		if bytesSinceIdx >= sparseIndexInterval || pos == 0 {
			relOff := uint32(batch.BaseOffset - s.baseOffset)
			s.index.Write(relOff, uint64(pos))
			bytesSinceIdx = 0
		}

		bytesSinceIdx += totalSize
		pos += totalSize
		nextOffset = batch.BaseOffset + int64(len(batch.Records))
	}
}

func beUint32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

// Append writes a RecordBatch to the segment and returns the base offset assigned.
func (s *Segment) Append(batch *RecordBatch) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	batch.BaseOffset = s.nextOffset
	data := batch.Encode()

	// Write sparse index entry if needed
	if s.bytesSinceIndex >= sparseIndexInterval || s.position == 0 {
		relOff := uint32(s.nextOffset - s.baseOffset)
		if err := s.index.Write(relOff, uint64(s.position)); err != nil {
			return 0, fmt.Errorf("write index: %w", err)
		}
		s.bytesSinceIndex = 0
	}

	n, err := s.logFile.Write(data)
	if err != nil {
		return 0, fmt.Errorf("write log: %w", err)
	}

	assignedOffset := s.nextOffset
	s.nextOffset += int64(len(batch.Records))
	s.position += int64(n)
	s.bytesSinceIndex += int64(n)

	return assignedOffset, nil
}

// Read reads record batches starting from the given offset.
// Returns up to maxBytes of data.
func (s *Segment) Read(offset int64, maxBytes int) ([]*RecordBatch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if offset < s.baseOffset || offset >= s.nextOffset {
		return nil, nil
	}

	// Use index to find starting position
	relOff := uint32(offset - s.baseOffset)
	pos := s.index.Lookup(relOff)
	if pos < 0 {
		pos = 0
	}

	f, err := os.Open(filepath.Join(s.dir, fmt.Sprintf("%020d.log", s.baseOffset)))
	if err != nil {
		return nil, fmt.Errorf("open log for read: %w", err)
	}
	defer f.Close()

	if _, err := f.Seek(pos, io.SeekStart); err != nil {
		return nil, err
	}

	var batches []*RecordBatch
	bytesRead := 0

	for bytesRead < maxBytes {
		header := make([]byte, 12) // BaseOffset(8) + BatchLength(4)
		if _, err := io.ReadFull(f, header); err != nil {
			break
		}

		batchLen := int64(int32(beUint32(header[8:])))
		totalSize := 12 + batchLen

		batchData := make([]byte, totalSize)
		copy(batchData, header)
		if _, err := io.ReadFull(f, batchData[12:]); err != nil {
			break
		}

		batch, err := DecodeRecordBatch(batchData)
		if err != nil {
			break
		}

		// Skip batches before our target offset
		if batch.BaseOffset+int64(len(batch.Records)) <= offset {
			continue
		}

		batches = append(batches, batch)
		bytesRead += int(totalSize)
	}

	return batches, nil
}

// Size returns the current size of the log file.
func (s *Segment) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.position
}

// NextOffset returns the next offset to be assigned.
func (s *Segment) NextOffset() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextOffset
}

// BaseOffset returns this segment's base offset.
func (s *Segment) GetBaseOffset() int64 {
	return s.baseOffset
}

// Close closes the segment's log file and index.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.logFile.Sync(); err != nil {
		return err
	}
	if err := s.logFile.Close(); err != nil {
		return err
	}
	return s.index.Close()
}

// Remove deletes the segment's files from disk.
func (s *Segment) Remove() error {
	logPath := filepath.Join(s.dir, fmt.Sprintf("%020d.log", s.baseOffset))
	idxPath := filepath.Join(s.dir, fmt.Sprintf("%020d.index", s.baseOffset))
	if err := os.Remove(logPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(idxPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
