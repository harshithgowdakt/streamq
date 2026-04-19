package log

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// CommitLogConfig holds configuration for a CommitLog.
type CommitLogConfig struct {
	MaxSegmentBytes int64         // max size of a single segment before rolling
	RetentionBytes  int64         // max total size (0 = unlimited)
	RetentionAge    time.Duration // max age of segments (0 = unlimited)
}

// DefaultCommitLogConfig returns sensible defaults.
func DefaultCommitLogConfig() CommitLogConfig {
	return CommitLogConfig{
		MaxSegmentBytes: 1024 * 1024 * 1024, // 1 GB
		RetentionBytes:  0,
		RetentionAge:    0,
	}
}

// CommitLog manages an ordered list of segments for a single partition.
type CommitLog struct {
	mu       sync.RWMutex
	dir      string
	config   CommitLogConfig
	segments []*Segment
	active   *Segment
}

// NewCommitLog creates or recovers a CommitLog in the given directory.
func NewCommitLog(dir string, config CommitLogConfig) (*CommitLog, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}

	cl := &CommitLog{
		dir:    dir,
		config: config,
	}

	if err := cl.loadSegments(); err != nil {
		return nil, err
	}

	// Create initial segment if none exist
	if len(cl.segments) == 0 {
		seg, err := NewSegment(dir, 0)
		if err != nil {
			return nil, fmt.Errorf("create initial segment: %w", err)
		}
		cl.segments = append(cl.segments, seg)
		cl.active = seg
	} else {
		cl.active = cl.segments[len(cl.segments)-1]
	}

	return cl, nil
}

// loadSegments discovers and opens existing segments from disk.
func (cl *CommitLog) loadSegments() error {
	entries, err := os.ReadDir(cl.dir)
	if err != nil {
		return fmt.Errorf("readdir: %w", err)
	}

	var baseOffsets []int64
	seen := make(map[int64]bool)

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		offsetStr := strings.TrimSuffix(name, ".log")
		offset, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			continue
		}
		if !seen[offset] {
			seen[offset] = true
			baseOffsets = append(baseOffsets, offset)
		}
	}

	sort.Slice(baseOffsets, func(i, j int) bool { return baseOffsets[i] < baseOffsets[j] })

	for _, bo := range baseOffsets {
		seg, err := NewSegment(cl.dir, bo)
		if err != nil {
			return fmt.Errorf("open segment %d: %w", bo, err)
		}
		cl.segments = append(cl.segments, seg)
	}

	return nil
}

// Append writes a RecordBatch to the active segment, rolling if needed.
func (cl *CommitLog) Append(batch *RecordBatch) (int64, error) {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	// Roll segment if active exceeds max size
	if cl.active.Size() >= cl.config.MaxSegmentBytes {
		if err := cl.rollSegment(); err != nil {
			return 0, fmt.Errorf("roll segment: %w", err)
		}
	}

	offset, err := cl.active.Append(batch)
	if err != nil {
		return 0, err
	}

	return offset, nil
}

// rollSegment creates a new segment starting at the active segment's next offset.
func (cl *CommitLog) rollSegment() error {
	nextBase := cl.active.NextOffset()
	seg, err := NewSegment(cl.dir, nextBase)
	if err != nil {
		return err
	}
	cl.segments = append(cl.segments, seg)
	cl.active = seg
	return nil
}

// Read reads batches starting from the given offset, returning up to maxBytes.
func (cl *CommitLog) Read(offset int64, maxBytes int) ([]*RecordBatch, error) {
	cl.mu.RLock()
	defer cl.mu.RUnlock()

	// Find the segment containing this offset
	segIdx := cl.findSegment(offset)
	if segIdx < 0 {
		return nil, nil
	}

	var allBatches []*RecordBatch
	bytesRemaining := maxBytes

	for i := segIdx; i < len(cl.segments) && bytesRemaining > 0; i++ {
		batches, err := cl.segments[i].Read(offset, bytesRemaining)
		if err != nil {
			return nil, err
		}
		for _, b := range batches {
			allBatches = append(allBatches, b)
			bytesRemaining -= b.BatchSize()
			if bytesRemaining <= 0 {
				break
			}
		}
		// Advance offset past this segment for the next iteration
		if len(batches) > 0 {
			last := batches[len(batches)-1]
			offset = last.BaseOffset + int64(len(last.Records))
		}
	}

	return allBatches, nil
}

// findSegment returns the index of the segment that could contain the given offset.
func (cl *CommitLog) findSegment(offset int64) int {
	n := len(cl.segments)
	if n == 0 {
		return -1
	}

	// Binary search: find last segment with baseOffset <= offset
	idx := sort.Search(n, func(i int) bool {
		return cl.segments[i].GetBaseOffset() > offset
	})
	idx-- // segment just before the one that's too high

	if idx < 0 {
		idx = 0
	}

	return idx
}

// NextOffset returns the next offset to be assigned.
func (cl *CommitLog) NextOffset() int64 {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	return cl.active.NextOffset()
}

// OldestOffset returns the base offset of the oldest segment.
func (cl *CommitLog) OldestOffset() int64 {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	if len(cl.segments) == 0 {
		return 0
	}
	return cl.segments[0].GetBaseOffset()
}

// EnforceRetention removes segments that violate retention policies.
func (cl *CommitLog) EnforceRetention() error {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	if len(cl.segments) <= 1 {
		return nil // never delete the active segment
	}

	// Retention by size
	if cl.config.RetentionBytes > 0 {
		var totalSize int64
		for _, s := range cl.segments {
			totalSize += s.Size()
		}

		for totalSize > cl.config.RetentionBytes && len(cl.segments) > 1 {
			old := cl.segments[0]
			totalSize -= old.Size()
			old.Close()
			old.Remove()
			cl.segments = cl.segments[1:]
		}
	}

	// Retention by age
	if cl.config.RetentionAge > 0 {
		cutoff := time.Now().Add(-cl.config.RetentionAge)
		for len(cl.segments) > 1 {
			logPath := filepath.Join(cl.dir, fmt.Sprintf("%020d.log", cl.segments[0].GetBaseOffset()))
			fi, err := os.Stat(logPath)
			if err != nil {
				break
			}
			if fi.ModTime().Before(cutoff) {
				old := cl.segments[0]
				old.Close()
				old.Remove()
				cl.segments = cl.segments[1:]
			} else {
				break
			}
		}
	}

	return nil
}

// Close closes all segments.
func (cl *CommitLog) Close() error {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	for _, s := range cl.segments {
		if err := s.Close(); err != nil {
			return err
		}
	}
	return nil
}
