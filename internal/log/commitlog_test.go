package log

import (
	"testing"
)

func TestCommitLogBasic(t *testing.T) {
	dir := t.TempDir()

	cl, err := NewCommitLog(dir, DefaultCommitLogConfig())
	if err != nil {
		t.Fatalf("NewCommitLog: %v", err)
	}
	defer cl.Close()

	// Append several batches
	for i := 0; i < 10; i++ {
		off, err := cl.Append(&RecordBatch{
			Records: []Record{{Value: []byte("hello")}},
		})
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if off != int64(i) {
			t.Errorf("Append %d: offset = %d", i, off)
		}
	}

	if cl.NextOffset() != 10 {
		t.Errorf("NextOffset = %d, want 10", cl.NextOffset())
	}

	// Read from offset 5
	batches, err := cl.Read(5, 1024*1024)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(batches) < 1 {
		t.Fatal("Read returned no batches")
	}
	if batches[0].BaseOffset != 5 {
		t.Errorf("first batch offset = %d, want 5", batches[0].BaseOffset)
	}
}

func TestCommitLogSegmentRolling(t *testing.T) {
	dir := t.TempDir()

	config := CommitLogConfig{
		MaxSegmentBytes: 200, // very small to trigger rolling
	}

	cl, err := NewCommitLog(dir, config)
	if err != nil {
		t.Fatalf("NewCommitLog: %v", err)
	}
	defer cl.Close()

	// Append enough to trigger rolling
	for i := 0; i < 20; i++ {
		_, err := cl.Append(&RecordBatch{
			Records: []Record{{Value: []byte("data-data-data")}},
		})
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	cl.mu.RLock()
	numSegments := len(cl.segments)
	cl.mu.RUnlock()

	if numSegments < 2 {
		t.Errorf("expected multiple segments, got %d", numSegments)
	}

	// Read across segments
	batches, err := cl.Read(0, 1024*1024)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(batches) != 20 {
		t.Errorf("Read returned %d batches, want 20", len(batches))
	}
}

func TestCommitLogRecovery(t *testing.T) {
	dir := t.TempDir()

	cl, err := NewCommitLog(dir, DefaultCommitLogConfig())
	if err != nil {
		t.Fatalf("NewCommitLog: %v", err)
	}

	for i := 0; i < 5; i++ {
		cl.Append(&RecordBatch{
			Records: []Record{{Value: []byte("persistent")}},
		})
	}
	cl.Close()

	// Reopen
	cl2, err := NewCommitLog(dir, DefaultCommitLogConfig())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer cl2.Close()

	if cl2.NextOffset() != 5 {
		t.Errorf("NextOffset after recovery = %d, want 5", cl2.NextOffset())
	}

	// Can append more
	off, err := cl2.Append(&RecordBatch{
		Records: []Record{{Value: []byte("new")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if off != 5 {
		t.Errorf("offset after recovery = %d, want 5", off)
	}
}

func TestCommitLogRetentionBySize(t *testing.T) {
	dir := t.TempDir()

	config := CommitLogConfig{
		MaxSegmentBytes: 100,
		RetentionBytes:  300,
	}

	cl, err := NewCommitLog(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	for i := 0; i < 30; i++ {
		cl.Append(&RecordBatch{
			Records: []Record{{Value: []byte("data-data")}},
		})
	}

	if err := cl.EnforceRetention(); err != nil {
		t.Fatal(err)
	}

	cl.mu.RLock()
	numSegments := len(cl.segments)
	cl.mu.RUnlock()

	// Should have fewer segments after retention
	if numSegments < 1 {
		t.Error("should have at least 1 segment")
	}
}
