package log

import (
	"testing"
)

func TestSegmentAppendAndRead(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	defer seg.Close()

	// Append a few batches
	for i := 0; i < 5; i++ {
		batch := &RecordBatch{
			Records: []Record{
				{Key: []byte("key"), Value: []byte("value")},
			},
		}
		off, err := seg.Append(batch)
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if off != int64(i) {
			t.Errorf("Append %d: offset = %d, want %d", i, off, i)
		}
	}

	if seg.NextOffset() != 5 {
		t.Errorf("NextOffset = %d, want 5", seg.NextOffset())
	}

	// Read all from offset 0
	batches, err := seg.Read(0, 1024*1024)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(batches) != 5 {
		t.Errorf("Read returned %d batches, want 5", len(batches))
	}

	// Read from offset 3
	batches, err = seg.Read(3, 1024*1024)
	if err != nil {
		t.Fatalf("Read(3): %v", err)
	}
	if len(batches) < 1 {
		t.Fatal("Read(3) returned no batches")
	}
	if batches[0].BaseOffset != 3 {
		t.Errorf("Read(3): first batch offset = %d, want 3", batches[0].BaseOffset)
	}
}

func TestSegmentRecovery(t *testing.T) {
	dir := t.TempDir()

	// Create and populate a segment
	seg, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}

	for i := 0; i < 3; i++ {
		seg.Append(&RecordBatch{
			Records: []Record{{Value: []byte("msg")}},
		})
	}
	seg.Close()

	// Reopen — should recover
	seg2, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer seg2.Close()

	if seg2.NextOffset() != 3 {
		t.Errorf("NextOffset after recovery = %d, want 3", seg2.NextOffset())
	}

	// Should be able to append more
	off, err := seg2.Append(&RecordBatch{
		Records: []Record{{Value: []byte("after-recovery")}},
	})
	if err != nil {
		t.Fatalf("Append after recovery: %v", err)
	}
	if off != 3 {
		t.Errorf("offset after recovery append = %d, want 3", off)
	}
}

func TestSegmentMultiRecordBatch(t *testing.T) {
	dir := t.TempDir()

	seg, err := NewSegment(dir, 0)
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	defer seg.Close()

	// Batch with 3 records
	off, err := seg.Append(&RecordBatch{
		Records: []Record{
			{Value: []byte("a")},
			{Value: []byte("b")},
			{Value: []byte("c")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if off != 0 {
		t.Errorf("first batch offset = %d, want 0", off)
	}

	// Next batch should start at offset 3
	off, err = seg.Append(&RecordBatch{
		Records: []Record{{Value: []byte("d")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if off != 3 {
		t.Errorf("second batch offset = %d, want 3", off)
	}
}
