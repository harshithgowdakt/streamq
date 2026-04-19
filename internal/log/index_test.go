package log

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIndexWriteAndLookup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")

	idx, err := NewIndex(path)
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	defer idx.Close()

	// Write some entries
	entries := []struct {
		relOffset uint32
		pos       uint64
	}{
		{0, 0},
		{5, 1000},
		{10, 2000},
		{15, 3000},
	}

	for _, e := range entries {
		if err := idx.Write(e.relOffset, e.pos); err != nil {
			t.Fatalf("Write(%d, %d): %v", e.relOffset, e.pos, err)
		}
	}

	if idx.Entries() != 4 {
		t.Errorf("Entries() = %d, want 4", idx.Entries())
	}

	// Lookup exact matches
	if pos := idx.Lookup(0); pos != 0 {
		t.Errorf("Lookup(0) = %d, want 0", pos)
	}
	if pos := idx.Lookup(10); pos != 2000 {
		t.Errorf("Lookup(10) = %d, want 2000", pos)
	}

	// Lookup between entries (should return floor)
	if pos := idx.Lookup(7); pos != 1000 {
		t.Errorf("Lookup(7) = %d, want 1000", pos)
	}
	if pos := idx.Lookup(20); pos != 3000 {
		t.Errorf("Lookup(20) = %d, want 3000", pos)
	}
}

func TestIndexPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist.index")

	idx, err := NewIndex(path)
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	idx.Write(0, 0)
	idx.Write(10, 5000)
	idx.Close()

	// Reopen
	idx2, err := NewIndex(path)
	if err != nil {
		t.Fatalf("reopen NewIndex: %v", err)
	}
	defer idx2.Close()

	if idx2.Entries() != 2 {
		t.Errorf("Entries() = %d after reopen, want 2", idx2.Entries())
	}
	if pos := idx2.Lookup(10); pos != 5000 {
		t.Errorf("Lookup(10) = %d after reopen, want 5000", pos)
	}
}

func TestIndexEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.index")

	idx, err := NewIndex(path)
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	defer idx.Close()

	if pos := idx.Lookup(0); pos != -1 {
		t.Errorf("Lookup on empty index = %d, want -1", pos)
	}
}

func TestIndexCleanup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cleanup.index")

	idx, err := NewIndex(path)
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	idx.Write(0, 0)
	idx.Close()

	// Verify file was truncated to actual size
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if fi.Size() != indexEntrySize {
		t.Errorf("file size = %d, want %d", fi.Size(), indexEntrySize)
	}
}
