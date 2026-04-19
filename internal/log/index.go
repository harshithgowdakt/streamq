package log

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
)

const (
	indexEntrySize = 12 // 4-byte relative offset + 8-byte position
	indexMaxSize   = 10 * 1024 * 1024 // 10 MB max index file
)

// Index is a memory-mapped sparse index mapping relative offsets to file positions.
// Each entry is 12 bytes: uint32 relativeOffset + uint64 position.
type Index struct {
	file *os.File
	mmap []byte
	size int64 // current written size in bytes
	path string
}

// NewIndex creates or opens an index file. The file is pre-allocated and mmap'd.
func NewIndex(path string) (*Index, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open index: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat index: %w", err)
	}

	currentSize := fi.Size()

	// Truncate to max size for mmap (sparse file, won't use disk)
	if err := f.Truncate(int64(indexMaxSize)); err != nil {
		f.Close()
		return nil, fmt.Errorf("truncate index: %w", err)
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, indexMaxSize,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("mmap index: %w", err)
	}

	return &Index{
		file: f,
		mmap: data,
		size: currentSize,
		path: path,
	}, nil
}

// Write appends an index entry.
func (idx *Index) Write(relOffset uint32, pos uint64) error {
	if idx.size+indexEntrySize > int64(indexMaxSize) {
		return fmt.Errorf("index full")
	}

	off := idx.size
	binary.BigEndian.PutUint32(idx.mmap[off:], relOffset)
	binary.BigEndian.PutUint64(idx.mmap[off+4:], pos)
	idx.size += indexEntrySize
	return nil
}

// Lookup finds the greatest entry whose relative offset is <= target.
// Returns the file position, or -1 if no entry qualifies.
func (idx *Index) Lookup(targetRelOffset uint32) (pos int64) {
	entries := idx.size / indexEntrySize
	if entries == 0 {
		return -1
	}

	lo, hi := int64(0), entries-1
	result := int64(-1)

	for lo <= hi {
		mid := lo + (hi-lo)/2
		off := mid * indexEntrySize
		relOff := binary.BigEndian.Uint32(idx.mmap[off:])

		if relOff <= targetRelOffset {
			result = int64(binary.BigEndian.Uint64(idx.mmap[off+4:]))
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	return result
}

// Entries returns the number of entries in the index.
func (idx *Index) Entries() int64 {
	return idx.size / indexEntrySize
}

// Close syncs and unmaps the index, then truncates to actual size.
func (idx *Index) Close() error {
	if err := syscall.Munmap(idx.mmap); err != nil {
		return fmt.Errorf("munmap: %w", err)
	}
	if err := idx.file.Truncate(idx.size); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	return idx.file.Close()
}

// SanitizeForRecovery truncates the index to a valid size (multiple of entry size).
func (idx *Index) SanitizeForRecovery(validEntries int64) {
	idx.size = validEntries * indexEntrySize
}
