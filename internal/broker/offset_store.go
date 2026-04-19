package broker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// OffsetAndMetadata holds a committed offset with optional metadata.
type OffsetAndMetadata struct {
	Offset   int64  `json:"offset"`
	Metadata string `json:"metadata,omitempty"`
}

// OffsetStore persists consumer group offsets to disk as JSON files.
type OffsetStore struct {
	mu      sync.RWMutex
	dataDir string // {broker.DataDir}/__consumer_offsets/
}

// NewOffsetStore creates an OffsetStore, ensuring the data directory exists.
func NewOffsetStore(dataDir string) *OffsetStore {
	dir := filepath.Join(dataDir, "__consumer_offsets")
	os.MkdirAll(dir, 0755)
	return &OffsetStore{dataDir: dir}
}

// offsetData is the JSON structure written per group.
type offsetData struct {
	// topic -> partition -> OffsetAndMetadata
	Offsets map[string]map[string]OffsetAndMetadata `json:"offsets"`
}

// Save writes offsets for a group to disk.
func (s *OffsetStore) Save(groupID string, offsets map[string]map[int32]OffsetAndMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Convert int32 keys to string for JSON
	data := offsetData{
		Offsets: make(map[string]map[string]OffsetAndMetadata),
	}
	for topic, parts := range offsets {
		partMap := make(map[string]OffsetAndMetadata)
		for p, om := range parts {
			key := partitionKey(p)
			partMap[key] = om
		}
		data.Offsets[topic] = partMap
	}

	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	path := filepath.Join(s.dataDir, groupID+".json")
	return os.WriteFile(path, b, 0644)
}

// Load reads offsets for a group from disk.
func (s *OffsetStore) Load(groupID string) (map[string]map[int32]OffsetAndMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := filepath.Join(s.dataDir, groupID+".json")
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var data offsetData
	if err := json.Unmarshal(b, &data); err != nil {
		return nil, err
	}

	result := make(map[string]map[int32]OffsetAndMetadata)
	for topic, parts := range data.Offsets {
		partMap := make(map[int32]OffsetAndMetadata)
		for pStr, om := range parts {
			p := parsePartitionKey(pStr)
			partMap[p] = om
		}
		result[topic] = partMap
	}

	return result, nil
}

func partitionKey(p int32) string {
	// Simple int to string
	buf := make([]byte, 0, 10)
	buf = appendInt32(buf, p)
	return string(buf)
}

func appendInt32(buf []byte, v int32) []byte {
	return append(buf, []byte(int32ToString(v))...)
}

func int32ToString(v int32) string {
	if v == 0 {
		return "0"
	}
	neg := false
	if v < 0 {
		neg = true
		v = -v
	}
	var digits [10]byte
	i := len(digits)
	for v > 0 {
		i--
		digits[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		digits[i] = '-'
	}
	return string(digits[i:])
}

func parsePartitionKey(s string) int32 {
	var v int32
	neg := false
	i := 0
	if len(s) > 0 && s[0] == '-' {
		neg = true
		i = 1
	}
	for ; i < len(s); i++ {
		v = v*10 + int32(s[i]-'0')
	}
	if neg {
		v = -v
	}
	return v
}
