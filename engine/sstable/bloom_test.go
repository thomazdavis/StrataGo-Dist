package sstable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBloomFilter_Basic(t *testing.T) {
	// Create a filter sized for 100 items
	bf := NewBloomFilter(100)

	keys := [][]byte{[]byte("apple"), []byte("banana"), []byte("cherry")}
	for _, k := range keys {
		bf.AddHash(HashKey(k))
	}

	// true positives
	for _, k := range keys {
		assert.True(t, bf.MayContain(k), "Filter must return true for inserted keys")
	}

	// true negatives (theoretically can be false positive)
	missing := [][]byte{[]byte("grape"), []byte("orange"), []byte("pear")}
	for _, k := range missing {
		assert.False(t, bf.MayContain(k), "Filter should return false for missing keys")
	}
}

func TestBloomFilter_Serialization(t *testing.T) {
	bf := NewBloomFilter(50)
	bf.AddHash(HashKey([]byte("strata")))
	bf.AddHash(HashKey([]byte("go")))

	// Convert to bytes (simulate writing to disk)
	data := bf.Bytes()

	// Load from bytes (simulate reading from disk)
	loadedBF := LoadBloomFilter(data)

	// Verify the loaded filter behaves identically
	assert.True(t, loadedBF.MayContain([]byte("strata")))
	assert.True(t, loadedBF.MayContain([]byte("go")))
	assert.False(t, loadedBF.MayContain([]byte("python")))
}

func TestBloomFilter_MinimumSize(t *testing.T) {
	// Requesting a tiny filter should enforce the 64-item minimum
	// (64 items * 10 bits = 640 bits = 80 bytes)
	bf := NewBloomFilter(5)

	assert.Equal(t, 80, len(bf.Bytes()), "Filter should enforce a minimum byte size to prevent high collision rates")
}
