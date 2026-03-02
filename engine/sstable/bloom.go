package sstable

import (
	"github.com/spaolacci/murmur3"
)

// We use 10 bits per key, which gives us a False Positive rate of roughly 1%
// We use 7 hash probes per key (the mathematical optimal for 10 bits-per-key)
const (
	bitsPerKey = 10
	kProbes    = 7
)

type BloomFilter struct {
	bitset []byte
}

// NewBloomFilter creates a new filter optimally sized for the given number of keys.
func NewBloomFilter(numKeys int) *BloomFilter {
	if numKeys < 64 {
		numKeys = 64
	}

	bits := numKeys * bitsPerKey
	bytes := (bits + 7) / 8

	return &BloomFilter{
		bitset: make([]byte, bytes),
	}
}

// Add inserts a key into the Bloom Filter.
func (bf *BloomFilter) Add(key []byte) {
	// Redundancy removed: Just calculate the hash and pass it to AddHash
	bf.AddHash(HashKey(key))
}

// MayContain checks if a key might be in the filter
// If it returns false, the key is not in the database for sure
func (bf *BloomFilter) MayContain(key []byte) bool {
	if len(bf.bitset) == 0 {
		return true // If no filter exists, assume it might contain the key
	}

	h := HashKey(key)
	delta := (h >> 17) | (h << 15)
	bitLength := uint32(len(bf.bitset) * 8)

	for i := 0; i < kProbes; i++ {
		bitPos := h % bitLength

		bytePos := bitPos / 8
		bitOffset := bitPos % 8

		// Even if one bit is zero, the key is absent
		if (bf.bitset[bytePos] & (1 << bitOffset)) == 0 {
			return false
		}

		h += delta
	}

	return true
}

// Bytes returns the raw byte slice so it can be written to the SSTable disk file.
func (bf *BloomFilter) Bytes() []byte {
	return bf.bitset
}

// LoadBloomFilter creates a BloomFilter object from raw bytes read from the disk.
func LoadBloomFilter(data []byte) *BloomFilter {
	return &BloomFilter{
		bitset: data,
	}
}

// HashKey generates a 32-bit Murmur3 hash of the key.
func HashKey(key []byte) uint32 {
	return murmur3.Sum32(key)
}

// AddHash inserts a pre-calculated hash into the filter.
// This allows us to build the filter without holding full keys in memory.
func (bf *BloomFilter) AddHash(h uint32) {
	delta := (h >> 17) | (h << 15)
	bitLength := uint32(len(bf.bitset) * 8)

	for i := 0; i < kProbes; i++ {
		bitPos := h % bitLength
		bf.bitset[bitPos/8] |= (1 << (bitPos % 8))
		h += delta
	}
}
