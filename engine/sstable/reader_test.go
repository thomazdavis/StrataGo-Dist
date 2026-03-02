package sstable

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thomazdavis/stratago/memtable"
)

func TestReader_Get(t *testing.T) {
	filename := "test_read.sst"
	defer os.Remove(filename)

	list := memtable.NewSkipList()
	list.Put([]byte("Td:1"), []byte("Camp Zoo"))
	list.Put([]byte("J:5"), []byte("M&T Bank Stadium"))

	builder, _ := NewBuilder(filename)
	builder.Flush(list)

	reader, err := NewReader(filename)
	assert.NoError(t, err)
	defer reader.Close()

	val, found := reader.Get([]byte("Td:1"))
	assert.True(t, found)
	assert.Equal(t, []byte("Camp Zoo"), val)

	val, found = reader.Get([]byte("J:5"))
	assert.True(t, found)
	assert.Equal(t, []byte("M&T Bank Stadium"), val)

	_, found = reader.Get([]byte("ATM:99"))
	assert.False(t, found)
}

func TestReader_SparseIndex_LargeFile(t *testing.T) {
	// Create a file larger than IndexInterval (1024 bytes)
	filename := "test_sparse.sst"
	defer os.Remove(filename)

	list := memtable.NewSkipList()

	// Write 100 entries. Assuming ~20-30 bytes per entry, this is ~2KB-3KB.
	// This ensures we generate at least 1 or 2 index entries.
	for i := range 100 {
		key := []byte(fmt.Sprintf("key-%03d", i)) // key-000 to key-099
		val := []byte(fmt.Sprintf("val-%03d", i))
		list.Put(key, val)
	}

	builder, err := NewBuilder(filename)
	assert.NoError(t, err)
	err = builder.Flush(list)
	assert.NoError(t, err)

	// Open Reader (Should load Index from Footer)
	reader, err := NewReader(filename)
	assert.NoError(t, err)
	defer reader.Close()

	// Verify Index was loaded
	// We expect > 0 entries if file size > 1024 bytes
	assert.Greater(t, len(reader.index), 0, "Sparse index should have entries for large files")

	// Test Gets (Jumping via Index)

	// Test First Item
	val, found := reader.Get([]byte("key-000"))
	assert.True(t, found)
	assert.Equal(t, []byte("val-000"), val)

	// Test Last Item (Requires scanning from last index point)
	val, found = reader.Get([]byte("key-099"))
	assert.True(t, found)
	assert.Equal(t, []byte("val-099"), val)

	// Test Middle Item
	val, found = reader.Get([]byte("key-050"))
	assert.True(t, found)
	assert.Equal(t, []byte("val-050"), val)
}

func TestReader_EarlyExit_CheckOffset(t *testing.T) {
	filename := "test_offset_check.sst"
	defer os.Remove(filename)

	list := memtable.NewSkipList()
	list.Put([]byte("A"), []byte("val")) // Read 1
	list.Put([]byte("C"), []byte("val")) // Read 2 (Stop here)
	list.Put([]byte("E"), []byte("val")) // Should NOT Read
	list.Put([]byte("G"), []byte("val")) // Should NOT Read

	builder, _ := NewBuilder(filename)
	builder.Flush(list)

	reader, _ := NewReader(filename)
	defer reader.Close()

	// Search for "B" (Between A and C)
	reader.Get([]byte("B"))

	// Check where the file pointer stopped
	currentPos, _ := reader.file.Seek(0, 1) // 1 = SeekCurrent

	stat, _ := reader.file.Stat()

	assert.True(t, currentPos < stat.Size()-8, "File pointer should assume early exit and not reach EOF")
}

func BenchmarkReader_Get(b *testing.B) {
	filename := "bench_read.sst"
	defer os.Remove(filename)

	list := memtable.NewSkipList()
	// Generate 10,000 items
	for i := 0; i < 10000; i++ {
		list.Put([]byte(fmt.Sprintf("key-%05d", i)), []byte("value"))
	}

	builder, _ := NewBuilder(filename)
	builder.Flush(list)

	reader, _ := NewReader(filename)
	defer reader.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// look for the last key (Worst case for linear scan)
		reader.Get([]byte("key-09999"))
	}
}

func BenchmarkReader_BloomFilter_Speedup(b *testing.B) {
	filename := "bench_bloom.sst"
	defer os.Remove(filename)

	list := memtable.NewSkipList()
	// Insert 10,000 keys
	for i := 0; i < 10000; i++ {
		list.Put([]byte(fmt.Sprintf("user-%05d", i)), []byte("FCB"))
	}

	builder, _ := NewBuilder(filename)
	builder.Flush(list)

	reader, _ := NewReader(filename)
	defer reader.Close()

	b.ResetTimer()

	// Searching for a key that does exist
	// The engine is forced to do a disk Seek() and Read()
	b.Run("ExistingKey_DiskIO", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			reader.Get([]byte("user-05000"))
		}
	})

	// Searching for a key that doesn't exist.
	// the bloom filter says no, so the engine instantly returns false without touching the disk
	b.Run("MissingKey_BloomFilterSkip", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			reader.Get([]byte("user-99999"))
		}
	})
}
