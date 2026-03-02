package sstable

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thomazdavis/stratago/memtable"
)

func TestBuilder_Flush(t *testing.T) {
	filename := "test_level0.sst"
	defer os.Remove(filename)

	list := memtable.NewSkipList()
	list.Put([]byte("Barca:1"), []byte("Spain"))
	list.Put([]byte("Bayern:2"), []byte("Deutschland"))

	builder, err := NewBuilder(filename)
	assert.NoError(t, err)

	err = builder.Flush(list)
	assert.NoError(t, err)

	info, err := os.Stat(filename)
	assert.NoError(t, err)
	assert.True(t, info.Size() > 0, "SSTable file should not be empty")
}

func TestEngine_RotationSimulation(t *testing.T) {
	sstFile := "test_rotation.sst"
	walFile := "test_rotation.log"
	defer os.Remove(sstFile)
	defer os.Remove(walFile)

	list := memtable.NewSkipList()
	list.Put([]byte("key"), []byte("val"))

	builder, err := NewBuilder(sstFile)
	assert.NoError(t, err)
	err = builder.Flush(list)
	assert.NoError(t, err)

	f, _ := os.Create(walFile)
	f.Close()

	err = os.Remove(walFile)
	assert.NoError(t, err, "Should be able to delete old WAL")
}

func TestBuilder_CleanupOnFailure(t *testing.T) {
	filename := "failed_write.sst"
	defer os.Remove(filename)

	builder, _ := NewBuilder(filename)

	// Force a failure by closing the file handle before Flush
	builder.file.Close()

	list := memtable.NewSkipList()
	list.Put([]byte("k"), []byte("v"))

	err := builder.Flush(list)
	assert.Error(t, err)

	// Verify the .tmp file was cleaned up
	_, err = os.Stat(builder.tmpFilename)
	assert.True(t, os.IsNotExist(err), "Temp file should be removed on error")

	// Verify the final file was never created
	_, err = os.Stat(filename)
	assert.True(t, os.IsNotExist(err), "Final file should not exist on error")
}

func TestBuilder_EmptyFlush(t *testing.T) {
	filename := "empty.sst"
	defer os.Remove(filename)

	builder, _ := NewBuilder(filename)
	list := memtable.NewSkipList() // Empty list

	err := builder.Flush(list)
	assert.NoError(t, err)

	// Reader should handle empty files gracefully
	reader, err := NewReader(filename)
	assert.NoError(t, err)
	defer reader.Close()

	_, found := reader.Get([]byte("any"))
	assert.False(t, found)
}
