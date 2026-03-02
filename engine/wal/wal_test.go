package wal

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWAL_WriteAndRecover(t *testing.T) {
	filename := "test_wal.log"
	defer os.Remove(filename)

	w, err := NewWAL(filename)
	assert.NoError(t, err)

	err = w.WriteEntry([]byte("user:101"), []byte("Thomas"))
	assert.NoError(t, err)

	err = w.WriteEntry([]byte("user:102"), []byte("Davis"))
	assert.NoError(t, err)

	// Close the WAL (Simulate Shutdown)
	err = w.Close()
	assert.NoError(t, err)

	// Re-open and Recover (Simulate Restart)
	w2, err := NewWAL(filename)
	assert.NoError(t, err)
	defer w2.Close()

	restoredData, err := w2.Recover()
	assert.NoError(t, err)

	// Verify Data Integrity
	assert.Equal(t, 2, len(restoredData))
	assert.Equal(t, []byte("Thomas"), restoredData["user:101"])
	assert.Equal(t, []byte("Davis"), restoredData["user:102"])
}

func TestWAL_Recovery_Corruption(t *testing.T) {
	filename := "corrupt_wal.log"
	defer os.Remove(filename)

	w, _ := NewWAL(filename)
	w.WriteEntry([]byte("valid_key"), []byte("value"))
	w.Close()

	// Manually corrupt the file by appending junk
	f, _ := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0644)
	f.Write([]byte{0xFF, 0x00, 0x11}) // Partial/Garbage header
	f.Close()

	w2, _ := NewWAL(filename)
	data, err := w2.Recover()

	// Should not error, but should stop at the point of corruption
	assert.NoError(t, err)
	assert.Equal(t, 1, len(data))
	assert.Equal(t, []byte("value"), data["valid_key"])
}
