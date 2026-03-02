package stratago

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/thomazdavis/stratago/memtable"
	"github.com/thomazdavis/stratago/sstable"
	"github.com/thomazdavis/stratago/wal"
)

func (db *StrataGo) Flush() error {
	db.mu.Lock()

	if db.activeMemtable.Size == 0 && db.immutableMemtable == nil {
		db.mu.Unlock()
		return nil
	}

	// Only rotate if we don't have pending data
	if db.immutableMemtable == nil {
		// Rotate Memtable and WAL
		db.immutableMemtable = db.activeMemtable
		db.activeMemtable = memtable.NewSkipList()

		oldWAL := db.wal
		if err := oldWAL.Close(); err != nil {
			db.activeMemtable = db.immutableMemtable
			db.immutableMemtable = nil
			db.mu.Unlock()
			return err
		}

		flushingWALPath := filepath.Join(db.dataDir, "wal.log.flushing")
		if err := os.Rename(filepath.Join(db.dataDir, "wal.log"), flushingWALPath); err != nil {
			db.activeMemtable = db.immutableMemtable
			db.immutableMemtable = nil
			db.mu.Unlock()
			return err
		}

		newWal, err := wal.NewWAL(filepath.Join(db.dataDir, "wal.log"))
		if err != nil {
			os.Rename(flushingWALPath, filepath.Join(db.dataDir, "wal.log"))
			db.activeMemtable = db.immutableMemtable
			db.immutableMemtable = nil
			db.mu.Unlock()
			return err
		}

		db.wal = newWal
	}

	db.mu.Unlock()

	sstName := fmt.Sprintf("data_%d.sst", time.Now().UnixNano())
	sstPath := filepath.Join(db.dataDir, sstName)

	builder, err := sstable.NewBuilder(sstPath)
	if err != nil {
		return db.recoverFromFlushFailure(err)
	}

	if err := builder.Flush(db.immutableMemtable); err != nil {
		return db.recoverFromFlushFailure(err)
	}

	reader, err := sstable.NewReader(sstPath)
	if err != nil {
		return err
	}

	verifyData, err := reader.ReadAll()
	if err != nil {
		reader.Close()
		os.Remove(sstPath)
		return db.recoverFromFlushFailure(fmt.Errorf("SSTable verification failed: %w", err))
	}

	expectedSize := db.immutableMemtable.Size
	if len(verifyData) != expectedSize {
		reader.Close()
		os.Remove(sstPath)
		return db.recoverFromFlushFailure(fmt.Errorf("SSTable size mismatch: expected %d, got %d", expectedSize, len(verifyData)))
	}

	db.mu.Lock()
	db.sstReaders = append(db.sstReaders, reader)
	db.immutableMemtable = nil
	db.mu.Unlock()

	os.Remove(filepath.Join(db.dataDir, "wal.log.flushing"))
	return nil
}

func (db *StrataGo) recoverFromFlushFailure(originalErr error) error {
	db.mu.RLock()
	iter := db.immutableMemtable.NewIterator()
	db.mu.RUnlock()

	for iter.Next() {
		if err := db.wal.WriteEntry(iter.Key(), iter.Value()); err != nil {
			fmt.Printf("CRITICAL: Failed to persist to WAL: %v\n", err)
		}
	}

	// Clear immutable so we can flush again later
	db.mu.Lock()
	db.immutableMemtable = nil
	db.mu.Unlock()

	return fmt.Errorf("flush failed, data preserved: %w", originalErr)
}
