package stratago

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/thomazdavis/stratago/memtable"
	"github.com/thomazdavis/stratago/sstable"
	"github.com/thomazdavis/stratago/wal"
)

const DefaultMemtableThreshold = 4 * 1024 * 1024 // 4MB

type StrataGo struct {
	mu                sync.RWMutex
	activeMemtable    *memtable.SkipList
	immutableMemtable *memtable.SkipList
	wal               *wal.WAL
	sstReaders        []*sstable.Reader
	dataDir           string
	flushChan         chan struct{}
	closeChan         chan struct{}
	wg                sync.WaitGroup
	closed            bool
}

type sstableInfo struct {
	path      string
	timestamp int64
}

func Open(dataDir string) (*StrataGo, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	mem := memtable.NewSkipList()
	walPath := filepath.Join(dataDir, "wal.log")
	walLog, err := wal.NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	// Crash protection
	flushingPath := filepath.Join(dataDir, "wal.log.flushing")
	if _, err := os.Stat(flushingPath); err == nil {
		// Found an abandoned log, recovering it into the memory
		tempWAL, err := wal.NewWAL(flushingPath)
		if err == nil {
			restored, err := tempWAL.Recover()
			if err != nil {
				tempWAL.Close()
				fmt.Printf("Warning: partial recovery from flushing WAL: %v\n", err)
			}
			for k, v := range restored {
				mem.Put([]byte(k), v)

				if err := walLog.WriteEntry([]byte(k), v); err != nil {
					return nil, fmt.Errorf("failed to persist recovered data: %w", err)
				}
			}
			tempWAL.Close()
			os.Remove(flushingPath)
		}
	}

	restored, err := walLog.Recover()
	if err != nil {
		return nil, fmt.Errorf("WAL recovery failed: %w", err)
	}

	for k, v := range restored {
		mem.Put([]byte(k), v)
	}

	files, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read data directory: %w", err)
	}
	var sstables []sstableInfo

	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".sst") {
			var ts int64
			fmt.Sscanf(f.Name(), "data_%d.sst", &ts)
			sstables = append(sstables, sstableInfo{
				path:      filepath.Join(dataDir, f.Name()),
				timestamp: ts,
			})
		}
	}

	sort.Slice(sstables, func(i, j int) bool {
		return sstables[i].timestamp < sstables[j].timestamp
	})

	var readers []*sstable.Reader
	for _, sst := range sstables {
		r, err := sstable.NewReader(sst.path)
		if err == nil {
			readers = append(readers, r)
		}
	}

	db := &StrataGo{
		activeMemtable: mem,
		wal:            walLog,
		sstReaders:     readers,
		dataDir:        dataDir,
		flushChan:      make(chan struct{}, 1),
		closeChan:      make(chan struct{}),
		closed:         false,
	}

	db.wg.Add(2) // two worker - flush + compaction
	go db.flushWorker()
	go db.compactionWorker()

	return db, nil
}

func (db *StrataGo) Put(key, value []byte) error {

	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return fmt.Errorf("database is closed")
	}
	db.mu.Unlock()

	if err := db.wal.WriteEntry(key, value); err != nil {
		return err
	}

	db.mu.Lock()
	db.activeMemtable.Put(key, value)

	needsFlush := db.activeMemtable.SizeBytes >= DefaultMemtableThreshold

	if needsFlush {
		select {
		case db.flushChan <- struct{}{}:
			// Signal to flush sent
		default:
			// Channel is full (Flush already pending/running)
			// Ignoring request
		}
	}
	db.mu.Unlock()

	return nil
}

func (db *StrataGo) Get(key []byte) ([]byte, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if val, found := db.activeMemtable.Get(key); found {
		if val == nil {
			return nil, false
		}
		return val, true
	}

	if db.immutableMemtable != nil {
		if val, found := db.immutableMemtable.Get(key); found {
			if val == nil {
				return nil, false
			}
			return val, true
		}
	}

	for i := len(db.sstReaders) - 1; i >= 0; i-- {
		if val, found := db.sstReaders[i].Get(key); found {
			if len(val) == 0 {
				return nil, false
			}
			return val, true
		}
	}
	return nil, false
}

// Delete marks a key as deleted by inserting a tombstone
func (db *StrataGo) Delete(key []byte) error {

	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return fmt.Errorf("database is closed")
	}
	db.mu.Unlock()

	// Writing the deletion to the WAL with value nil
	if err := db.wal.WriteEntry(key, nil); err != nil {
		return err
	}

	db.mu.Lock()
	db.activeMemtable.Put(key, nil)

	needsFlush := db.activeMemtable.SizeBytes >= DefaultMemtableThreshold
	if needsFlush {
		select {
		case db.flushChan <- struct{}{}:
		default:
		}
	}
	db.mu.Unlock()

	return nil
}

func (db *StrataGo) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	db.mu.Unlock()

	if err := db.Flush(); err != nil {
		return fmt.Errorf("final flush on close failed: %w", err)
	}

	select {
	case <-db.flushChan:
	default:
	}

	close(db.flushChan)
	close(db.closeChan)
	db.wg.Wait() // Wait for any in-progress flush to finish

	db.mu.Lock()
	defer db.mu.Unlock()

	db.wal.Close()
	for _, r := range db.sstReaders {
		r.Close()
	}
	return nil
}

// Purge closes the database, deletes all data files, and restarts the engine.
func (db *StrataGo) Purge() error {
	if err := db.Close(); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := os.RemoveAll(db.dataDir); err != nil {
		return err
	}
	if err := os.MkdirAll(db.dataDir, 0755); err != nil {
		return err
	}

	// Re-initialize Memory and WAL
	db.activeMemtable = memtable.NewSkipList()
	db.immutableMemtable = nil
	db.sstReaders = nil // Reset readers slice

	walPath := filepath.Join(db.dataDir, "wal.log")
	newWal, err := wal.NewWAL(walPath)
	if err != nil {
		return err
	}
	db.wal = newWal

	// Restarting worker
	db.flushChan = make(chan struct{}, 1)
	db.closeChan = make(chan struct{})
	db.closed = false
	db.wg.Add(2)
	go db.flushWorker()
	go db.compactionWorker()

	return nil
}

func (db *StrataGo) flushWorker() {
	defer db.wg.Done()

	for range db.flushChan {
		if err := db.Flush(); err != nil {
			fmt.Printf("Background flush failed: %v\n", err)
		}
	}
}
