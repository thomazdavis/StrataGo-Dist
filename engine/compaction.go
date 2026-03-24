package stratago

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/thomazdavis/stratago-dist/engine/sstable"
	"github.com/thomazdavis/stratago-dist/metrics"
)

const CompactionThreshold = 4

// compactionWorker runs in the background and checks for compaction opportunities
func (db *StrataGo) compactionWorker() {
	defer db.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := db.RunCompaction(); err != nil {
				fmt.Printf("Compaction failed: %v\n", err)
			}
		case <-db.closeChan:
			return
		}
	}
}

// getTier returns a bucket index based on file size
func getTier(size int64) int {
	mb := int64(1024 * 1024)
	if size < 10*mb {
		return 0 // Tier 0: < 10MB
	} else if size < 50*mb {
		return 1 // Tier 1: 10MB - 50MB
	} else if size < 250*mb {
		return 2 // Tier 2: 50MB - 250MB
	} else if size < 1024*mb {
		return 3 // Tier 3: 250MB - 1GB
	}
	return 4 // Tier 4: > 1GB
}

// RunCompaction executes a Size-Tiered compaction job
func (db *StrataGo) RunCompaction() error {
	// Select the files
	filesToCompact, currentTier := db.selectFilesForCompaction()

	// If we didn't find any valid group, abort gracefully
	if len(filesToCompact) == 0 {
		return nil
	}

	fmt.Printf("Starting compaction on Tier %d (Merging %d files)...\n", currentTier, len(filesToCompact))

	// Iterators (Newest to Oldest)
	var iters []*sstable.Iterator
	for i := len(filesToCompact) - 1; i >= 0; i-- {
		iter, err := filesToCompact[i].NewIterator()
		if err != nil {
			return fmt.Errorf("failed to create iterator: %w", err)
		}
		iters = append(iters, iter)
	}

	var newestTimestamp int64
	fmt.Sscanf(filepath.Base(filesToCompact[len(filesToCompact)-1].Path()), "data_%d.sst", &newestTimestamp)
	mergedSSTName := fmt.Sprintf("data_%d.sst", newestTimestamp)
	mergedSSTPath := filepath.Join(db.dataDir, mergedSSTName)

	builder, err := sstable.NewBuilder(mergedSSTPath)
	if err != nil {
		return err
	}

	if err := sstable.Merge(iters, builder); err != nil {
		return fmt.Errorf("merge failed: %w", err)
	}

	newReader, err := sstable.NewReader(mergedSSTPath)
	if err != nil {
		return err
	}

	db.mu.Lock()

	// Lookup map of the exact memory pointers just merged
	compactedMap := make(map[*sstable.Reader]bool)
	for _, r := range filesToCompact {
		compactedMap[r] = true
	}

	// Rebuild the new array
	var newReaders []*sstable.Reader
	inserted := false

	for _, reader := range db.sstReaders {
		if compactedMap[reader] {
			// Insert the newly merged file exactly where the old block started to preserve time-series order.
			if !inserted {
				newReaders = append(newReaders, newReader)
				inserted = true
			}
			continue
		}
		newReaders = append(newReaders, reader)
	}

	db.sstReaders = newReaders

	// Update the Prometheus Gauge to reflect the deleted files
	metrics.SSTableCount.WithLabelValues(metrics.NodeID).Set(float64(len(db.sstReaders)))

	db.mu.Unlock()

	for _, it := range iters {
		it.Close()
	}

	// Delete the old files from disk
	for _, r := range filesToCompact {
		oldPath := r.Path()
		r.Close()
		os.Remove(oldPath)
	}

	for _, it := range iters {
		it.Close()
	}

	fmt.Println("Compaction complete!")
	return nil
}

// selectFilesForCompaction scans the current SSTables and finds a contiguous group
// of files in the same size tier. Returns the files to compact, and the tier
func (db *StrataGo) selectFilesForCompaction() ([]*sstable.Reader, int) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var filesToCompact []*sstable.Reader
	currentTier := -1
	var currentGroup []*sstable.Reader

	for _, r := range db.sstReaders {
		stat, err := os.Stat(r.Path())
		if err != nil {
			continue
		}

		tier := getTier(stat.Size())

		if tier == currentTier {
			currentGroup = append(currentGroup, r)
			// If we hit our threshold of contiguous files in the same tier
			if len(currentGroup) == CompactionThreshold {
				filesToCompact = currentGroup
				return filesToCompact, currentTier
			}
		} else {
			// Reset the group because the tier changed
			currentTier = tier
			currentGroup = []*sstable.Reader{r}
		}
	}

	// Return nil if no valid group was found
	return nil, -1
}
