package sstable

import (
	"bytes"
	"os"
	"testing"

	"github.com/thomazdavis/stratago/memtable"
)

func buildTestSSTable(filename string, kvs map[string]string) *Reader {
	list := memtable.NewSkipList()
	for k, v := range kvs {
		list.Put([]byte(k), []byte(v))
	}
	builder, _ := NewBuilder(filename)
	builder.Flush(list)
	reader, _ := NewReader(filename)
	return reader
}

func TestMerge_Deduplication(t *testing.T) {
	// Create 3 files with overlapping keys
	f1 := buildTestSSTable("test_oldest.sst", map[string]string{"A": "1", "B": "1"})
	defer os.Remove("test_oldest.sst")
	defer f1.Close()

	f2 := buildTestSSTable("test_middle.sst", map[string]string{"B": "2", "C": "2"})
	defer os.Remove("test_middle.sst")
	defer f2.Close()

	f3 := buildTestSSTable("test_newest.sst", map[string]string{"A": "3", "D": "3"})
	defer os.Remove("test_newest.sst")
	defer f3.Close()

	// Iterators ordered from newest to oldest
	iter3, _ := f3.NewIterator()
	iter2, _ := f2.NewIterator()
	iter1, _ := f1.NewIterator()
	iters := []*Iterator{iter3, iter2, iter1}

	// merging
	builder, _ := NewBuilder("test_merged.sst")
	err := Merge(iters, builder)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	defer os.Remove("test_merged.sst")

	mergedReader, _ := NewReader("test_merged.sst")
	defer mergedReader.Close()

	// Expected State:
	// A=3 (from newest, overrides oldest)
	// B=2 (from middle, overrides oldest)
	// C=2 (from middle)
	// D=3 (from newest)
	expected := map[string]string{
		"A": "3",
		"B": "2",
		"C": "2",
		"D": "3",
	}

	for key, expectedVal := range expected {
		val, found := mergedReader.Get([]byte(key))
		if !found {
			t.Errorf("Expected to find key %s, but didn't", key)
		}
		if !bytes.Equal(val, []byte(expectedVal)) {
			t.Errorf("Key %s: expected %s, got %s", key, expectedVal, string(val))
		}
	}
}
