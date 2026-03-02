package sstable

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"
)

type Reader struct {
	file   *os.File
	index  []IndexEntry
	filter *BloomFilter
	mu     sync.Mutex
}

// Opens an existing SSTable for reading
func NewReader(filename string) (*Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	r := &Reader{file: file}
	if err := r.loadIndex(); err != nil {
		file.Close()
		return nil, err
	}
	return r, nil
}

// loadIndex reads the Footer and then the Index Block
func (r *Reader) loadIndex() error {
	stat, err := r.file.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()

	if fileSize < 16 { // Footer fixed 16 bytes, 8 Sparse + 8 Bloom
		return nil
	}

	// Read Footer
	footer := make([]byte, 16)
	if _, err := r.file.ReadAt(footer, fileSize-16); err != nil {
		return err
	}
	indexOffset := int64(binary.LittleEndian.Uint64(footer[0:8]))
	filterOffset := int64(binary.LittleEndian.Uint64(footer[8:16]))

	// Read Bloom Filter block
	if _, err := r.file.Seek(filterOffset, 0); err != nil {
		return err
	}
	var filterLen uint32
	if err := binary.Read(r.file, binary.LittleEndian, &filterLen); err != nil {
		return err
	}
	filterData := make([]byte, filterLen)
	if _, err := io.ReadFull(r.file, filterData); err != nil {
		return err
	}
	r.filter = LoadBloomFilter(filterData)

	// Read Index block
	if _, err := r.file.Seek(indexOffset, 0); err != nil {
		return err
	}

	var numEntries uint32
	if err := binary.Read(r.file, binary.LittleEndian, &numEntries); err != nil {
		return err
	}
	r.index = make([]IndexEntry, numEntries)

	for i := 0; i < int(numEntries); i++ {
		var keyLen uint32
		if err := binary.Read(r.file, binary.LittleEndian, &keyLen); err != nil {
			return err
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(r.file, key); err != nil {
			return err
		}
		var offset int64
		if err := binary.Read(r.file, binary.LittleEndian, &offset); err != nil {
			return err
		}
		r.index[i] = IndexEntry{Key: key, Offset: offset}
	}
	return nil
}

func (r *Reader) Close() error {
	return r.file.Close()
}

// Get searches for a key in the SSTable
func (r *Reader) Get(searchKey []byte) ([]byte, bool) {
	if r.filter != nil && !r.filter.MayContain(searchKey) {
		return nil, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	startOffset := r.findIndexEntry(searchKey)

	_, err := r.file.Seek(startOffset, 0)
	if err != nil {
		return nil, false
	}

	for {
		// Read key size (4 bytes)
		var keySize uint32
		err := binary.Read(r.file, binary.LittleEndian, &keySize)
		if err == io.EOF {
			break // Key not found
		}
		if err != nil {
			return nil, false // file corrupted
		}

		var valSize uint32
		err = binary.Read(r.file, binary.LittleEndian, &valSize)
		if err != nil {
			return nil, false
		}

		// Read Key Payload
		key := make([]byte, keySize)
		if _, err := io.ReadFull(r.file, key); err != nil {
			return nil, false
		}

		cmp := bytes.Compare(key, searchKey)

		if cmp == 0 {
			val := make([]byte, valSize)
			if _, err := io.ReadFull(r.file, val); err != nil {
				return nil, false
			}
			return val, true
		} else if cmp > 0 {
			return nil, false
		}

		_, err = r.file.Seek(int64(valSize), 1)
		if err != nil {
			return nil, false
		}
	}
	return nil, false
}

func (r *Reader) Path() string {
	return r.file.Name()
}

// ReadAll retrieves all key-value pairs from the SSTable file.
func (r *Reader) ReadAll() (map[string][]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, err := r.file.Seek(0, 0); err != nil {
		return nil, err
	}

	data := make(map[string][]byte)

	stat, _ := r.file.Stat()
	fileSize := stat.Size()
	limit := fileSize

	if fileSize > 16 {
		footer := make([]byte, 16)
		r.file.ReadAt(footer, fileSize-16)
		limit = int64(binary.LittleEndian.Uint64(footer[8:16]))
	}

	// Reset seek
	r.file.Seek(0, 0)
	currentPos := int64(0)

	for currentPos < limit {
		var keySize, valSize uint32
		if err := binary.Read(r.file, binary.LittleEndian, &keySize); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if err := binary.Read(r.file, binary.LittleEndian, &valSize); err != nil {
			return nil, err
		}

		key := make([]byte, keySize)
		if _, err := io.ReadFull(r.file, key); err != nil {
			return nil, err
		}

		val := make([]byte, valSize)
		if _, err := io.ReadFull(r.file, val); err != nil {
			return nil, err
		}

		data[string(key)] = val
		currentPos += int64(8 + keySize + valSize)
	}
	return data, nil
}

func (r *Reader) findIndexEntry(searchKey []byte) int64 {
	if len(r.index) == 0 {
		return 0
	}

	// Binary search
	left, right := 0, len(r.index)-1
	result := int64(0)

	for left <= right {
		mid := (left + right) / 2
		cmp := bytes.Compare(r.index[mid].Key, searchKey)

		if cmp <= 0 {
			result = r.index[mid].Offset
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return result
}
