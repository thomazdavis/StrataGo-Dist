package sstable

import (
	"encoding/binary"
	"io"
	"os"
)

type Iterator struct {
	file       *os.File
	limit      int64
	currentPos int64
	key        []byte
	val        []byte
	err        error
}

// Creates a sequential scanner for an SSTable
func (r *Reader) NewIterator() (*Iterator, error) {
	// Using a new file handle so that Iterator's position
	// doesn't interfere with concurrent Get() calls
	f, err := os.Open(r.file.Name())
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	fileSize := stat.Size()
	limit := fileSize

	if fileSize > 16 {
		footer := make([]byte, 16)
		f.ReadAt(footer, fileSize-16)
		limit = int64(binary.LittleEndian.Uint64(footer[8:16]))
	}

	return &Iterator{
		file:       f,
		limit:      limit,
		currentPos: 0,
	}, nil
}

func (it *Iterator) Next() bool {
	if it.currentPos >= it.limit {
		return false
	}

	var keySize, valSize uint32

	if err := binary.Read(it.file, binary.LittleEndian, &keySize); err != nil {
		it.err = err
		return false
	}
	if err := binary.Read(it.file, binary.LittleEndian, &valSize); err != nil {
		it.err = err
		return false
	}

	it.key = make([]byte, keySize)
	if _, err := io.ReadFull(it.file, it.key); err != nil {
		it.err = err
		return false
	}

	it.val = make([]byte, valSize)
	if _, err := io.ReadFull(it.file, it.val); err != nil {
		it.err = err
		return false
	}

	it.currentPos += int64(8 + keySize + valSize)
	return true
}

func (it *Iterator) Key() []byte {
	return it.key
}

func (it *Iterator) Value() []byte {
	return it.val
}

func (it *Iterator) Error() error {
	if it.err == io.EOF {
		return nil
	}
	return it.err
}

func (it *Iterator) Close() error {
	return it.file.Close()
}
