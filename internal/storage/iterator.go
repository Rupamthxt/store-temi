package storage

import (
	"encoding/binary"
	"io"
)

// Iterator represents a stream of sorted records from an SSTable.
type Iterator struct {
	reader        *SSTableReader
	currentOffset int64

	// Current Data (The "Head" of the stream)
	Key   string
	Value string
	Valid bool // False if we reached the end
}

// NewIterator creates a standard iterator for an SSTable
func (r *SSTableReader) NewIterator() *Iterator {
	// Start at the beginning of the file (offset 0)
	r.file.Seek(0, io.SeekStart)

	it := &Iterator{
		reader:        r,
		currentOffset: 0,
		Valid:         true,
	}
	it.Next() // Load the first record
	return it
}

// Next advances to the next record
func (it *Iterator) Next() {
	// Check if we hit the end of the Data Section
	if it.currentOffset >= it.reader.dataEnd {
		it.Valid = false
		return
	}

	// 1. Read Key Length
	var kLen uint32
	if err := binary.Read(it.reader.file, binary.BigEndian, &kLen); err != nil {
		it.Valid = false
		return
	}

	// 2. Read Value Length
	var vLen uint32
	if err := binary.Read(it.reader.file, binary.BigEndian, &vLen); err != nil {
		it.Valid = false
		return
	}

	// 3. Read Key
	kBytes := make([]byte, kLen)
	if _, err := io.ReadFull(it.reader.file, kBytes); err != nil {
		it.Valid = false
		return
	}

	// 4. Read Value
	vBytes := make([]byte, vLen)
	if _, err := io.ReadFull(it.reader.file, vBytes); err != nil {
		it.Valid = false
		return
	}

	it.Key = string(kBytes)
	it.Value = string(vBytes)

	// Update offset (4 + 4 + kLen + vLen)
	it.currentOffset += int64(4 + 4 + kLen + vLen)
}
