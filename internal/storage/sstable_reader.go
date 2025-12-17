package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type SSTableReader struct {
	file     *os.File
	Filename string
	index    []IndexEntry // The sparse index (Key -> FileOffset)
	dataEnd  int64        // End of data section (start of index)
}

func NewSSTableReader(filePath string) (*SSTableReader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	// 1. Read the Footer (Last 8 bytes) to find the Index
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := stat.Size()

	if fileSize < 8 {
		return nil, fmt.Errorf("file too small")
	}

	// Seek to where the Footer starts
	_, err = file.Seek(fileSize-8, io.SeekStart)
	if err != nil {
		return nil, err
	}

	// Read the Footer
	footerBuf := make([]byte, 8)
	if _, err := file.Read(footerBuf); err != nil {
		return nil, err
	}
	indexOffset := int64(binary.BigEndian.Uint64(footerBuf))

	// 2. Read the Index Block
	// We assume the index goes from indexOffset to (fileSize - 8)
	// (In a real DB, you might store the index size in the footer too)
	if _, err := file.Seek(indexOffset, io.SeekStart); err != nil {
		return nil, err
	}

	// Read the entire index block
	indexSize := (fileSize - 8) - indexOffset
	indexBytes := make([]byte, indexSize)
	if _, err := file.Read(indexBytes); err != nil {
		return nil, err
	}

	// 3. Parse the Index
	reader := &SSTableReader{
		file:     file,
		Filename: filePath,
		index:    parseIndex(indexBytes),
		dataEnd:  indexOffset,
	}

	return reader, nil
}

// parseIndex turns raw bytes back into a slice of IndexEntry
func parseIndex(data []byte) []IndexEntry {
	var entries []IndexEntry
	buf := bytes.NewReader(data)

	for buf.Len() > 0 {
		// Read Key Length (4B)
		var kLen uint32
		binary.Read(buf, binary.BigEndian, &kLen)

		// Read Key
		kBytes := make([]byte, kLen)
		buf.Read(kBytes)

		// Read Offset (8B)
		var offset uint64
		binary.Read(buf, binary.BigEndian, &offset)

		entries = append(entries, IndexEntry{
			Key:    string(kBytes),
			Offset: int64(offset),
		})
	}
	return entries
}

func (r *SSTableReader) Get(key string) (string, error) {
	// 1. Find the Candidate Block using the Index
	// We want the last entry where entry.Key <= key
	// This is standard Binary Search logic.

	var targetOffset int64 = -1

	// Iterate to find the right block
	// (Optimization: Use sort.Search for O(log N) instead of this loop)
	for i := 0; i < len(r.index); i++ {
		if r.index[i].Key <= key {
			targetOffset = r.index[i].Offset
		} else {
			// The current index key is > our key.
			// So our key must be in the PREVIOUS block (if it exists).
			break
		}
	}

	if targetOffset == -1 {
		return "", fmt.Errorf("key not found (before first block)")
	}

	// 2. Seek to that Block
	if _, err := r.file.Seek(targetOffset, io.SeekStart); err != nil {
		return "", err
	}

	// 3. Scan the Block (Linear Scan)
	// We read record by record until we find the key or hit the next block/EOF.
	// For simplicity, let's assume we read 4KB or until we fail.

	// (A robust implementation would know the block size or have a block header)
	// Here, we just read stream of records.

	for {
		// SAFETY CHECK: Are we at the end of the data section?
		currentPos, _ := r.file.Seek(0, io.SeekCurrent)
		if currentPos >= r.dataEnd {
			break // Stop! We reached the Index Block.
		}
		// Read Key Length
		var kLen uint32
		err := binary.Read(r.file, binary.BigEndian, &kLen)
		if err != nil {
			break
		} // EOF

		// Read Value Length
		var vLen uint32
		binary.Read(r.file, binary.BigEndian, &vLen)

		if kLen > 1024*1024 || vLen > 1024*1024*10 { // e.g. 1MB key / 10MB value limit
			return "", fmt.Errorf("corrupt file: absurd record size")
		}

		// Read Key
		keyBytes := make([]byte, kLen)
		r.file.Read(keyBytes)
		currentKey := string(keyBytes)

		// Read Value
		valBytes := make([]byte, vLen)
		r.file.Read(valBytes)

		// Check Match
		if currentKey == key {
			return string(valBytes), nil
		}

		// Optimization: Since the block is sorted, if currentKey > key,
		// we know the key is NOT in this file. We can stop early.
		if currentKey > key {
			break
		}
	}

	return "", fmt.Errorf("key not found in SSTable")
}

func (r *SSTableReader) Close() error {
	return r.file.Close()
}
