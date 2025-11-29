package storage

import (
	"encoding/binary"
	"os"
)

// IndexEntry tracks the first key of a block and where that block starts.
type IndexEntry struct {
	Key    string
	Offset int64
}

type SSTableBuilder struct {
	file          *os.File
	currentBlock  []byte
	index         []IndexEntry
	currentOffset int64
}

func NewSSTableBuilder(path string) (*SSTableBuilder, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &SSTableBuilder{
		file:         f,
		currentBlock: make([]byte, 0, 4096), // 4KB buffer
	}, nil
}

// Add appends a key-value pair.
// IMPORTANT: Keys MUST be added in sorted order!
func (b *SSTableBuilder) Add(key, value []byte) error {
	// 1. Check if adding this record would overflow the 4KB block
	entrySize := 4 + 4 + len(key) + len(value) // size tracking (simplified)

	if len(b.currentBlock)+entrySize > 4096 {
		if err := b.flushBlock(); err != nil {
			return err
		}
	}

	// 2. If this is the first record of a new block, add to Index
	if len(b.currentBlock) == 0 {
		b.index = append(b.index, IndexEntry{
			Key:    string(key),
			Offset: b.currentOffset,
		})
	}

	// 3. Append to the in-memory block buffer
	// (Helper function to pack bytes - implementation needed)
	b.currentBlock = append(b.currentBlock, encodeRecord(key, value)...)

	return nil
}

// flushBlock writes the in-memory buffer to disk
func (b *SSTableBuilder) flushBlock() error {
	if len(b.currentBlock) == 0 {
		return nil
	}

	// Write the block
	n, err := b.file.Write(b.currentBlock)
	if err != nil {
		return err
	}

	// Update global offset
	b.currentOffset += int64(n)

	// Reset buffer
	b.currentBlock = b.currentBlock[:0]
	return nil
}

// Close finishes the SSTable by writing the Index and Footer
func (b *SSTableBuilder) Close() error {
	// 1. Flush any remaining data in the buffer to disk
	if err := b.flushBlock(); err != nil {
		return err
	}

	// 2. Mark the start of the Index Block
	indexStartOffset := b.currentOffset

	// 3. Write the Index Block
	// Format for each entry: [Key Size (4B)] [Key Bytes] [Offset (8B)]
	for _, entry := range b.index {
		kBytes := []byte(entry.Key)
		kLen := uint32(len(kBytes))

		// Prepare buffer: 4 bytes len + key bytes + 8 bytes offset
		buf := make([]byte, 4+len(kBytes)+8)

		binary.BigEndian.PutUint32(buf[0:4], kLen)
		copy(buf[4:4+len(kBytes)], kBytes)
		binary.BigEndian.PutUint64(buf[4+len(kBytes):], uint64(entry.Offset))

		if _, err := b.file.Write(buf); err != nil {
			return err
		}
	}

	// 4. Write the Footer
	// The footer is just a pointer to where the Index Block starts.
	// It must be a fixed size (8 bytes) so we can always find it by seeking to (FileSize - 8).
	footer := make([]byte, 8)
	binary.BigEndian.PutUint64(footer, uint64(indexStartOffset))

	if _, err := b.file.Write(footer); err != nil {
		return err
	}

	// 5. Sync and Close
	if err := b.file.Sync(); err != nil { // Ensure data hits the physical disk
		return err
	}
	return b.file.Close()
}

// encodeRecord packs len|key|len|val (Helper)
func encodeRecord(key, val []byte) []byte {
	kLen := uint32(len(key))
	vLen := uint32(len(val))

	// Total size = 4 + 4 + len(key) + len(val)
	totalLen := 4 + 4 + kLen + vLen
	buf := make([]byte, totalLen)

	// Pack integers
	binary.BigEndian.PutUint32(buf[0:4], kLen)
	binary.BigEndian.PutUint32(buf[4:8], vLen)

	// Pack data
	copy(buf[8:8+kLen], key)
	copy(buf[8+kLen:], val)

	return buf
}
