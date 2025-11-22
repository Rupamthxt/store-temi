package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// LogEntryPointer holds the metadata for a value's location in the log file.
// We'll store this in the in-memory keyDir map.
type LogEntryPointer struct {
	// offset is the byte position in the file where the *value* starts.
	offset int64
	// size is the length of the *value* in bytes.
	size uint32
	// timestamp of the entry, useful for compaction later
	timestamp uint64
}

// Database is the main struct holding the DB state.
type Database struct {
	file   *os.File                   // The log file on disk
	keyDir map[string]LogEntryPointer // The in-memory index

	// A RWMutex allows many concurrent readers OR one exclusive writer.
	// This is critical for performance and safety.
	mu sync.RWMutex
}

// headerSize is the fixed size of our entry header in bytes.
// 8 bytes (timestamp) + 4 bytes (key_size) + 4 bytes (value_size) = 16 bytes
const headerSize = 16

// NewDatabase creates a new Database instance, opens the log file,
// and loads the index from disk.
func NewDatabase(filePath string) (*Database, error) {
	// Open the file for reading and writing. Create it if it doesn't exist.
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	db := &Database{
		file:   file,
		keyDir: make(map[string]LogEntryPointer),
	}

	// Now, load the index from the file
	if err := db.loadIndex(); err != nil {
		return nil, fmt.Errorf("failed to load index: %w", err)
	}

	fmt.Printf("Database loaded with %d keys.\n", len(db.keyDir))
	return db, nil
}

// loadIndex reads the entire log file from the beginning and builds
// the in-memory keyDir.
func (db *Database) loadIndex() error {
	// Acquire a full lock while we are building the index
	db.mu.Lock()
	defer db.mu.Unlock()

	// Go to the very beginning of the file
	_, err := db.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	var currentOffset int64 = 0
	headerBuf := make([]byte, headerSize)

	for {
		// 1. Read the 16-byte header
		n, err := io.ReadFull(db.file, headerBuf)
		if err == io.EOF {
			// We've reached the end of the file. Stop.
			break
		}
		if err != nil {
			return fmt.Errorf("error reading header at offset %d: %w", currentOffset, err)
		}
		if n < headerSize {
			return fmt.Errorf("read partial header at offset %d, file may be corrupt", currentOffset)
		}

		// 2. Parse the header
		// We use BigEndian - this is an arbitrary but consistent choice.
		timestamp := binary.BigEndian.Uint64(headerBuf[0:8])
		keySize := binary.BigEndian.Uint32(headerBuf[8:12])
		valueSize := binary.BigEndian.Uint32(headerBuf[12:16])

		// 3. Read the key
		keyBytes := make([]byte, keySize)
		_, err = io.ReadFull(db.file, keyBytes)
		if err != nil {
			return fmt.Errorf("error reading key at offset %d: %w", currentOffset, err)
		}
		key := string(keyBytes)

		// 4. Create the index entry
		valueOffset := currentOffset + headerSize + int64(keySize)
		db.keyDir[key] = LogEntryPointer{
			offset:    valueOffset,
			size:      valueSize,
			timestamp: timestamp,
		}

		// 5. Seek past the value to the next header
		// We use SeekCurrent (1) to jump forward from our current position.
		nextHeaderOffset := valueOffset + int64(valueSize)
		_, err = db.file.Seek(nextHeaderOffset, io.SeekStart) // Seek from start is safer
		if err != nil {
			return fmt.Errorf("error seeking past value at offset %d: %w", currentOffset, err)
		}

		currentOffset = nextHeaderOffset
	}

	// IMPORTANT: After reading, make sure the file pointer is at the END
	// so that our next 'Set' operation appends correctly.
	_, err = db.file.Seek(0, io.SeekEnd)
	return err
}

// Close gracefully closes the database file.
func (db *Database) Close() error {
	return db.file.Close()
}

// Set appends a key-value pair to the log file and updates the in-memory index.
func (db *Database) Set(key string, value string) error {
	// 1. Acquire the EXCLUSIVE lock.
	// We are modifying the file and the map, so no one else can read or write.
	db.mu.Lock()
	defer db.mu.Unlock()

	// 2. Prepare the data
	timestamp := uint64(time.Now().Unix())
	keyBytes := []byte(key)
	valueBytes := []byte(value)

	keySize := uint32(len(keyBytes))
	valueSize := uint32(len(valueBytes))

	// 3. Prepare the header
	header := make([]byte, headerSize)
	binary.BigEndian.PutUint64(header[0:8], timestamp)
	binary.BigEndian.PutUint32(header[8:12], keySize)
	binary.BigEndian.PutUint32(header[12:16], valueSize)

	// 4. Seek to the end of the file to ensure we append
	// This returns the offset where our new record starts.
	currentOffset, err := db.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek to end: %w", err)
	}

	// 5. Write the data (Header + Key + Value)
	// Note: In a production system, you might buffer these writes to a bufio.Writer
	// and Flush() them to minimize syscalls, but direct Write is safer for now.
	if _, err := db.file.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	if _, err := db.file.Write(keyBytes); err != nil {
		return fmt.Errorf("failed to write key: %w", err)
	}
	if _, err := db.file.Write(valueBytes); err != nil {
		return fmt.Errorf("failed to write value: %w", err)
	}

	// 6. Update the In-Memory Index
	// We point to where the VALUE starts, which is:
	// start_of_record + header_size + key_size
	valueOffset := currentOffset + int64(headerSize) + int64(keySize)

	db.keyDir[key] = LogEntryPointer{
		offset:    valueOffset,
		size:      valueSize,
		timestamp: timestamp,
	}

	return nil
}

// Get retrieves the value for a key from the log file.
func (db *Database) Get(key string) (string, error) {
	// 1. Acquire the READ lock.
	// Multiple threads can read at the same time, but no one can write.
	db.mu.RLock()
	defer db.mu.RUnlock()

	// 2. Look up the key in the in-memory map
	entry, ok := db.keyDir[key]
	if !ok {
		return "", fmt.Errorf("key not found")
	}

	// 3. Prepare a buffer to hold the value
	valueBytes := make([]byte, entry.size)

	// 4. Read ONLY the value from the disk
	// ReadAt is thread-safe and doesn't move the file cursor.
	_, err := db.file.ReadAt(valueBytes, entry.offset)
	if err != nil {
		return "", fmt.Errorf("failed to read value at offset %d: %w", entry.offset, err)
	}

	return string(valueBytes), nil
}

func main() {
	// 1. Create/Open the database
	dbPath := "my_test.db"
	db, err := NewDatabase(dbPath)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 2. Write some data
	fmt.Println("Writing data...")
	if err := db.Set("user_1", "Alice"); err != nil {
		panic(err)
	}
	if err := db.Set("user_2", "Bob"); err != nil {
		panic(err)
	}
	// Overwrite user_1 to prove the log works (append-only update)
	if err := db.Set("user_1", "Alice_Updated"); err != nil {
		panic(err)
	}

	// 3. Read the data back
	fmt.Println("Reading data...")

	val1, err := db.Get("user_1")
	if err != nil {
		panic(err)
	}
	fmt.Printf("user_1: %s (Expected: Alice_Updated)\n", val1)

	val2, err := db.Get("user_2")
	if err != nil {
		panic(err)
	}
	fmt.Printf("user_2: %s (Expected: Bob)\n", val2)

	// 4. Persistence Test
	// If you run this program twice, the second time it should load the
	// old data from the file during NewDatabase().
}
