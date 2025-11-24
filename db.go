package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
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

// Query defines a simple filter condition.
type Query struct {
	Field    string      // e.g., "age"
	Operator string      // e.g., "eq" (==), "gt" (>), "lt" (<)
	Value    interface{} // e.g., 25
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

// Compact cleans up the log file by removing stale data.
func (db *Database) Compact() error {
	// 1. Lock the database entirely.
	// This prevents any reads or writes while we swap files.
	db.mu.Lock()
	defer db.mu.Unlock()

	// 2. Open a temporary new file for writing
	tempPath := db.file.Name() + ".tmp"
	newFile, err := os.OpenFile(tempPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	// 3. We will iterate through the EXISTING file to find valid data.
	// We rewind the current file to the start.
	if _, err := db.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var currentOffset int64 = 0
	headerBuf := make([]byte, headerSize)

	// We need a new map to track offsets in the NEW file
	newKeyDir := make(map[string]LogEntryPointer)
	var newOffset int64 = 0

	for {
		// --- READ STEP (Same as loadIndex) ---

		// Read Header
		if _, err := io.ReadFull(db.file, headerBuf); err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		timestamp := binary.BigEndian.Uint64(headerBuf[0:8])
		keySize := binary.BigEndian.Uint32(headerBuf[8:12])
		valueSize := binary.BigEndian.Uint32(headerBuf[12:16])

		// Read Key
		keyBytes := make([]byte, keySize)
		if _, err := io.ReadFull(db.file, keyBytes); err != nil {
			return err
		}
		key := string(keyBytes)

		// Read Value
		valueBytes := make([]byte, valueSize)
		if _, err := io.ReadFull(db.file, valueBytes); err != nil {
			return err
		}

		// --- DECISION STEP ---

		// Calculate where the value lived in the OLD file
		oldValueOffset := currentOffset + headerSize + int64(keySize)

		// Check our current index. Does it point to THIS specific record?
		// If db.keyDir[key].offset matches, this is the LATEST data. Keep it.
		// If it doesn't match, it means a newer write happened later. Skip it.
		if db.keyDir[key].offset == oldValueOffset {

			// --- WRITE STEP ---

			// Write to the NEW file
			// 1. Header
			if _, err := newFile.Write(headerBuf); err != nil {
				return err
			}
			// 2. Key
			if _, err := newFile.Write(keyBytes); err != nil {
				return err
			}
			// 3. Value
			if _, err := newFile.Write(valueBytes); err != nil {
				return err
			}

			// Update our temporary index
			newValueOffset := newOffset + headerSize + int64(keySize)
			newKeyDir[key] = LogEntryPointer{
				offset:    newValueOffset,
				size:      valueSize,
				timestamp: timestamp,
			}

			// Advance new file offset
			newOffset += int64(headerSize + keySize + valueSize)
		}

		// Advance old file offset (we read header + key + value)
		currentOffset += int64(headerSize + keySize + valueSize)
	}

	// 4. SWAP STEP

	// We have written the new file. Now we need to switch over.

	// Close both files
	oldPath := db.file.Name()
	db.file.Close()
	newFile.Close()

	// Replace the old file with the new one
	if err := os.Rename(tempPath, oldPath); err != nil {
		return fmt.Errorf("failed to replace old file: %w", err)
	}

	// Re-open the file (now pointing to the compacted data)
	file, err := os.OpenFile(oldPath, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	db.file = file

	// Replace the in-memory index
	db.keyDir = newKeyDir

	fmt.Println("Compaction complete.")
	return nil
}

// Select performs a full table scan to find documents matching the query.
// It returns a list of matching JSON strings.
func (db *Database) Select(q Query) ([]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var results []string

	// Iterate over every key in our in-memory index
	for key, _ := range db.keyDir {
		// 1. Get the raw value (this uses our existing Get logic)
		jsonStr, err := db.Get(key)
		if err != nil {
			continue // Skip corrupted/missing records
		}

		// 2. Parse the JSON
		// We use a generic map because we don't know the schema ahead of time.
		var doc map[string]interface{}
		if err := json.Unmarshal([]byte(jsonStr), &doc); err != nil {
			continue // Skip non-JSON values
		}

		// 3. Extract the field we are looking for
		fieldVal, exists := doc[q.Field]
		if !exists {
			continue
		}

		// 4. Compare (The Logic)
		match := false

		switch q.Operator {
		case "eq":
			// Equality check
			if fieldVal == q.Value {
				match = true
			}
		case "gt":
			// Greater Than (numbers only)
			// JSON numbers are float64 in Go generic maps
			if v, ok := fieldVal.(float64); ok {
				if target, ok := q.Value.(float64); ok {
					if v > target {
						match = true
					}
				}
			}
		case "contains":
			// String contains
			if v, ok := fieldVal.(string); ok {
				if target, ok := q.Value.(string); ok {
					if strings.Contains(v, target) {
						match = true
					}
				}
			}
		}

		if match {
			results = append(results, jsonStr)
		}
	}

	return results, nil
}

func main() {
	dbPath := "users.db"
	os.Remove(dbPath)
	db, _ := NewDatabase(dbPath)
	defer db.Close()

	fmt.Println("Seeding data...")

	// Insert JSON documents
	db.Set("user_1", `{"name": "Alice", "age": 30, "role": "admin"}`)
	db.Set("user_2", `{"name": "Bob",   "age": 42, "role": "user"}`)
	db.Set("user_3", `{"name": "Carol", "age": 25, "role": "user"}`)
	db.Set("user_4", `{"name": "Dave",  "age": 30, "role": "moderator"}`)

	// Query 1: Find all users with age == 30
	fmt.Println("\n--- Query: Age == 30 ---")
	results, _ := db.Select(Query{
		Field:    "age",
		Operator: "eq",
		Value:    30.0, // JSON numbers are floats!
	})
	for _, r := range results {
		fmt.Println(r)
	}

	// Query 2: Find all users older than 40
	fmt.Println("\n--- Query: Age > 40 ---")
	results, _ = db.Select(Query{
		Field:    "age",
		Operator: "gt",
		Value:    40.0,
	})
	for _, r := range results {
		fmt.Println(r)
	}

	// Query 3: Find users with role "admin"
	fmt.Println("\n--- Query: Role == admin ---")
	results, _ = db.Select(Query{
		Field:    "role",
		Operator: "eq",
		Value:    "admin",
	})
	for _, r := range results {
		fmt.Println(r)
	}
}
