package main

import (
	"custom-db/internal/index"
	"custom-db/internal/storage"
	"encoding/json"
	"fmt"
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
	// The Active MemTable (In-Memory Buffer)
	// New writes go here. When it fills up, we flush it to disk.
	memTable *index.SkipList
	sstables []*storage.SSTableReader
	// We still need a lock for concurrency
	mu sync.RWMutex

	// Directory where we store SSTables (e.g., "./data/")
	dataDir string

	vectorIndex *index.NSWIndex
}

// Query defines a simple filter condition.
type Query struct {
	Field    string // e.g., "age"
	Operator string // e.g., "eq" (==), "gt" (>), "lt" (<)
	Value    any    // e.g., 25
}

type SearchResult struct {
	Key   string
	Score float64
	Value string // The full JSON document
}

// NewDatabase creates a new Database instance, opens the log file,
// and loads the index from disk.
func NewDatabase(dir string) (*Database, error) {
	// Ensure the directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	db := &Database{
		dataDir:     dir,
		memTable:    index.NewSkipList(), // Initialize the empty MemTable
		vectorIndex: index.NewNSWIndex(),
	}

	return db, nil
}

// FlushMemTable forces the current MemTable to disk as an SSTable.
func (db *Database) FlushMemTable() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// OPTIMIZATION: Check if it's already empty
	data := db.memTable.All()
	if len(data) == 0 {
		return nil // Nothing to flush!
	}

	fmt.Println("Flushing MemTable to disk...")

	// 1. Generate a unique filename
	filename := fmt.Sprintf("data_%d.sst", time.Now().UnixNano())

	// 2. Initialize the Builder
	builder, err := storage.NewSSTableBuilder(filename)
	if err != nil {
		return err
	}

	// 3. Iterate over the SkipList (MemTable)
	// Note: You need to implement sl.All() to return key/value pairs

	for _, node := range data {
		err := builder.Add([]byte(node.Key), []byte(node.Value))
		if err != nil {
			return err
		}
	}

	// 4. Finish the file
	if err := builder.Close(); err != nil {
		return err
	}

	// 5. Open it for reading
	reader, err := storage.NewSSTableReader(filename)
	if err == nil {
		// Prepend to the list (Newest files should be checked first!)
		db.sstables = append([]*storage.SSTableReader{reader}, db.sstables...)
	}

	// 5. Reset MemTable
	// We create a fresh SkipList for new writes.
	// The old one is now safely on disk as an SSTable.
	db.memTable = index.NewSkipList()

	// 6. Register the new SSTable (Phase 6: The SSTable Reader)
	// db.sstables = append(db.sstables, filename)

	fmt.Printf("Flush complete: %s created.\n", filename)
	return nil
}

// // loadIndex reads the entire log file from the beginning and builds
// // the in-memory keyDir.
// func (db *Database) loadIndex() error {
// 	// Acquire a full lock while we are building the index
// 	db.mu.Lock()
// 	defer db.mu.Unlock()

// 	// Go to the very beginning of the file
// 	_, err := db.file.Seek(0, io.SeekStart)
// 	if err != nil {
// 		return err
// 	}

// 	var currentOffset int64 = 0
// 	headerBuf := make([]byte, headerSize)

// 	for {
// 		// 1. Read the 16-byte header
// 		n, err := io.ReadFull(db.file, headerBuf)
// 		if err == io.EOF {
// 			// We've reached the end of the file. Stop.
// 			break
// 		}
// 		if err != nil {
// 			return fmt.Errorf("error reading header at offset %d: %w", currentOffset, err)
// 		}
// 		if n < headerSize {
// 			return fmt.Errorf("read partial header at offset %d, file may be corrupt", currentOffset)
// 		}

// 		// 2. Parse the header
// 		// We use BigEndian - this is an arbitrary but consistent choice.
// 		timestamp := binary.BigEndian.Uint64(headerBuf[0:8])
// 		keySize := binary.BigEndian.Uint32(headerBuf[8:12])
// 		valueSize := binary.BigEndian.Uint32(headerBuf[12:16])

// 		// 3. Read the key
// 		keyBytes := make([]byte, keySize)
// 		_, err = io.ReadFull(db.file, keyBytes)
// 		if err != nil {
// 			return fmt.Errorf("error reading key at offset %d: %w", currentOffset, err)
// 		}
// 		key := string(keyBytes)

// 		// 4. Create the index entry
// 		valueOffset := currentOffset + headerSize + int64(keySize)
// 		db.keyDir[key] = LogEntryPointer{
// 			offset:    valueOffset,
// 			size:      valueSize,
// 			timestamp: timestamp,
// 		}

// 		// 5. Seek past the value to the next header
// 		// We use SeekCurrent (1) to jump forward from our current position.
// 		nextHeaderOffset := valueOffset + int64(valueSize)
// 		_, err = db.file.Seek(nextHeaderOffset, io.SeekStart) // Seek from start is safer
// 		if err != nil {
// 			return fmt.Errorf("error seeking past value at offset %d: %w", currentOffset, err)
// 		}

// 		currentOffset = nextHeaderOffset
// 	}

// 	// IMPORTANT: After reading, make sure the file pointer is at the END
// 	// so that our next 'Set' operation appends correctly.
// 	_, err = db.file.Seek(0, io.SeekEnd)
// 	return err
// }

// Close gracefully closes the database file.
// func (db *Database) Close() error {
// 	return db.file.Close()
// }

func (db *Database) Set(key, value string) error {
	db.mu.Lock()

	// 1. Write to MemTable
	db.memTable.Insert(key, value)

	// Check size WHILE locked to ensure thread safety
	currentSize := len(db.memTable.All())

	// 2. RELEASE Lock immediately
	db.mu.Unlock()

	// 3. Flush if needed (FlushMemTable will acquire its own lock)
	if currentSize >= 10 {
		return db.FlushMemTable()
	}

	return nil
}

// Get retrieves the value for a key from the log file.
func (db *Database) Get(key string) (string, error) {
	// 1. Acquire the READ lock.
	// Multiple threads can read at the same time, but no one can write.
	db.mu.RLock()
	defer db.mu.RUnlock()

	// 1. Check MemTable (Fastest)
	// (You need to implement Find on SkipList, returning value + bool)
	if val, found := db.memTable.Find(key); found {
		return val, nil
	}

	// 2. Check SSTables (Newest to Oldest)
	for _, reader := range db.sstables {
		val, err := reader.Get(key)
		if err == nil {
			return val, nil // Found it!
		}
	}

	return "", fmt.Errorf("key not found")
}

func (db *Database) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(db.sstables) == 0 {
		return nil // Nothing to compact
	}

	fmt.Println("Starting Compaction...")

	// 1. Create a new "Merged" SSTable
	outFilename := fmt.Sprintf("%s/compacted_%d.sst", db.dataDir, time.Now().UnixNano())
	builder, err := storage.NewSSTableBuilder(outFilename)
	if err != nil {
		return err
	}

	// 2. Create Iterators for all existing SSTables
	// Note: db.sstables is ordered [Newest, ..., Oldest]
	var iterators []*storage.Iterator
	for _, reader := range db.sstables {
		iterators = append(iterators, reader.NewIterator())
	}

	// 3. The Merge Loop
	for {
		// Find the smallest key among all active iterators
		smallestKey := ""
		firstValid := true

		// We also need to know which iterators have this key so we can advance them
		var iteratorsWithSmallest []int

		for i, it := range iterators {
			if !it.Valid {
				continue
			}

			if firstValid {
				smallestKey = it.Key
				iteratorsWithSmallest = []int{i}
				firstValid = false
			} else {
				if it.Key < smallestKey {
					smallestKey = it.Key
					iteratorsWithSmallest = []int{i}
				} else if it.Key == smallestKey {
					iteratorsWithSmallest = append(iteratorsWithSmallest, i)
				}
			}
		}

		// If no iterators are valid, we are done!
		if firstValid {
			break
		}

		// 4. Resolve Conflict: Who wins?
		// Our iterators slice is [Newest ... Oldest].
		// So the first index in `iteratorsWithSmallest` is the newest version.
		winnerIndex := iteratorsWithSmallest[0]
		winnerVal := iterators[winnerIndex].Value

		// 5. Write to the new file
		if err := builder.Add([]byte(smallestKey), []byte(winnerVal)); err != nil {
			return err
		}

		// 6. Advance ALL iterators that had this key
		// This effectively discards the older versions (shadowing)
		for _, idx := range iteratorsWithSmallest {
			iterators[idx].Next()
		}
	}

	// 7. Close and Finish the new file
	if err := builder.Close(); err != nil {
		return err
	}

	// 8. Atomic Switch
	// Close old readers
	for _, reader := range db.sstables {
		// A. Close the file handle so the OS releases the lock
		if err := reader.Close(); err != nil {
			fmt.Printf("Warning: failed to close %s: %v\n", reader.Filename, err)
		}

		// B. Delete the physical file
		if err := os.Remove(reader.Filename); err != nil {
			fmt.Printf("Warning: failed to delete %s: %v\n", reader.Filename, err)
		} else {
			fmt.Printf("Deleted old segment: %s\n", reader.Filename)
		}
	}

	// Open the new single SSTable
	newReader, err := storage.NewSSTableReader(outFilename)
	if err != nil {
		return err
	}

	// Replace the list
	db.sstables = []*storage.SSTableReader{newReader}

	fmt.Println("Compaction Complete. Merged into:", outFilename)
	return nil
}

func (db *Database) rebuildVectorIndex() error {
	fmt.Println("🔮 Rehydrating Vector Index from Disk...")

	// We iterate over ALL SSTables (Oldest to Newest usually preferred for updates,
	// but since our compaction handles uniqueness, order matters less here unless we have duplicates.
	// Let's go Newest -> Oldest (db.sstables order) and ignore keys we've already seen.

	seen := make(map[string]bool)

	for _, reader := range db.sstables {
		it := reader.NewIterator()
		for ; it.Valid; it.Next() {
			if seen[it.Key] {
				continue // We already have the newer version of this key
			}

			// Attempt to parse JSON and extract vector
			// (We reuse the logic from Set())
			vec, err := extractVector(it.Value)
			if err == nil {
				// ADD TO INDEX
				// For now, we just add to our map. In Part 2, we will add to the Graph.
				db.vectorIndex.Insert(it.Key, vec)
			}
			seen[it.Key] = true
		}
	}
	fmt.Printf("✅ Index Rebuilt. %d vectors loaded.\n", len(db.vectorIndex.Nodes))
	return nil
}

// Helper to parse JSON (Add this to db.go)
func extractVector(jsonStr string) ([]float64, error) {
	var doc map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &doc); err != nil {
		return nil, err
	}

	if vecInterface, ok := doc["vector"].([]interface{}); ok {
		vec := make([]float64, len(vecInterface))
		for i, v := range vecInterface {
			if f, ok := v.(float64); ok {
				vec[i] = f
			} else {
				return nil, fmt.Errorf("invalid vector data")
			}
		}
		return vec, nil
	}
	return nil, fmt.Errorf("no vector field")
}

// Select performs a full table scan to find documents matching the query.
// It returns a list of matching JSON strings.
// func (db *Database) Select(q Query) ([]string, error) {
// 	db.mu.RLock()
// 	defer db.mu.RUnlock()

// 	var results []string

// 	// 1. Check Ordered Index (Best for GT/LT)
// 	if sl, ok := db.orderedIndexes[q.Field]; ok {
// 		var keys []string

// 		// Define "Infinity" for open-ended ranges
// 		maxFloat := 1.79e+308
// 		minFloat := -1.79e+308

// 		switch q.Operator {
// 		case "gt":
// 			if v, ok := q.Value.(float64); ok {
// 				// From (Value + epsilon) to Infinity
// 				keys = sl.RangeSearch(v+0.000001, maxFloat)
// 			}
// 		case "lt":
// 			if v, ok := q.Value.(float64); ok {
// 				// From -Infinity to (Value - epsilon)
// 				keys = sl.RangeSearch(minFloat, v-0.000001)
// 			}
// 		case "range": // Special case: Value is []float64{min, max}
// 			if rangeVals, ok := q.Value.([]float64); ok && len(rangeVals) == 2 {
// 				keys = sl.RangeSearch(rangeVals[0], rangeVals[1])
// 			}
// 		}

// 		if len(keys) > 0 {
// 			// Fetch actual data
// 			for _, key := range keys {
// 				val, err := db.getInternal(key)
// 				if err == nil {
// 					results = append(results, val)
// 				}
// 			}
// 			return results, nil
// 		}
// 	}

// 	//2. Chceck hash Index (Best for EQ)
// 	// OPTIMIZATION: Use Index if available for Equality checks
// 	if q.Operator == "eq" {
// 		if indexMap, ok := db.indexes[q.Field]; ok {
// 			// We have an index!
// 			// Lookup the list of keys directly. O(1) lookup.
// 			if keys, found := indexMap[q.Value]; found {
// 				// We have the keys, now just fetch the data
// 				for _, key := range keys {
// 					val, err := db.getInternal(key) // Use the helper!
// 					if err == nil {
// 						results = append(results, val)
// 					}
// 				}
// 				// Return immediately! No full scan needed.
// 				return results, nil
// 			}
// 			return results, nil // Index exists but value not found
// 		}
// 	}

// 	// Iterate over every key in our in-memory index
// 	for key := range db.keyDir {
// 		// 1. Get the raw value (this uses our existing Get logic)
// 		jsonStr, err := db.getInternal(key)
// 		if err != nil {
// 			continue // Skip corrupted/missing records
// 		}

// 		// 2. Parse the JSON
// 		// We use a generic map because we don't know the schema ahead of time.
// 		var doc map[string]interface{}
// 		if err := json.Unmarshal([]byte(jsonStr), &doc); err != nil {
// 			continue // Skip non-JSON values
// 		}

// 		// 3. Extract the field we are looking for
// 		fieldVal, exists := doc[q.Field]
// 		if !exists {
// 			continue
// 		}

// 		// 4. Compare (The Logic)
// 		match := false

// 		switch q.Operator {
// 		case "eq":
// 			// Equality check
// 			if fieldVal == q.Value {
// 				match = true
// 			}
// 		case "gt":
// 			// Greater Than (numbers only)
// 			// JSON numbers are float64 in Go generic maps
// 			if v, ok := fieldVal.(float64); ok {
// 				if target, ok := q.Value.(float64); ok {
// 					if v > target {
// 						match = true
// 					}
// 				}
// 			}
// 		case "contains":
// 			// String contains
// 			if v, ok := fieldVal.(string); ok {
// 				if target, ok := q.Value.(string); ok {
// 					if strings.Contains(v, target) {
// 						match = true
// 					}
// 				}
// 			}
// 		}

// 		if match {
// 			results = append(results, jsonStr)
// 		}
// 	}

// 	return results, nil
// }

// // CreateIndex tells the DB to index a specific field (e.g., "age").
// func (db *Database) CreateIndex(field string) {
// 	db.mu.Lock()
// 	defer db.mu.Unlock()

// 	if _, exists := db.indexes[field]; exists {
// 		return // Already indexed
// 	}

// 	// Initialize the map for this field
// 	db.indexes[field] = make(map[interface{}][]string)

// 	// Re-scan existing data to populate the index
// 	// (In a real DB, you'd do this more efficiently, but for now we iterate keys)
// 	for key := range db.keyDir {
// 		// We have to reuse our internal Get logic here, but we already hold the lock!
// 		// WARNING: Calling db.Get() here would Deadlock because Get() tries to Lock() again.
// 		// We need a helper that assumes we already have the lock.
// 		val, err := db.getInternal(key)
// 		if err != nil {
// 			continue
// 		}

// 		var doc map[string]interface{}
// 		if err := json.Unmarshal([]byte(val), &doc); err != nil {
// 			continue
// 		}

// 		if val, ok := doc[field]; ok {
// 			db.indexes[field][val] = append(db.indexes[field][val], key)
// 		}
// 	}
// 	fmt.Printf("Index created on field: %s\n", field)
// }

// func (db *Database) CreateOrderedIndex(field string) {
// 	db.mu.Lock()
// 	defer db.mu.Unlock()

// 	if _, exists := db.orderedIndexes[field]; exists {
// 		return
// 	}

// 	sl := NewSkipList()

// 	// Populate it with existing data
// 	for key := range db.keyDir {
// 		val, err := db.getInternal(key)
// 		if err != nil {
// 			continue
// 		}

// 		var doc map[string]interface{}
// 		json.Unmarshal([]byte(val), &doc)

// 		// Only index numeric values for range queries
// 		if v, ok := doc[field].(float64); ok {
// 			sl.Insert(v, key)
// 		}
// 	}

// 	db.orderedIndexes[field] = sl
// 	fmt.Printf("Ordered Index created on field: %s\n", field)
// }

// Helper: read value without locking (caller must hold lock)
// func (db *Database) getInternal(key string) (string, error) {
// 	entry, ok := db.keyDir[key]
// 	if !ok {
// 		return "", fmt.Errorf("key not found")
// 	}
// 	valueBytes := make([]byte, entry.size)
// 	_, err := db.file.ReadAt(valueBytes, entry.offset)
// 	if err != nil {
// 		return "", fmt.Errorf("failed to read value at offset %d: %w", entry.offset, err)
// 	}
// 	return string(valueBytes), err
// }

// SearchVector finds the top K nearest neighbors to the query vector.
// func (db *Database) SearchVector(query []float64, topK int) ([]SearchResult, error) {
// 	db.mu.RLock()
// 	defer db.mu.RUnlock()

// 	var candidates []SearchResult

// 	// 1. Scan ALL vectors (Brute Force)
// 	for key, vec := range db.vectorIndex {
// 		score := CosineSimilarity(query, vec)

// 		// Optimization: Only keep if score is somewhat relevant (optional)
// 		if score > 0 {
// 			// We need to fetch the actual value string
// 			val, _ := db.getInternal(key)
// 			candidates = append(candidates, SearchResult{Key: key, Score: score, Value: val})
// 		}
// 	}

// 	// 2. Sort by Score (Descending: High similarity first)
// 	sort.Slice(candidates, func(i, j int) bool {
// 		return candidates[i].Score > candidates[j].Score
// 	})

// 	// 3. Take Top K
// 	if len(candidates) > topK {
// 		candidates = candidates[:topK]
// 	}

// 	return candidates, nil
// }

func main() {
	fmt.Println("🚀 Starting Database Engine...")

	// 1. Setup a clean test directory
	dbPath := "./data_test"
	os.RemoveAll(dbPath) // Start fresh every time
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		panic(err)
	}

	// 2. Initialize the Database
	db, err := NewDatabase(dbPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize DB: %v", err))
	}

	// 3. WRITE PHASE
	// We set the flush threshold to 10 in our Set() function.
	// We will write 15 items.
	// - Items 0-9 will trigger a flush and go to DISK (SSTable).
	// - Items 10-14 will stay in RAM (MemTable).
	fmt.Println("\n--- ✍️  Write Phase (15 records) ---")
	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("user_%02d", i)
		val := fmt.Sprintf(`{"name": "User %d", "active": true}`, i)

		err := db.Set(key, val)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Set: %s \tOK\n", key)
	}

	// 4. READ PHASE
	fmt.Println("\n--- 📖 Read Phase ---")

	// Test A: Read from MemTable (Recent data: user_12)
	fmt.Print("Test A (RAM Check - user_12): ")
	val, err := db.Get("user_12")
	if err == nil {
		fmt.Printf("✅ FOUND -> %s\n", val)
	} else {
		fmt.Printf("❌ ERROR -> %v\n", err)
	}

	// Test B: Read from SSTable (Flushed data: user_03)
	// If this works, your SSTableReader and sparse index are working!
	fmt.Print("Test B (DISK Check - user_03): ")
	val, err = db.Get("user_03")
	if err == nil {
		fmt.Printf("✅ FOUND -> %s\n", val)
	} else {
		fmt.Printf("❌ ERROR -> %v\n", err)
	}

	// Test C: Read Missing Key
	fmt.Print("Test C (Miss Check - user_99): ")
	val, err = db.Get("user_99")
	if err != nil {
		fmt.Printf("✅ CORRECTLY MISSED (Error: %v)\n", err)
	} else {
		fmt.Printf("❌ UNEXPECTED FOUND -> %s\n", val)
	}

	fmt.Println("\n--- 🧹 Compaction Phase ---")
	// Force a compaction
	if err := db.Compact(); err != nil {
		panic(err)
	}

	// Verify data is still there
	val, err = db.Get("user_03")
	if err == nil {
		fmt.Printf("Post-Compaction Read (user_03): ✅ FOUND -> %s\n", val)
	} else {
		fmt.Printf("Post-Compaction Read (user_03): ❌ ERROR -> %v\n", err)
	}

	fmt.Println("\n🎉 Test Complete.")
}
