package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// Global Database Instance
var db *Database

func main() {
	fmt.Println("🚀 Starting Vector Database Server...")

	// 1. Initialize the Database
	var err error
	db, err = NewDatabase("./data_prod") // Use a production folder
	if err != nil {
		panic(fmt.Sprintf("Failed to init DB: %v", err))
	}

	// 2. Define API Routes
	http.HandleFunc("/get", handleGet)         // GET /get?key=user_1
	http.HandleFunc("/set", handleSet)         // POST /set
	http.HandleFunc("/delete", handleDelete)   // DELETE /delete?key=user_1
	http.HandleFunc("/search", handleSearch)   // POST /search
	http.HandleFunc("/compact", handleCompact) // POST /compact

	// 3. Start Server
	fmt.Println("🌐 Server listening on http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}

// --- HANDLERS ---

// GET /get?key=abc
func handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing 'key' parameter", http.StatusBadRequest)
		return
	}

	val, err := db.Get(key)
	if err != nil {
		http.Error(w, fmt.Sprintf("Key not found: %v", err), http.StatusNotFound)
		return
	}

	// Return raw string (it might be JSON, but we return it as text/json)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(val))
}

// POST /set
// Body: {"key": "abc", "value": {...}}
func handleSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// We decode 'value' as an interface{} so we can accept ANY JSON object
	var req struct {
		Key   string      `json:"key"`
		Value interface{} `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	// Convert the 'value' object back to a string for storage
	valBytes, _ := json.Marshal(req.Value)
	valStr := string(valBytes)

	if err := db.Set(req.Key, valStr); err != nil {
		http.Error(w, "Failed to write", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status": "ok"}`))
}

// DELETE /delete?key=abc
func handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing 'key'", http.StatusBadRequest)
		return
	}

	if err := db.Delete(key); err != nil {
		http.Error(w, "Failed to delete", http.StatusInternalServerError)
		return
	}

	w.Write([]byte(`{"status": "deleted"}`))
}

// POST /search
// Body: {"vector": [0.1, 0.9, ...], "k": 5}
func handleSearch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Vector []float64 `json:"vector"`
		K      int       `json:"k"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.K <= 0 {
		req.K = 1
	}

	results, err := db.SearchVector(req.Vector, req.K)
	if err != nil {
		http.Error(w, "Search failed", http.StatusInternalServerError)
		return
	}

	// Encode results back to JSON
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// POST /compact
func handleCompact(w http.ResponseWriter, r *http.Request) {
	if err := db.Compact(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(`{"status": "compaction complete"}`))
}
