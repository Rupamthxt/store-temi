package index

import (
	"math/rand"
	"time"
)

const (
	maxLevel = 16
	p        = 0.5
)

// SkipListNode represents a node in the skip list
type SkipListNode struct {
	score float64 // The value we sort by (e.g., age: 30)
	key   string  // The primary key (e.g., "user_1")
	next  []*SkipListNode
	value string // The associated value (e.g., JSON string)
}

// SkipList is our ordered index
type SkipList struct {
	head  *SkipListNode
	level int
	rand  *rand.Rand
}

func NewSkipList() *SkipList {
	return &SkipList{
		head:  &SkipListNode{next: make([]*SkipListNode, maxLevel)},
		level: 1,
		rand:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// randomLevel determines how tall the "tower" for a new node is
func (sl *SkipList) randomLevel() int {
	lvl := 1
	for sl.rand.Float64() < p && lvl < maxLevel {
		lvl++
	}
	return lvl
}

// Insert adds a new key to the list
func (sl *SkipList) Insert(key string, value string) {
	update := make([]*SkipListNode, maxLevel)
	current := sl.head

	// 1. Find the spot where we need to insert
	for i := sl.level - 1; i >= 0; i-- {
		// "current.next[i].key < key"
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
		update[i] = current
	}

	// 2. Create the new node
	lvl := sl.randomLevel()
	if lvl > sl.level {
		for i := sl.level; i < lvl; i++ {
			update[i] = sl.head
		}
		sl.level = lvl
	}

	newNode := &SkipListNode{
		key:   key,
		value: value, // Store the value!
		next:  make([]*SkipListNode, lvl),
	}

	// 3. Link it in
	for i := 0; i < lvl; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}
}

// RangeSearch returns all values with score >= min and score <= max
func (sl *SkipList) RangeSearch(min, max float64) []string {
	current := sl.head

	// 1. Zoom down to the start of the range (>= min)
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].score < min {
			current = current.next[i]
		}
	}

	// Move to the first actual node
	current = current.next[0]

	var results []string

	// 2. Walk forward until we hit the end of the range (> max)
	for current != nil && current.score <= max {
		results = append(results, current.key)
		current = current.next[0]
	}

	return results
}

// Iterator returns all keys and values in sorted order.
// In a real DB, you'd return a proper Iterator object, but a slice is fine for now.
type NodeData struct {
	Key   string
	Value string // In Phase 5 this handles the JSON string
}

func (sl *SkipList) All() []NodeData {
	var nodes []NodeData
	current := sl.head.next[0]
	for current != nil {
		nodes = append(nodes, NodeData{
			Key:   current.key,
			Value: current.value,
		})
		current = current.next[0]
	}
	return nodes
}

// Find searches for a key in the SkipList.
// Returns the value and true if found, or empty string and false if not.
func (sl *SkipList) Find(key string) (string, bool) {
	current := sl.head

	// 1. Traverse down from the top level
	for i := sl.level - 1; i >= 0; i-- {
		// Move forward as long as the next node's key is LESS than our target
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
	}

	// 2. We are now at level 0, sitting exactly *before* where the key should be.
	// Move one step forward to check the actual node.
	current = current.next[0]

	// 3. Check for exact match
	if current != nil && current.key == key {
		return current.value, true
	}

	return "", false
}
