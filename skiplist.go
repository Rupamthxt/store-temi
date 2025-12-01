package main

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
	val   string  // The primary key (e.g., "user_1")
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
func (sl *SkipList) Insert(score float64, val string, value string) {
	update := make([]*SkipListNode, maxLevel)
	current := sl.head

	// 1. Find the spot where we need to insert
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].score < score {
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
		score: score,
		val:   val,
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
		results = append(results, current.val)
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
	current := sl.head.next[0] // Level 0 has all nodes
	for current != nil {
		nodes = append(nodes, NodeData{Key: current.val, Value: "..."})
		// WAIT: Our SkipList implementation in Phase 4.5 only stored Keys in `val`!
		// We need to fix the SkipList to store Values too.
		current = current.next[0]
	}
	return nodes
}
