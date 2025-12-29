package index

import (
	"math"
	"sort"
	"sync"
)

type VectorNode struct {
	ID        string
	Vector    []float64
	Neighbors []string // List of Neighbor IDs
}

type NSWIndex struct {
	Nodes map[string]*VectorNode
	mu    sync.RWMutex
}

func NewNSWIndex() *NSWIndex {
	return &NSWIndex{
		Nodes: make(map[string]*VectorNode),
	}
}

// Cosine Similarity (Helper)
func CosineSimilarity(a, b []float64) float64 {
	// ... (Copy your existing implementation here) ...
	// For brevity, assuming you copy-paste the math from vector.go
	var dot, nA, nB float64
	for i := 0; i < len(a); i++ {
		dot += a[i] * b[i]
		nA += a[i] * a[i]
		nB += b[i] * b[i]
	}
	if nA == 0 || nB == 0 {
		return 0
	}
	return dot / (math.Sqrt(nA) * math.Sqrt(nB))
}

// Insert adds a node and connects it to the graph
func (idx *NSWIndex) Insert(id string, vec []float64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	node := &VectorNode{ID: id, Vector: vec}
	idx.Nodes[id] = node

	// If this is the first node, we are done
	if len(idx.Nodes) == 1 {
		return
	}

	// CONNECT TO NEIGHBORS (The "Small World" Magic)
	// In a real HNSW, we traverse layers. In simple NSW, we can just
	// find the Top-M closest existing nodes and connect to them.
	// For a "Production" feel without extreme complexity, we will scan to find neighbors.
	// (Note: This insert is slow O(N), but search is fast.
	// Real HNSW optimizes insert to O(log N) too).

	candidates := idx.findNearest(vec, 5) // Connect to 5 closest friends

	for _, neighborID := range candidates {
		neighbor := idx.Nodes[neighborID]

		// Add edge (Bi-directional)
		node.Neighbors = append(node.Neighbors, neighborID)
		neighbor.Neighbors = append(neighbor.Neighbors, id)
	}
}

// Search finds the nearest neighbor using the Graph Traversal
func (idx *NSWIndex) Search(query []float64, k int) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.Nodes) == 0 {
		return nil
	}

	// 1. Start at a random entry point
	var entryPoint *VectorNode
	for _, n := range idx.Nodes {
		entryPoint = n
		break
	}

	current := entryPoint
	bestDist := CosineSimilarity(query, current.Vector)

	// Greedy Walk: Keep moving to a neighbor if it's closer
	for {
		madeProgress := false
		for _, neighborID := range current.Neighbors {
			neighbor := idx.Nodes[neighborID]
			dist := CosineSimilarity(query, neighbor.Vector)

			if dist > bestDist {
				bestDist = dist
				current = neighbor
				madeProgress = true
			}
		}

		if !madeProgress {
			break // We are at a local maximum (peak)
		}
	}

	// "Current" is now the closest node we found.
	// In a real implementation, we'd use a priority queue to collect top-K.
	// For this simple version, we return the peak.
	return []string{current.ID}
}

// Helper for Insert: Finds nearest nodes to connect to (Brute force for insert is acceptable for small DBs)
func (idx *NSWIndex) findNearest(query []float64, k int) []string {
	type res struct {
		id    string
		score float64
	}
	var candidates []res

	for id, node := range idx.Nodes {
		// Don't link to self
		if node.Vector == nil {
			continue
		}
		score := CosineSimilarity(query, node.Vector)
		candidates = append(candidates, res{id, score})
	}

	sort.Slice(candidates, func(i, j int) bool { return candidates[i].score > candidates[j].score })

	if len(candidates) > k {
		candidates = candidates[:k]
	}

	var ids []string
	for _, c := range candidates {
		ids = append(ids, c.id)
	}
	return ids
}
