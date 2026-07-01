package cluster

import (
	"crypto/sha256"
	"encoding/binary"
    "fmt"
    "sort"
    "sync"
)

const defaultReplicas = 150 // virtual nodes per physical node

type Ring struct {
	mu sync.RWMutex
	nodes map[uint32]string // hash position -> node address
	sorted []uint32 // sorted positions for binary search
	replicas int
}

func NewRing(replicas int) *Ring {
    if replicas <= 0 {
        replicas = defaultReplicas
    }
    return &Ring{
        nodes:    make(map[uint32]string),
        replicas: replicas,
    }
}

// Adds a single physical node to the consistent hashing ring, placing it at r.replicas different positions (its virtual nodes)
func (r *Ring) AddNode(addr string) {
    r.mu.Lock()
    defer r.mu.Unlock()

    for i := 0; i < r.replicas; i++ {
        pos := r.hashPosition(fmt.Sprintf("%s#%d", addr, i)) // e.g: "10.0.0.5:6379#1" (a unique virtual-node identifier)
        r.nodes[pos] = addr
        r.sorted = append(r.sorted, pos)
    }
    sort.Slice(r.sorted, func(i, j int) bool { // re-sorting after adding a node (O(NlogN))
        return r.sorted[i] < r.sorted[j]
    })
}

// removes a physical node's r.replicas virtual positions from the ring
func (r *Ring) RemoveNode(addr string) {
    r.mu.Lock()
    defer r.mu.Unlock()

    for i := 0; i < r.replicas; i++ {
        pos := r.hashPosition(fmt.Sprintf("%s#%d", addr, i))
        delete(r.nodes, pos)
    }

    r.sorted = r.sorted[:0] // Rebuild sorted slice, resizing to 0 length yet keeping capacity
    for pos := range r.nodes {
        r.sorted = append(r.sorted, pos)
    }
    sort.Slice(r.sorted, func(i, j int) bool {
        return r.sorted[i] < r.sorted[j]
    })
}

// Returns the node address responsible for the given key.
func (r *Ring) GetNode(key string) (string, bool) {
    r.mu.RLock()
    defer r.mu.RUnlock()

    if len(r.sorted) == 0 {
        return "", false
    }

    pos := r.hashPosition(key)

    // Binary search: find first virtual node position >= pos
    idx := sort.Search(len(r.sorted), func(i int) bool {
        return r.sorted[i] >= pos
    })

    // Wrap around the ring
    if idx == len(r.sorted) {
        idx = 0
    }

    return r.nodes[r.sorted[idx]], true
}

// Returns the N nodes responsible for the key (for replication)
func (r *Ring) GetNodes(key string, n int) []string {
    r.mu.RLock()
    defer r.mu.RUnlock()

    if len(r.sorted) == 0 {
        return nil
    }

    pos := r.hashPosition(key)
    idx := sort.Search(len(r.sorted), func(i int) bool {
        return r.sorted[i] >= pos
    })
    if idx == len(r.sorted) {
        idx = 0
    }

    seen := make(map[string]bool)
    var result []string
    for i := 0; len(result) < n && i < len(r.sorted); i++ {
        addr := r.nodes[r.sorted[(idx+i)%len(r.sorted)]]
        if !seen[addr] { // Deduplication by physical node
            seen[addr] = true
            result = append(result, addr)
        }
    }
    return result
}

func (r *Ring) hashPosition(key string) uint32 {
    h := sha256.Sum256([]byte(key)) // generates a 32 byte SHA-256 hash
    return binary.BigEndian.Uint32(h[:4]) // takes the first 4 bytes to get a 32-bit to get uint32 [0 - 2^32)
}

// Returns all unique physical node addresses currently in the ring
func (r *Ring) Nodes() []string {
    r.mu.RLock()
    defer r.mu.RUnlock()
    seen := make(map[string]bool)
    var result []string
    for _, addr := range r.nodes {
        if !seen[addr] {
            seen[addr] = true
            result = append(result, addr)
        }
    }
    return result
}