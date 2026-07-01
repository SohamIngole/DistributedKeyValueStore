package cluster_test

import (
	"DistributedKeyValueStore/internal/cluster"
	"fmt"
	"testing"
)

// Test for even distribution of keys acorss nodes
func TestDistribution(t *testing.T) {
	r := cluster.NewRing(150)
	nodes := []string{"node1:6379", "node2:6380", "node3:6381"}
	for _, n := range nodes {
		r.AddNode(n)
	}

	// Distribute 10,000 keys and check for roughly even spread
	counts := make(map[string]int)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key:%d", i)
		node, _ := r.GetNode(key)
		counts[node]++
	}

	for node, count := range counts {
		pct := float64(count) / 100.0
		// Each node should own roughly 23% to 43% (a 10% margin of error)
		if pct < 23 || pct > 43 {
			t.Errorf("node %s owns %.1f%% of keys — distribution is too uneven", node, pct)
		}
	}
}

// Testing for checking which keys move on removal of a node
func TestRemappingOnNodeRemoval(t *testing.T) {
	r := cluster.NewRing(150)
	r.AddNode("node1:6379")
	r.AddNode("node2:6380")
	r.AddNode("node3:6381")

	// Record which node owns each key before removal
	before := make(map[string]string)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key:%d", i)
		node, _ := r.GetNode(key)
		before[key] = node
	}

	r.RemoveNode("node3:6381")

	remapped := 0
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key:%d", i)
		after, _ := r.GetNode(key)
		if before[key] != after && before[key] != "node3:6381" {
			// A key that was NOT on node3 moved, which is wrong
			remapped++
		}
	}

	// At most 5% extra remapping allowed (as some hash positions could be adjacent)
	if remapped > 500 {
		t.Errorf("too many keys unnecessarily remapped: %d/10000", remapped)
	}
}
