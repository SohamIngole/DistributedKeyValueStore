package store_test

import (
	"fmt"
	"sync"
	"testing"

	"DistributedKeyValueStore/internal/store"
)

// Baseline: sequential set
func BenchmarkSetSequential(b *testing.B) {
	s := store.New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set(fmt.Sprintf("key:%d", i%10000), "value", 0) // Cost of "Set" on a stable-size, already-populated map
	}
}

// Real-world: concurrent reads dominate
func BenchmarkSetParallelHighContention(b *testing.B) {
	s := store.New()
	s.Set("hotkey", "value", 0) // one key, many readers
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Get("hotkey")
		}
	})
}

// Realistic mixed workload: 90% reads, 10% writes
func BenchmarkMixedWorkload(b *testing.B) {
	s := store.New()
	for i := 0; i < 10000; i++ {
		s.Set(fmt.Sprintf("key:%d", i), "value", 0) // Pre-populating 10k keys
	}

	var wg sync.WaitGroup
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if i%10 == 0 {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				s.Set(fmt.Sprintf("key:%d", n%10000), "newvalue", 0)
			}(i)
		}
	}
	wg.Wait()
}
