package store

import "time"

const evictionInterval = 100 * time.Millisecond
const maxEvictPerShard = 20 // Don't sweep too many keys per cycle (Bounded Latency)

// defer only runs on return, and this function never returns, so go runEviction() runs indefinitely
func (s *Store) runEviction () {
	ticker := time.NewTicker(evictionInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.sweepExpired()
	}
}

func (s *Store) sweepExpired () {
	for _, sh := range s.shards {
		sh.mu.Lock()
		evicted := 0
		for key, e := range sh.data {
			if e.isExpired() {
				delete(sh.data, key)
				evicted++
				if evicted >= maxEvictPerShard {
					break
				}
			}
		}
		sh.mu.Unlock()
	}
}

