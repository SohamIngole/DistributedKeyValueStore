package store

import (
	"hash/fnv"
	"sync"
	"time"
)

const numShards = 256

type entry struct {
	value     string
	expiresAt time.Time // zero value means no expiry
}

// Used at the time of Lazy deletion
func (e entry) isExpired() bool {
	return !e.expiresAt.IsZero() && time.Now().After(e.expiresAt)
}

type shard struct {
	mu   sync.RWMutex
	data map[string]entry
}

type Store struct {
	shards [numShards]*shard
}

func New() *Store {
	s := &Store{}

	for i := range s.shards {
		s.shards[i] = &shard{
			data: make(map[string]entry),
		}
	}

	go s.runEviction() // background eviction goroutine, clears expired keys periodically
	return s
}

func (s *Store) getShard(key string) *shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return s.shards[h.Sum32()%numShards]
}

func (s *Store) Set(key, value string, ttl time.Duration) { // ttl = 0 means no expiry
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := entry{value: value}
	if ttl > 0 {
		e.expiresAt = time.Now().Add(ttl)
	}

	sh.data[key] = e
}

func (s *Store) Get(key string) (string, bool) {
	sh := s.getShard(key)
	sh.mu.RLock()
	e, ok := sh.data[key]
	sh.mu.RUnlock()

	if !ok {
		return "", false
	}

	if e.isExpired() {
		// Lazy deletion
		sh.mu.Lock()
		delete(sh.data, key)
		sh.mu.Unlock()
		return "", false
	}

	return e.value, true
}

func (s *Store) Delete(keys ...string) int {
	deleted := 0
	for _, key := range keys {
		sh := s.getShard(key)
		sh.mu.Lock()
		if _, ok := sh.data[key]; ok {
			delete(sh.data, key)
			deleted++
		}
		sh.mu.Unlock()
	}
	return deleted
}

func (s *Store) Exists(key string) bool {
	_, ok := s.Get(key)
	return ok
}

// Returns time left until expiry
// Returns -1 if key exists but has no expiry
// Returns -2 if key does not exist
func (s *Store) TTL(key string) time.Duration {
	sh := s.getShard(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e, ok := sh.data[key]
	if !ok || e.isExpired() {
		return -2
	}
	if e.expiresAt.IsZero() {
		return -1
	}

	remaining := time.Until(e.expiresAt)
	if remaining < 0 {
		return -2
	}
	return remaining
}

func (s *Store) Len() int {
	total := 0

	for _, sh := range s.shards {
		sh.mu.RLock()
		total += len(sh.data)
		sh.mu.RUnlock()
	}
	return total
}

// Extensions

// Sets a TTL on an existing key. Returns false if key doesn't exist.
func (s *Store) Expire(key string, ttl time.Duration) bool {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, ok := sh.data[key]
	if !ok || e.isExpired() {
		return false
	}

	e.expiresAt = time.Now().Add(ttl)
	sh.data[key] = e
	return true
}

// Removes the TTL on the key (makes it permenant). Returns false if key missing
func (s *Store) Persist(key string) bool {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, ok := sh.data[key]
	if !ok || e.isExpired() {
		return false
	}

	e.expiresAt = time.Time{}
	sh.data[key] = e
	return true
}

// Returns all live keys matching a simple glob pattern (or all if pattern = "*")
// O(N) - only use for debugging
func (s *Store) Keys(pattern string) []string {
	var keys []string

	for _, sh := range s.shards {
		sh.mu.RLock()
		for key, e := range sh.data {
			if !e.isExpired() && matchPattern(pattern, key) {
				keys = append(keys, key)
			}
		}
		sh.mu.RUnlock()
	}
	return keys
}

// implements simple glob matching (* = any sequence)
func matchPattern (pattern, key string) bool {
	if pattern == "*" {
		return true
	}
	// Exact Match for now
	return pattern == key
}