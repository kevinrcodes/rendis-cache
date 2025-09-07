// Package store holds the server's keyspace: an in-memory map from string keys
// to string values, safe for concurrent use by many connections.
package store

import "sync"

// entry is one stored value.
type entry struct {
	value string
}

// Store is a concurrent in-memory key-value map. The zero Store is not usable;
// create one with New.
type Store struct {
	mu   sync.RWMutex
	data map[string]entry
}

// New returns an empty Store.
func New() *Store {
	return &Store{data: make(map[string]entry)}
}

// Set stores value under key, replacing any value already there.
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = entry{value: value}
}

// Get returns the value stored under key, and whether the key exists.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	return e.value, ok
}

// Len returns the number of keys held.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
