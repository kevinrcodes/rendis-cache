// Package store holds the server's keyspace: an in-memory map from string keys
// to string values, safe for concurrent use by many connections. Keys may carry
// an expiry, after which they behave as if they had never been set.
package store

import (
	"sync"
	"time"
)

// entry is one stored value together with its optional expiry.
type entry struct {
	value string

	// expiresAt is when the key stops being visible. The zero time means the
	// key lives until it is overwritten.
	expiresAt time.Time
}

// expired reports whether the entry should no longer be visible at now.
func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && !now.Before(e.expiresAt)
}

// SetOptions modifies how a value is stored.
type SetOptions struct {
	// ExpiresAt is when the key should expire. The zero time stores the key
	// without an expiry, discarding any expiry it already had.
	ExpiresAt time.Time

	// KeepTTL retains the expiry already on the key, if any, ignoring
	// ExpiresAt.
	KeepTTL bool
}

// Store is a concurrent in-memory key-value map. The zero Store is not usable;
// create one with New.
type Store struct {
	mu   sync.RWMutex
	data map[string]entry

	// now reads the clock. It is a field so that tests can drive expiry
	// without sleeping.
	now func() time.Time
}

// New returns an empty Store that expires keys against the wall clock.
func New() *Store { return NewWithClock(time.Now) }

// NewWithClock returns an empty Store that reads the time from now.
func NewWithClock(now func() time.Time) *Store {
	return &Store{data: make(map[string]entry), now: now}
}

// Now returns the store's current time. Callers computing an expiry relative to
// "now" should use this, so that they agree with the store about the clock.
func (s *Store) Now() time.Time { return s.now() }

// Set stores value under key, replacing any value already there.
func (s *Store) Set(key, value string, opts SetOptions) {
	s.mu.Lock()
	defer s.mu.Unlock()

	expiresAt := opts.ExpiresAt
	if opts.KeepTTL {
		// An expiry already in the past counts as no expiry: the old entry is
		// logically gone, and the new value should outlive it.
		if old, ok := s.data[key]; ok && !old.expired(s.now()) {
			expiresAt = old.expiresAt
		} else {
			expiresAt = time.Time{}
		}
	}
	s.data[key] = entry{value: value, expiresAt: expiresAt}
}

// Get returns the value stored under key, and whether the key exists and has
// not expired.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok || e.expired(s.now()) {
		// Expired entries are hidden here and reclaimed by RemoveExpired, so
		// that a read does not need the write lock.
		return "", false
	}
	return e.value, true
}

// RemoveExpired deletes every entry whose expiry has passed and returns how
// many were removed. Callers should run it periodically: without it, a key that
// expires and is never read again holds its memory forever.
func (s *Store) RemoveExpired() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	removed := 0
	for key, e := range s.data {
		if e.expired(now) {
			delete(s.data, key)
			removed++
		}
	}
	return removed
}

// Len returns the number of keys held, including expired keys that have not yet
// been reclaimed.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
