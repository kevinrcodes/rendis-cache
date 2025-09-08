package store_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"my-redis/internal/store"
)

func TestGetMissingKey(t *testing.T) {
	s := store.New()
	if value, ok := s.Get("absent"); ok {
		t.Errorf("Get(absent) = %q, true; want \"\", false", value)
	}
}

func TestSetThenGet(t *testing.T) {
	s := store.New()
	s.Set("foo", "bar", store.SetOptions{})

	value, ok := s.Get("foo")
	if !ok || value != "bar" {
		t.Errorf("Get(foo) = %q, %v; want \"bar\", true", value, ok)
	}
}

func TestSetOverwrites(t *testing.T) {
	s := store.New()
	s.Set("foo", "first", store.SetOptions{})
	s.Set("foo", "second", store.SetOptions{})

	if value, _ := s.Get("foo"); value != "second" {
		t.Errorf("Get(foo) = %q, want \"second\"", value)
	}
	if got := s.Len(); got != 1 {
		t.Errorf("Len() = %d, want 1", got)
	}
}

func TestEmptyValueIsDistinctFromMissing(t *testing.T) {
	s := store.New()
	s.Set("foo", "", store.SetOptions{})

	if value, ok := s.Get("foo"); !ok || value != "" {
		t.Errorf("Get(foo) = %q, %v; want \"\", true", value, ok)
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := store.New()

	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			for range 100 {
				s.Set(key, key, store.SetOptions{})
				if value, ok := s.Get(key); !ok || value != key {
					t.Errorf("Get(%s) = %q, %v; want %q, true", key, value, ok, key)
					return
				}
			}
		}()
	}
	wg.Wait()

	if got := s.Len(); got != 16 {
		t.Errorf("Len() = %d, want 16", got)
	}
}

// fakeClock is a manually advanced clock, so expiry tests need no sleeping.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func TestKeyIsVisibleUntilItExpires(t *testing.T) {
	clock := newFakeClock()
	s := store.NewWithClock(clock.Now)
	s.Set("foo", "bar", store.SetOptions{ExpiresAt: clock.Now().Add(100 * time.Millisecond)})

	clock.advance(99 * time.Millisecond)
	if value, ok := s.Get("foo"); !ok || value != "bar" {
		t.Errorf("just before expiry: Get(foo) = %q, %v; want \"bar\", true", value, ok)
	}

	// A key expires the instant its expiry time is reached, not after it.
	clock.advance(time.Millisecond)
	if value, ok := s.Get("foo"); ok {
		t.Errorf("at expiry: Get(foo) = %q, true; want \"\", false", value)
	}
}

func TestSetWithoutExpiryClearsAnExistingOne(t *testing.T) {
	clock := newFakeClock()
	s := store.NewWithClock(clock.Now)
	s.Set("foo", "bar", store.SetOptions{ExpiresAt: clock.Now().Add(time.Second)})
	s.Set("foo", "baz", store.SetOptions{})

	clock.advance(time.Hour)
	if value, ok := s.Get("foo"); !ok || value != "baz" {
		t.Errorf("Get(foo) = %q, %v; want \"baz\", true", value, ok)
	}
}

func TestKeepTTLRetainsTheExistingExpiry(t *testing.T) {
	clock := newFakeClock()
	s := store.NewWithClock(clock.Now)
	s.Set("foo", "bar", store.SetOptions{ExpiresAt: clock.Now().Add(time.Second)})

	clock.advance(500 * time.Millisecond)
	s.Set("foo", "baz", store.SetOptions{KeepTTL: true})

	if value, ok := s.Get("foo"); !ok || value != "baz" {
		t.Errorf("Get(foo) = %q, %v; want \"baz\", true", value, ok)
	}
	clock.advance(500 * time.Millisecond)
	if value, ok := s.Get("foo"); ok {
		t.Errorf("after the retained expiry: Get(foo) = %q, true; want \"\", false", value)
	}
}

func TestKeepTTLOnAKeyWithoutOneStoresItForever(t *testing.T) {
	clock := newFakeClock()
	s := store.NewWithClock(clock.Now)
	s.Set("fresh", "value", store.SetOptions{KeepTTL: true})

	s.Set("stale", "old", store.SetOptions{ExpiresAt: clock.Now().Add(time.Second)})
	clock.advance(time.Hour)
	s.Set("stale", "new", store.SetOptions{KeepTTL: true})

	clock.advance(time.Hour)
	for _, key := range []string{"fresh", "stale"} {
		if _, ok := s.Get(key); !ok {
			t.Errorf("Get(%s) reported the key as absent, want it stored without an expiry", key)
		}
	}
}

func TestRemoveExpiredReclaimsOnlyExpiredKeys(t *testing.T) {
	clock := newFakeClock()
	s := store.NewWithClock(clock.Now)
	s.Set("permanent", "value", store.SetOptions{})
	s.Set("short", "value", store.SetOptions{ExpiresAt: clock.Now().Add(time.Second)})
	s.Set("long", "value", store.SetOptions{ExpiresAt: clock.Now().Add(time.Hour)})

	if got := s.RemoveExpired(); got != 0 {
		t.Errorf("RemoveExpired() before anything expired = %d, want 0", got)
	}

	clock.advance(time.Minute)
	if got := s.RemoveExpired(); got != 1 {
		t.Errorf("RemoveExpired() = %d, want 1", got)
	}
	if got := s.Len(); got != 2 {
		t.Errorf("Len() = %d, want 2", got)
	}
	if got := s.RemoveExpired(); got != 0 {
		t.Errorf("RemoveExpired() a second time = %d, want 0", got)
	}
}
