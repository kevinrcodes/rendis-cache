package store_test

import (
	"fmt"
	"sync"
	"testing"

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
	s.Set("foo", "bar")

	value, ok := s.Get("foo")
	if !ok || value != "bar" {
		t.Errorf("Get(foo) = %q, %v; want \"bar\", true", value, ok)
	}
}

func TestSetOverwrites(t *testing.T) {
	s := store.New()
	s.Set("foo", "first")
	s.Set("foo", "second")

	if value, _ := s.Get("foo"); value != "second" {
		t.Errorf("Get(foo) = %q, want \"second\"", value)
	}
	if got := s.Len(); got != 1 {
		t.Errorf("Len() = %d, want 1", got)
	}
}

func TestEmptyValueIsDistinctFromMissing(t *testing.T) {
	s := store.New()
	s.Set("foo", "")

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
				s.Set(key, key)
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
