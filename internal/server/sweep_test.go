package server

import (
	"log/slog"
	"testing"
	"time"

	"my-redis/internal/store"
)

// TestSweepExpiredKeysReclaimsMemory checks the background sweep, which is what
// stops a key that expires and is never read again from being held forever.
func TestSweepExpiredKeysReclaimsMemory(t *testing.T) {
	srv, err := Listen("127.0.0.1:0", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer srv.Close()

	srv.store.Set("expired", "value", store.SetOptions{ExpiresAt: time.Now().Add(-time.Second)})
	srv.store.Set("permanent", "value", store.SetOptions{})

	go srv.sweepExpiredKeys(time.Millisecond)

	deadline := time.Now().Add(5 * time.Second)
	for srv.store.Len() > 1 {
		if time.Now().After(deadline) {
			t.Fatalf("expired key still held after %d keys remained", srv.store.Len())
		}
		time.Sleep(time.Millisecond)
	}
	if _, ok := srv.store.Get("permanent"); !ok {
		t.Error("the sweep removed a key that had not expired")
	}
}
