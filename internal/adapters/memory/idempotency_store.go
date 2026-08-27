package memory

import (
	"context"
	"sync"
	"time"
)

type idempotencyEntry struct {
	expiresAt time.Time
}

// IdempotencyStore is an in-memory ports.IdempotencyStore with TTL expiry.
type IdempotencyStore struct {
	mu      sync.Mutex
	entries map[string]idempotencyEntry
	now     func() time.Time
}

// NewIdempotencyStore builds an empty IdempotencyStore.
func NewIdempotencyStore() *IdempotencyStore {
	return &IdempotencyStore{
		entries: make(map[string]idempotencyEntry),
		now:     time.Now,
	}
}

func (s *IdempotencyStore) Seen(ctx context.Context, key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[key]
	if !ok {
		return false, nil
	}
	if s.now().After(e.expiresAt) {
		delete(s.entries, key)
		return false, nil
	}
	return true, nil
}

func (s *IdempotencyStore) Mark(ctx context.Context, key string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[key] = idempotencyEntry{expiresAt: s.now().Add(ttl)}
	return nil
}
