package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

type windowEntry struct {
	tx         domain.Transaction
	recordedAt time.Time
}

// WindowStore is an in-memory ports.WindowStore with TTL expiry and a
// failure toggle for demoing degraded mode.
type WindowStore struct {
	mu     sync.Mutex
	byCust map[string][]windowEntry
	down   bool
	now    func() time.Time
}

// NewWindowStore builds an empty WindowStore.
func NewWindowStore() *WindowStore {
	return &WindowStore{
		byCust: make(map[string][]windowEntry),
		now:    time.Now,
	}
}

// SetDown toggles simulated unavailability for every subsequent call.
func (s *WindowStore) SetDown(down bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.down = down
}

func (s *WindowStore) Record(ctx context.Context, customerID string, tx domain.Transaction, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return fmt.Errorf("window store: %w", domain.ErrEnrichmentUnavailable)
	}
	s.evictLocked(customerID, ttl)
	s.byCust[customerID] = append(s.byCust[customerID], windowEntry{tx: tx, recordedAt: s.now()})
	return nil
}

func (s *WindowStore) CountByChannel(ctx context.Context, customerID string, channel domain.Channel, span time.Duration) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return 0, fmt.Errorf("window store: %w", domain.ErrEnrichmentUnavailable)
	}
	cutoff := s.now().Add(-span)
	count := 0
	for _, e := range s.byCust[customerID] {
		if e.tx.Channel == channel && e.recordedAt.After(cutoff) {
			count++
		}
	}
	return count, nil
}

// evictLocked drops entries older than ttl for customerID. Caller must hold s.mu.
func (s *WindowStore) evictLocked(customerID string, ttl time.Duration) {
	cutoff := s.now().Add(-ttl)
	entries := s.byCust[customerID]
	kept := entries[:0]
	for _, e := range entries {
		if e.recordedAt.After(cutoff) {
			kept = append(kept, e)
		}
	}
	s.byCust[customerID] = kept
}
