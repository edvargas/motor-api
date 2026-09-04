package memory

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// earthRadiusKm is used to convert the haversine angular distance to
// kilometers.
const earthRadiusKm = 6371.0

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

func (s *WindowStore) MaxDistanceKm(ctx context.Context, customerID string, tx domain.Transaction, span time.Duration) (float64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return 0, fmt.Errorf("window store: %w", domain.ErrEnrichmentUnavailable)
	}
	cutoff := s.now().Add(-span)
	var maxKm float64
	for _, e := range s.byCust[customerID] {
		if e.tx.TransactionID == tx.TransactionID || !e.recordedAt.After(cutoff) {
			continue
		}
		if d := haversineKm(tx.Geo, e.tx.Geo); d > maxKm {
			maxKm = d
		}
	}
	return maxKm, nil
}

func (s *WindowStore) DistinctDeviceCount(ctx context.Context, customerID string, span time.Duration) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return 0, fmt.Errorf("window store: %w", domain.ErrEnrichmentUnavailable)
	}
	cutoff := s.now().Add(-span)
	devices := make(map[string]struct{})
	for _, e := range s.byCust[customerID] {
		if e.tx.DeviceID == "" || !e.recordedAt.After(cutoff) {
			continue
		}
		devices[e.tx.DeviceID] = struct{}{}
	}
	return len(devices), nil
}

// haversineKm returns the great-circle distance between a and b in
// kilometers.
func haversineKm(a, b domain.Geo) float64 {
	lat1, lon1 := a.Lat*math.Pi/180, a.Lon*math.Pi/180
	lat2, lon2 := b.Lat*math.Pi/180, b.Lon*math.Pi/180
	dLat := lat2 - lat1
	dLon := lon2 - lon1

	h := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthRadiusKm * math.Asin(math.Sqrt(h))
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
