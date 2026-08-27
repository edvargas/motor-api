package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// RiskStore is an in-memory ports.RiskStore seeded with a fixed set of
// personalized profiles, and a failure toggle for demoing degraded mode.
type RiskStore struct {
	mu       sync.RWMutex
	profiles map[string]domain.RiskProfile
	down     bool
}

// NewRiskStore builds a RiskStore seeded with custom (customerID -> profile).
func NewRiskStore(custom map[string]domain.RiskProfile) *RiskStore {
	profiles := make(map[string]domain.RiskProfile, len(custom))
	for k, v := range custom {
		profiles[k] = v
	}
	return &RiskStore{profiles: profiles}
}

// SetDown toggles simulated unavailability for every subsequent call.
func (s *RiskStore) SetDown(down bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.down = down
}

func (s *RiskStore) Get(ctx context.Context, customerID string) (*domain.RiskProfile, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.down {
		return nil, fmt.Errorf("risk store: %w", domain.ErrEnrichmentUnavailable)
	}
	p, ok := s.profiles[customerID]
	if !ok {
		return nil, domain.ErrRiskProfileNotFound
	}
	return &p, nil
}
