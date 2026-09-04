package ports

import (
	"context"
	"time"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// WindowStore maintains the per-customer sliding window used by rules.
// Implementations return a wrapped domain.ErrEnrichmentUnavailable when
// the store is toggled down, so the pipeline can enter degraded mode.
type WindowStore interface {
	// Record appends tx to customerID's window, expiring entries after ttl.
	Record(ctx context.Context, customerID string, tx domain.Transaction, ttl time.Duration) error
	// CountByChannel counts customerID's transactions on channel within the
	// last span (relative to the store's clock).
	CountByChannel(ctx context.Context, customerID string, channel domain.Channel, span time.Duration) (int, error)
	// MaxDistanceKm returns the greatest great-circle distance, in
	// kilometers, between tx.Geo and the geo of any other transaction
	// recorded for customerID within the last span. It returns 0 when no
	// other transaction is on file within span.
	MaxDistanceKm(ctx context.Context, customerID string, tx domain.Transaction, span time.Duration) (float64, error)
	// DistinctDeviceCount counts the distinct device_id values seen for
	// customerID within the last span (relative to the store's clock).
	DistinctDeviceCount(ctx context.Context, customerID string, span time.Duration) (int, error)
}
