package ports

import (
	"context"
	"time"
)

// IdempotencyStore tracks transaction keys already processed.
type IdempotencyStore interface {
	// Seen reports whether key was already marked.
	Seen(ctx context.Context, key string) (bool, error)
	// Mark records key as processed for ttl.
	Mark(ctx context.Context, key string, ttl time.Duration) error
}
