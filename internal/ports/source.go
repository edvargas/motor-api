package ports

import (
	"context"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// TransactionSource is the inbound port that stands in for a partitioned
// Kafka consumer. Run pushes transactions onto the returned channel,
// respecting ctx cancellation, and closes the channel when done.
//
// In a real system, offset commit for a partition would happen here after
// the dispatcher confirms the batch drawn from that partition was handed
// off to workers (at-least-once delivery) — see the mock implementation in
// internal/adapters/memory/source.go for where that call would sit.
type TransactionSource interface {
	Run(ctx context.Context) (<-chan domain.Transaction, error)
}
