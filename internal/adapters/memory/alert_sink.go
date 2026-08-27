package memory

import (
	"context"
	"log/slog"
	"sync"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// AlertSink is an in-memory ports.AlertSink that accumulates every
// published alert and logs it, standing in for the "alertas" topic.
type AlertSink struct {
	mu     sync.Mutex
	alerts []domain.Alert
	logger *slog.Logger
}

// NewAlertSink builds an empty AlertSink that logs through slog.Default().
func NewAlertSink() *AlertSink {
	return &AlertSink{logger: slog.Default()}
}

func (s *AlertSink) Publish(ctx context.Context, alert domain.Alert) error {
	s.mu.Lock()
	s.alerts = append(s.alerts, alert)
	s.mu.Unlock()

	s.logger.InfoContext(ctx, "alert published",
		"alert_id", alert.AlertID,
		"customer_id", alert.CustomerID,
		"transaction_id", alert.TransactionID,
		"severity", alert.Severity,
		"categories", alert.Categories,
		"score", alert.Score,
		"evaluation", alert.Evaluation,
	)
	return nil
}

// Alerts returns a snapshot of every alert published so far.
func (s *AlertSink) Alerts() []domain.Alert {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.Alert, len(s.alerts))
	copy(out, s.alerts)
	return out
}
