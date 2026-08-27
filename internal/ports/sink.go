package ports

import (
	"context"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// AlertSink is the outbound port that stands in for publishing to the
// "alertas" topic.
type AlertSink interface {
	Publish(ctx context.Context, alert domain.Alert) error
}
