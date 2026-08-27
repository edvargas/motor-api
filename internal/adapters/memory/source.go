package memory

import (
	"context"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// ScriptedSource is an in-memory ports.TransactionSource that replays a
// fixed script of transactions, standing in for a partitioned Kafka
// consumer (partitioned by customer_id in a real deployment).
type ScriptedSource struct {
	script []domain.Transaction
}

// NewScriptedSource builds a ScriptedSource that will replay script, in
// order, when Run is called.
func NewScriptedSource(script []domain.Transaction) *ScriptedSource {
	return &ScriptedSource{script: script}
}

func (s *ScriptedSource) Run(ctx context.Context) (<-chan domain.Transaction, error) {
	out := make(chan domain.Transaction)
	go func() {
		defer close(out)
		for _, tx := range s.script {
			select {
			case <-ctx.Done():
				return
			case out <- tx:
				// In a real Kafka consumer, offset commit for this
				// partition would happen here, after the dispatcher has
				// accepted the batch containing tx (at-least-once
				// delivery) — not before, and not per-message, to avoid
				// committing offsets for messages that never reached a
				// worker.
			}
		}
	}()
	return out, nil
}
