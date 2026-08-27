// Package loadgen produces synthetic transaction traffic to exercise the
// engine's performance through the HTTP API, and fires it with a small
// internal load-testing client.
package loadgen

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// Options configures synthetic transaction generation.
type Options struct {
	NumTransactions int   // total transactions to generate; preset 100_000
	NumCustomers    int   // distinct customers to spread them across; preset 5_000
	Seed            int64 // 0 means "use a time-derived seed" (non-reproducible)
}

var channels = []domain.Channel{domain.ChannelPix, domain.ChannelTED, domain.ChannelCard}

// Generate produces opts.NumTransactions synthetic transactions, with
// purposeful triggers baked in so the demo isn't alert-sparse:
//   - ~5% of customers get a burst of 9 pix transactions in quick
//     succession (triggers velocidade-pix).
//   - ~10% of transactions use an amount above the default limite_valor
//     of 5000 (triggers valor-atipico).
//   - ~2% of transactions are exact duplicates of an earlier one in the
//     same batch (exercises idempotency).
//
// Determinism: pass a non-zero Seed to get the same output across runs.
func Generate(opts Options) []domain.Transaction {
	seed := opts.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))

	customers := make([]string, opts.NumCustomers)
	for i := range customers {
		customers[i] = fmt.Sprintf("cust-%08d-0000-0000-0000-000000000000", i)
	}

	burstCustomers := make(map[int]bool)
	for i := 0; i < opts.NumCustomers/20; i++ { // ~5%
		burstCustomers[rng.Intn(opts.NumCustomers)] = true
	}

	out := make([]domain.Transaction, 0, opts.NumTransactions)
	base := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)

	for i := 0; i < opts.NumTransactions; i++ {
		custIdx := rng.Intn(opts.NumCustomers)
		channel := channels[rng.Intn(len(channels))]
		if burstCustomers[custIdx] {
			channel = domain.ChannelPix
		}

		amount := 50 + rng.Float64()*450 // plausible baseline: 50-500
		if rng.Float64() < 0.10 {        // ~10% above default limite_valor
			amount = 5100 + rng.Float64()*10000
		}

		tx := domain.Transaction{
			CustomerID:    customers[custIdx],
			TransactionID: fmt.Sprintf("tx-%010d", i),
			Amount:        amount,
			Currency:      "BRL",
			Channel:       channel,
			DeviceID:      fmt.Sprintf("device-%06d", custIdx),
			Geo:           domain.Geo{Country: "BR", Lat: -23.55, Lon: -46.63},
			CapturedAt:    base.Add(time.Duration(i) * time.Millisecond),
		}
		out = append(out, tx)

		if rng.Float64() < 0.02 && len(out) > 1 { // ~2% exact duplicates
			dup := out[rng.Intn(len(out))]
			dup.CapturedAt = tx.CapturedAt
			out = append(out, dup)
		}
	}

	return out
}
