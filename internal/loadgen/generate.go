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
//   - ~5% of customers are marked as "burst customers"; each one is
//     guaranteed at least 9 consecutive pix transactions with CapturedAt
//     timestamps one second apart (well inside the velocidade-pix rule's
//     300-second window), reliably triggering velocidade-pix. Marked
//     customers may also incidentally draw pix transactions during the
//     normal random generation below.
//   - ~10% of transactions use an amount above the default limite_valor
//     of 5000 (triggers valor-atipico).
//   - ~2% of transactions are exact duplicates of an earlier one in the
//     same batch (exercises idempotency).
//
// The guaranteed bursts are appended after the main random stream, so the
// total output count is opts.NumTransactions plus the burst and duplicate
// transactions.
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

	// Guarantee each marked burst customer a deterministic burst of at
	// least 9 pix transactions, spaced 1 second apart, so velocidade-pix
	// reliably fires regardless of how the random draws above landed.
	const burstSize = 9
	burstBase := base.Add(time.Duration(opts.NumTransactions) * time.Millisecond)
	burstSeq := 0
	for custIdx := 0; custIdx < opts.NumCustomers; custIdx++ {
		if !burstCustomers[custIdx] {
			continue
		}
		for j := 0; j < burstSize; j++ {
			tx := domain.Transaction{
				CustomerID:    customers[custIdx],
				TransactionID: fmt.Sprintf("tx-burst-%06d-%02d", custIdx, j),
				Amount:        50 + rng.Float64()*450,
				Currency:      "BRL",
				Channel:       domain.ChannelPix,
				DeviceID:      fmt.Sprintf("device-%06d", custIdx),
				Geo:           domain.Geo{Country: "BR", Lat: -23.55, Lon: -46.63},
				CapturedAt:    burstBase.Add(time.Duration(burstSeq) * time.Second),
			}
			out = append(out, tx)
			burstSeq++
		}
	}

	return out
}
