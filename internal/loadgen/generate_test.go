package loadgen

import (
	"sort"
	"testing"
	"time"

	"github.com/edvargas05/motor-deteccao/internal/domain"
	"github.com/stretchr/testify/assert"
)

func TestGenerateDeterministicWithSeed(t *testing.T) {
	a := Generate(Options{NumTransactions: 500, NumCustomers: 50, Seed: 42})
	b := Generate(Options{NumTransactions: 500, NumCustomers: 50, Seed: 42})

	assert.Equal(t, len(a), len(b))
	for i := range a {
		assert.Equal(t, a[i], b[i])
	}
}

func TestGenerateProducesPlausibleVolume(t *testing.T) {
	txs := Generate(Options{NumTransactions: 1000, NumCustomers: 100, Seed: 1})
	assert.GreaterOrEqual(t, len(txs), 1000, "duplicates only add to the count, never subtract")
	for _, tx := range txs {
		assert.NoError(t, tx.Validate())
	}
}

// TestGenerateBurstCustomersHaveEnoughPixTransactions verifies the
// velocidade-pix trigger promised in Generate's doc comment actually fires:
// at least one customer must have 9+ pix transactions whose CapturedAt
// timestamps fall within a 300-second window (the rule's span_seconds, per
// internal/config/default_rules.json). Under the old purely-probabilistic
// implementation (marking ~5% of customer indices and hoping enough of
// their random per-transaction draws land on pix within the volume
// generated here), this modest volume/customer count was not guaranteed to
// produce such a customer, so this test would fail intermittently or
// reliably at this scale. The deterministic burst-append fix guarantees it.
func TestGenerateBurstCustomersHaveEnoughPixTransactions(t *testing.T) {
	txs := Generate(Options{NumTransactions: 2000, NumCustomers: 2000, Seed: 7})

	byCustomer := make(map[string][]time.Time)
	for _, tx := range txs {
		if tx.Channel != domain.ChannelPix {
			continue
		}
		byCustomer[tx.CustomerID] = append(byCustomer[tx.CustomerID], tx.CapturedAt)
	}

	const window = 300 * time.Second
	const minRun = 9

	foundBurstCustomer := false
	for _, times := range byCustomer {
		sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })

		// Sliding window: for each start index, count how many timestamps
		// fall within `window` of it.
		for i := range times {
			count := 1
			for j := i + 1; j < len(times) && times[j].Sub(times[i]) < window; j++ {
				count++
			}
			if count >= minRun {
				foundBurstCustomer = true
				break
			}
		}
		if foundBurstCustomer {
			break
		}
	}

	assert.True(t, foundBurstCustomer,
		"expected at least one customer with >=9 pix transactions within a 300s window (velocidade-pix trigger)")
}
