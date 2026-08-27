package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/edvargas05/motor-deteccao/internal/domain"
	"github.com/edvargas05/motor-deteccao/internal/pipeline/dispatch"
)

// TestSameCustomerBatchThroughPoolMatchesSequential is the spec's
// explicitly required concurrency test: a batch of several events for the
// same customer_id, processed in parallel through the real dispatch.Pool,
// must produce the same verdicts (in submission order) as processing the
// identical events sequentially, direct against Processor.Process. This
// proves the pool's per-customer ordering guarantee means the sliding
// window never corrupts under real concurrency — the two prior
// per-package tests (pool_test.go's trivial closures, processor_test.go's
// single-goroutine calls) could not have caught a corruption here, since
// neither drives Processor and WindowStore through the pool together.
func TestSameCustomerBatchThroughPoolMatchesSequential(t *testing.T) {
	burstTxs := func() []domain.Transaction {
		txs := make([]domain.Transaction, 0, 12)
		for i := 0; i < 9; i++ {
			txs = append(txs, makeTx("burst-customer", txID(i), 100, domain.ChannelPix))
		}
		// Mix in a couple of valor-atipico triggers and a duplicate so the
		// comparison exercises more than one rule and idempotency too.
		txs = append(txs, makeTx("burst-customer", "high-1", 6000, domain.ChannelCard))
		txs = append(txs, makeTx("burst-customer", "high-2", 6000, domain.ChannelCard))
		txs = append(txs, txs[0]) // duplicate of the first burst transaction
		return txs
	}

	// Run 1: sequential, direct against Processor.Process.
	seqProcessor, _, _, _ := newTestProcessor(t)
	var sequentialVerdicts []Verdict
	for _, tx := range burstTxs() {
		v, err := seqProcessor.Process(context.Background(), tx)
		require.NoError(t, err)
		sequentialVerdicts = append(sequentialVerdicts, v)
	}

	// Run 2: identical fresh transactions, submitted through a real
	// multi-worker dispatch.Pool, all for the same customer_id.
	poolProcessor, _, _, _ := newTestProcessor(t)
	pool := dispatch.NewPool(8) // more workers than customers, to actually exercise routing
	defer pool.Close()

	type result struct {
		index   int
		verdict Verdict
	}
	results := make(chan result, len(burstTxs()))

	for i, tx := range burstTxs() {
		i, tx := i, tx
		enqueued := pool.Submit(context.Background(), dispatch.Job{
			CustomerID: tx.CustomerID,
			Run: func(jobCtx context.Context) {
				v, err := poolProcessor.Process(context.Background(), tx)
				require.NoError(t, err)
				results <- result{index: i, verdict: v}
			},
		})
		require.True(t, enqueued, "pool should accept every job in this test (no cancellation)")
	}

	pooledVerdicts := make([]Verdict, len(burstTxs()))
	for range burstTxs() {
		select {
		case r := <-results:
			pooledVerdicts[r.index] = r.verdict
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for pooled verdicts")
		}
	}

	require.Len(t, pooledVerdicts, len(sequentialVerdicts))
	for i := range sequentialVerdicts {
		seq, pooled := sequentialVerdicts[i], pooledVerdicts[i]
		assert.Equal(t, seq.Duplicate, pooled.Duplicate, "verdict %d: duplicate flag mismatch", i)
		assert.Equal(t, seq.Partial, pooled.Partial, "verdict %d: partial flag mismatch", i)
		if seq.Alert == nil {
			assert.Nil(t, pooled.Alert, "verdict %d: expected no alert", i)
			continue
		}
		require.NotNil(t, pooled.Alert, "verdict %d: expected an alert", i)
		assert.Equal(t, seq.Alert.Severity, pooled.Alert.Severity, "verdict %d: severity mismatch", i)
		assert.ElementsMatch(t, seq.Alert.Categories, pooled.Alert.Categories, "verdict %d: categories mismatch", i)
		assert.Equal(t, seq.Alert.Score, pooled.Alert.Score, "verdict %d: score mismatch", i)
	}

	// The 9th pix transaction (index 8) must have triggered velocidade-pix
	// in both runs — this is the concrete proof the window didn't corrupt:
	// if pooled processing reordered or dropped window updates, this
	// wouldn't fire, or would fire at the wrong index.
	require.NotNil(t, sequentialVerdicts[8].Alert, "sequential run: 9th pix tx should trigger velocidade-pix")
	require.NotNil(t, pooledVerdicts[8].Alert, "pooled run: 9th pix tx should trigger velocidade-pix")
	assert.Contains(t, pooledVerdicts[8].Alert.Categories, "velocidade")

	// The final duplicate (index 11, a repeat of index 0) must be caught
	// as a duplicate in both runs, proving idempotency state isn't
	// corrupted by concurrent submission either.
	assert.True(t, sequentialVerdicts[11].Duplicate)
	assert.True(t, pooledVerdicts[11].Duplicate)
}
