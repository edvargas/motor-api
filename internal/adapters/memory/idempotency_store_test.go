package memory

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdempotencyStoreSeenAndMark(t *testing.T) {
	s := NewIdempotencyStore()
	ctx := context.Background()

	seen, err := s.Seen(ctx, "k1")
	require.NoError(t, err)
	assert.False(t, seen)

	require.NoError(t, s.Mark(ctx, "k1", time.Minute))

	seen, err = s.Seen(ctx, "k1")
	require.NoError(t, err)
	assert.True(t, seen)
}

func TestIdempotencyStoreExpiry(t *testing.T) {
	s := NewIdempotencyStore()
	fixed := time.Now()
	s.now = func() time.Time { return fixed }
	ctx := context.Background()

	require.NoError(t, s.Mark(ctx, "k1", time.Second))
	s.now = func() time.Time { return fixed.Add(2 * time.Second) }

	seen, err := s.Seen(ctx, "k1")
	require.NoError(t, err)
	assert.False(t, seen, "entry should have expired")
}
