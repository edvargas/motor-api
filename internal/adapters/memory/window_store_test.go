package memory

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

func TestWindowStoreCountByChannel(t *testing.T) {
	s := NewWindowStore()
	ctx := context.Background()
	tx := domain.Transaction{CustomerID: "c1", Channel: domain.ChannelPix}

	for i := 0; i < 3; i++ {
		require.NoError(t, s.Record(ctx, "c1", tx, time.Hour))
	}

	n, err := s.CountByChannel(ctx, "c1", domain.ChannelPix, time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 3, n)

	n, err = s.CountByChannel(ctx, "c1", domain.ChannelTED, time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestWindowStoreSetDown(t *testing.T) {
	s := NewWindowStore()
	s.SetDown(true)
	ctx := context.Background()

	err := s.Record(ctx, "c1", domain.Transaction{CustomerID: "c1"}, time.Hour)
	require.ErrorIs(t, err, domain.ErrEnrichmentUnavailable)

	_, err = s.CountByChannel(ctx, "c1", domain.ChannelPix, time.Hour)
	require.ErrorIs(t, err, domain.ErrEnrichmentUnavailable)

	s.SetDown(false)
	require.NoError(t, s.Record(ctx, "c1", domain.Transaction{CustomerID: "c1", Channel: domain.ChannelPix}, time.Hour))
}
