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

func TestWindowStoreMaxDistanceKm(t *testing.T) {
	s := NewWindowStore()
	ctx := context.Background()

	// Sao Paulo
	saoPaulo := domain.Transaction{CustomerID: "c1", TransactionID: "t1", Geo: domain.Geo{Lat: -23.55, Lon: -46.63}}
	require.NoError(t, s.Record(ctx, "c1", saoPaulo, time.Hour))

	// Rio de Janeiro, ~360km away
	rio := domain.Transaction{CustomerID: "c1", TransactionID: "t2", Geo: domain.Geo{Lat: -22.91, Lon: -43.17}}
	require.NoError(t, s.Record(ctx, "c1", rio, time.Hour))

	d, err := s.MaxDistanceKm(ctx, "c1", rio, time.Hour)
	require.NoError(t, err)
	assert.InDelta(t, 360, d, 20)

	// A transaction not on file yet should compare against everything recorded so far.
	d, err = s.MaxDistanceKm(ctx, "c1", saoPaulo, time.Hour)
	require.NoError(t, err)
	assert.InDelta(t, 360, d, 20)
}

func TestWindowStoreDistinctDeviceCount(t *testing.T) {
	s := NewWindowStore()
	ctx := context.Background()

	require.NoError(t, s.Record(ctx, "c1", domain.Transaction{CustomerID: "c1", DeviceID: "d1"}, time.Hour))
	n, err := s.DistinctDeviceCount(ctx, "c1", time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	require.NoError(t, s.Record(ctx, "c1", domain.Transaction{CustomerID: "c1", DeviceID: "d2"}, time.Hour))
	n, err = s.DistinctDeviceCount(ctx, "c1", time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
}

func TestWindowStoreSetDown(t *testing.T) {
	s := NewWindowStore()
	s.SetDown(true)
	ctx := context.Background()

	err := s.Record(ctx, "c1", domain.Transaction{CustomerID: "c1"}, time.Hour)
	require.ErrorIs(t, err, domain.ErrEnrichmentUnavailable)

	_, err = s.CountByChannel(ctx, "c1", domain.ChannelPix, time.Hour)
	require.ErrorIs(t, err, domain.ErrEnrichmentUnavailable)

	_, err = s.MaxDistanceKm(ctx, "c1", domain.Transaction{CustomerID: "c1"}, time.Hour)
	require.ErrorIs(t, err, domain.ErrEnrichmentUnavailable)

	_, err = s.DistinctDeviceCount(ctx, "c1", time.Hour)
	require.ErrorIs(t, err, domain.ErrEnrichmentUnavailable)

	s.SetDown(false)
	require.NoError(t, s.Record(ctx, "c1", domain.Transaction{CustomerID: "c1", Channel: domain.ChannelPix}, time.Hour))
}
