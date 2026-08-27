package domain

import (
	"fmt"
	"time"
)

// Channel identifies the payment rail a transaction moved through.
type Channel string

const (
	ChannelPix  Channel = "pix"
	ChannelTED  Channel = "ted"
	ChannelCard Channel = "card"
)

func (c Channel) Valid() bool {
	switch c {
	case ChannelPix, ChannelTED, ChannelCard:
		return true
	default:
		return false
	}
}

// Geo is the geolocation captured with a transaction.
type Geo struct {
	Country string  `json:"country"`
	Lat     float64 `json:"lat"`
	Lon     float64 `json:"lon"`
}

// Transaction is the inbound event the engine evaluates.
type Transaction struct {
	CustomerID    string    `json:"customer_id"`
	TransactionID string    `json:"transaction_id"`
	Amount        float64   `json:"amount"`
	Currency      string    `json:"currency"`
	Channel       Channel   `json:"channel"`
	DeviceID      string    `json:"device_id"`
	Geo           Geo       `json:"geo"`
	CapturedAt    time.Time `json:"captured_at"`
}

// Validate rejects structurally invalid transactions before any processing.
func (t Transaction) Validate() error {
	if t.CustomerID == "" {
		return fmt.Errorf("%w: customer_id is required", ErrInvalidTransaction)
	}
	if t.TransactionID == "" {
		return fmt.Errorf("%w: transaction_id is required", ErrInvalidTransaction)
	}
	if t.Amount <= 0 {
		return fmt.Errorf("%w: amount must be positive", ErrInvalidTransaction)
	}
	if t.Currency == "" {
		return fmt.Errorf("%w: currency is required", ErrInvalidTransaction)
	}
	if !t.Channel.Valid() {
		return fmt.Errorf("%w: unknown channel %q", ErrInvalidTransaction, t.Channel)
	}
	if t.CapturedAt.IsZero() {
		return fmt.Errorf("%w: captured_at is required", ErrInvalidTransaction)
	}
	return nil
}

// IdempotencyKey is the key used by IdempotencyStore for this transaction.
func (t Transaction) IdempotencyKey() string {
	return t.CustomerID + ":" + t.TransactionID
}
