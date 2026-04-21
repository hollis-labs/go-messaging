package messaging

import (
	"context"
	"encoding/json"
)

// dispatcher is the canonical Dispatcher implementation, wrapping any Store.
type dispatcher struct {
	Store
}

// NewDispatcher returns a Dispatcher wrapping the given Store. The returned
// Dispatcher IS a Store (it embeds the underlying Store interface).
func NewDispatcher(s Store) Dispatcher {
	return &dispatcher{Store: s}
}

// Reply constructs and sends a response envelope to `parent`.
func (d *dispatcher) Reply(ctx context.Context, parent Envelope, payload json.RawMessage) (Envelope, error) {
	resp := Envelope{
		Kind:        MsgKindResponse,
		From:        parent.To,
		To:          parent.From,
		ThreadID:    parent.ThreadID,
		InReplyTo:   parent.ID,
		Payload:     payload,
		ContentType: "application/json",
	}
	return d.Send(ctx, resp)
}

// Request is implemented in Task 12.
func (d *dispatcher) Request(ctx context.Context, env Envelope) (Envelope, error) {
	// Stub — filled in Task 12.
	panic("messaging: Dispatcher.Request not yet implemented (Task 12)")
}
