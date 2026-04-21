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

// Request sends an envelope as Kind=request and blocks until a matching
// response (InReplyTo=<request id>) arrives or ctx expires.
func (d *dispatcher) Request(ctx context.Context, env Envelope) (Envelope, error) {
	env.Kind = MsgKindRequest
	env.ID = "" // force Store to assign fresh ID

	// Subscribe BEFORE Send so we don't miss a fast response.
	// Filter on Kind=response; we'll match InReplyTo after.
	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()

	sub, err := d.Store.Subscribe(subCtx, Filter{Kind: []Kind{MsgKindResponse}})
	if err != nil {
		return Envelope{}, err
	}

	sent, err := d.Store.Send(ctx, env)
	if err != nil {
		return Envelope{}, err
	}

	for {
		select {
		case resp, ok := <-sub:
			if !ok {
				// Channel closed; ctx done.
				if ctx.Err() == context.DeadlineExceeded {
					return Envelope{}, ErrRequestTimeout
				}
				return Envelope{}, ctx.Err()
			}
			if resp.InReplyTo == sent.ID {
				return resp, nil
			}
			// Different response; ignore and keep waiting.
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return Envelope{}, ErrRequestTimeout
			}
			return Envelope{}, ctx.Err()
		}
	}
}
