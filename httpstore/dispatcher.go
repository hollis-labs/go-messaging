package httpstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	messaging "github.com/hollis-labs/go-messaging"
)

// Dispatcher is a messaging.Dispatcher over the same server as its embedded
// Store. Request is one blocking POST {base}/request when the profile has
// BlockingRequest (Tether), and otherwise the generic Subscribe-then-Send
// helper from messaging.NewDispatcher. Reply always is the generic helper.
type Dispatcher struct {
	*Store
	inner messaging.Dispatcher
}

var _ messaging.Dispatcher = (*Dispatcher)(nil)

// NewDispatcher is New plus request/reply.
func NewDispatcher(baseURL string, opts ...Option) (*Dispatcher, error) {
	s, err := New(baseURL, opts...)
	if err != nil {
		return nil, err
	}
	return &Dispatcher{Store: s, inner: messaging.NewDispatcher(s)}, nil
}

// Reply sends a response to parent (messaging.Dispatcher semantics).
func (d *Dispatcher) Reply(ctx context.Context, parent messaging.Envelope, payload json.RawMessage) (messaging.Envelope, error) {
	return d.inner.Reply(ctx, parent, payload)
}

// Request sends env as a request and waits for the correlated response. It
// returns messaging.ErrRequestTimeout when ctx's deadline passes or the
// server answers 504. With a blocking profile the remaining time of ctx's
// deadline is passed to the server as timeout=<Go duration>; the call is not
// bounded by WithTimeout.
func (d *Dispatcher) Request(ctx context.Context, env messaging.Envelope) (messaging.Envelope, error) {
	if err := d.unsupported(OpRequest); err != nil {
		return messaging.Envelope{}, err
	}
	if !d.profile.BlockingRequest {
		return d.inner.Request(ctx, env)
	}
	if env.DeliveredAt != nil || env.ConsumedAt != nil {
		return messaging.Envelope{}, messaging.ErrPresetLifecycle
	}
	env.Kind = messaging.MsgKindRequest
	env.ID = ""
	env.CreatedAt = time.Time{}

	c := call{op: OpRequest, method: http.MethodPost, path: "request", body: env}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return messaging.Envelope{}, messaging.ErrRequestTimeout
		}
		c.query = url.Values{"timeout": {remaining.String()}}
	}
	var out messaging.Envelope
	if err := d.exchange(ctx, d.stream, c, &out); err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return messaging.Envelope{}, fmt.Errorf("%w: %w", messaging.ErrRequestTimeout, err)
		}
		return messaging.Envelope{}, err
	}
	return out, nil
}
