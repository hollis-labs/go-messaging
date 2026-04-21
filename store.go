package messaging

import (
	"context"
	"encoding/json"
	"errors"
)

// Sentinel errors returned by Store/Dispatcher implementations.
var (
	ErrNotFound         = errors.New("envelope not found")
	ErrRequestTimeout   = errors.New("request timed out")
	ErrCanceled         = errors.New("envelope canceled")
	ErrStoreUnavailable = errors.New("store unavailable")
	ErrPresetLifecycle  = errors.New("caller set Store-managed lifecycle fields")
)

// Store is the persistence + query contract for envelopes.
//
// All impls (memstore, agent-mux daemon client, Nanite SQLite) MUST
// satisfy this contract identically. The contract test suite at
// github.com/hollis-labs/go-messaging/messagingtest.RunContract
// verifies every Store impl runs through the same behavioral checks.
type Store interface {
	// Send persists an envelope.
	//
	// Store assigns ID (fresh UUIDv7) and CreatedAt (now); any caller-set
	// values on these fields are overwritten. DeliveredAt and ConsumedAt
	// MUST be nil on input — Store returns ErrPresetLifecycle otherwise.
	//
	// Returns the envelope with Store-assigned fields populated.
	Send(ctx context.Context, env Envelope) (Envelope, error)

	// Get retrieves a single envelope by ID. Returns ErrNotFound if absent.
	Get(ctx context.Context, id string) (Envelope, error)

	// Inbox returns undelivered envelopes for `to`, chronologically by
	// CreatedAt (ties broken by UUIDv7 monotonic ID).
	//
	// Side effect: atomically marks returned envelopes as DeliveredAt=now
	// for this recipient. They will NOT appear in future Inbox calls.
	// Filter narrows the result set.
	Inbox(ctx context.Context, to Address, f Filter) ([]Envelope, error)

	// Thread returns envelopes sharing a ThreadID, chronological order.
	// Read-only; no lifecycle side effects. Filter narrows.
	Thread(ctx context.Context, threadID string, f Filter) ([]Envelope, error)

	// Consume advances ConsumedAt for (envelope, recipient). Idempotent.
	Consume(ctx context.Context, id string, recipient Address) error

	// Cancel marks an envelope as dead. Used when a sender wants to abort
	// a request whose response is no longer meaningful. Any in-flight
	// Subscribe wait on InReplyTo=<id> returns cleanly with ErrCanceled.
	// Idempotent.
	Cancel(ctx context.Context, id string) error

	// Subscribe streams newly-created envelopes matching the filter until
	// ctx is canceled. The returned channel closes when ctx is done.
	//
	// Guarantees: matches envelopes created AFTER subscription time only
	// (no historical replay — use Inbox for that).
	Subscribe(ctx context.Context, f Filter) (<-chan Envelope, error)
}

// Dispatcher is the higher-level convenience API.
// Wraps a Store with request/reply semantics.
type Dispatcher interface {
	Store

	// Request sends an envelope as Kind=request and blocks until a
	// matching response (InReplyTo=<request id>) arrives or ctx expires.
	//
	// Request always sets env.Kind = MsgKindRequest and assigns env.ID;
	// caller values on those fields are overwritten. Other fields
	// (From, To, ThreadID, Payload, Metadata) are preserved.
	//
	// On timeout: returns ErrRequestTimeout. The request envelope
	// remains in the Store; caller may invoke Cancel(id) to retire it.
	Request(ctx context.Context, env Envelope) (Envelope, error)

	// Reply constructs and sends a response envelope to `parent`.
	// Convenience: sets Kind=response, InReplyTo=parent.ID, propagates
	// ThreadID, swaps From/To.
	Reply(ctx context.Context, parent Envelope, payload json.RawMessage) (Envelope, error)
}
