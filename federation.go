package messaging

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// Router is a messaging.Store decorator that routes operations by the
// Authority segment of a message URN, giving any application federated
// messaging for free.
//
// Every Address carries an Authority — the email-style "domain" that owns
// the addressed entity. A Router holds one local Store plus a registry of
// foreign-authority routes:
//
//   - an operation whose authority matches a registered foreign route is
//     dispatched to that route's Store (typically an HTTP-backed Store
//     reaching the host that owns the authority);
//   - every other authority falls through to the local Store.
//
// This makes "internal vs external" a single routing question rather than
// a schema fork: a message is external precisely when its authority has a
// foreign route. A standalone install registers no foreign routes, so every
// authority resolves to the local Store and messaging works fully locally
// with zero extra configuration. Federation is therefore purely additive —
// the same code path serves the standalone and the federated deployment.
//
// Routing is keyed on the recipient authority, since delivery is always to
// the host that owns the recipient:
//
//   - Send      → env.To.Authority
//   - Inbox     → to.Authority
//   - Subscribe → to.Authority
//   - Consume   → recipient.Authority
//
// Get, Thread, and Cancel are keyed by an envelope or thread ID rather than
// an Address, so they carry no authority to route on; the Router serves them
// from the local Store. An application that needs to Get or Cancel an
// envelope held by a foreign authority should call that authority's Store
// directly.
//
// A Router is itself a Store, so it composes: wrap one in NewDispatcher to
// get a federated request/reply Dispatcher, or nest Routers if an authority
// hierarchy calls for it.
//
// All methods are safe for concurrent use; the route registry may be
// mutated (Register/Unregister) while operations are in flight.
type Router struct {
	local          Store
	localAuthority string
	strict         bool

	mu     sync.RWMutex
	routes map[string]Store
}

// Verify Router satisfies the Store contract.
var _ Store = (*Router)(nil)

// RouterOption configures a Router at construction. See WithStrictRouting.
type RouterOption func(*Router)

// WithStrictRouting makes the Router reject any address whose authority is
// neither the local authority nor a registered foreign route, returning
// ErrNoRoute instead of silently falling through to the local Store.
//
// Use it when a misaddressed envelope being delivered locally would be a
// bug rather than a benign default. Strict routing has no effect unless a
// non-empty local authority was passed to NewRouter — without one, the
// Router cannot tell an unknown authority apart from the local domain.
func WithStrictRouting() RouterOption {
	return func(r *Router) { r.strict = true }
}

// NewRouter constructs an authority Router around local, the Store that
// serves localAuthority.
//
// localAuthority is the authority this process owns — its "local domain".
// Pass "" if the application does not distinguish a single local authority;
// every authority without a foreign route is then treated as local. A
// non-empty localAuthority additionally enables the IsLocal check and, with
// WithStrictRouting, ErrNoRoute rejection of unknown authorities.
//
// Foreign routes are added after construction with Register.
func NewRouter(local Store, localAuthority string, opts ...RouterOption) *Router {
	r := &Router{
		local:          local,
		localAuthority: localAuthority,
		routes:         make(map[string]Store),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Register adds (or replaces) a foreign route: messages addressed to
// authority are dispatched to remote, which is typically an HTTP-backed
// Store reaching the host that owns that authority.
//
// It is a configuration error — and returns a non-nil error — to register
// an empty authority, a nil Store, or the Router's own local authority.
// Registering an authority that already has a route replaces it.
func (r *Router) Register(authority string, remote Store) error {
	if authority == "" {
		return fmt.Errorf("messaging: Register requires a non-empty authority")
	}
	if remote == nil {
		return fmt.Errorf("messaging: Register requires a non-nil Store for authority %q", authority)
	}
	if r.localAuthority != "" && authority == r.localAuthority {
		return fmt.Errorf("messaging: cannot register a foreign route for the local authority %q", authority)
	}
	r.mu.Lock()
	r.routes[authority] = remote
	r.mu.Unlock()
	return nil
}

// Unregister removes the foreign route for authority, if one exists.
// Subsequent operations for that authority fall through to the local Store
// (or, in strict mode, return ErrNoRoute). Unregistering an authority with
// no route is a no-op.
func (r *Router) Unregister(authority string) {
	r.mu.Lock()
	delete(r.routes, authority)
	r.mu.Unlock()
}

// Authorities returns the registered foreign authorities, sorted. The local
// authority is never included. The result is a fresh slice the caller owns.
func (r *Router) Authorities() []string {
	r.mu.RLock()
	out := make([]string, 0, len(r.routes))
	for a := range r.routes {
		out = append(out, a)
	}
	r.mu.RUnlock()
	sort.Strings(out)
	return out
}

// IsLocal reports whether authority is served by the local Store — that is,
// it has no registered foreign route. This is the one "internal vs
// external" question the federation model asks: internal and external
// differ only by routing.
func (r *Router) IsLocal(authority string) bool {
	r.mu.RLock()
	_, foreign := r.routes[authority]
	r.mu.RUnlock()
	return !foreign
}

// storeFor resolves the Store serving authority. A registered foreign route
// always wins; otherwise the local Store handles it, unless strict routing
// is on and authority is neither local nor routed, in which case it returns
// ErrNoRoute.
func (r *Router) storeFor(authority string) (Store, error) {
	r.mu.RLock()
	remote, foreign := r.routes[authority]
	r.mu.RUnlock()
	if foreign {
		return remote, nil
	}
	if r.strict && r.localAuthority != "" && authority != r.localAuthority {
		return nil, fmt.Errorf("%w: %q", ErrNoRoute, authority)
	}
	return r.local, nil
}

// Send routes the envelope to the Store owning env.To.Authority.
func (r *Router) Send(ctx context.Context, env Envelope) (Envelope, error) {
	s, err := r.storeFor(env.To.Authority)
	if err != nil {
		return Envelope{}, err
	}
	return s.Send(ctx, env)
}

// Get retrieves an envelope by ID from the local Store. IDs carry no
// authority, so cross-authority Get is not routed; see the Router doc.
func (r *Router) Get(ctx context.Context, id string) (Envelope, error) {
	return r.local.Get(ctx, id)
}

// Inbox routes to the Store owning to.Authority and returns that recipient's
// undelivered envelopes, marking them delivered there.
func (r *Router) Inbox(ctx context.Context, to Address, f Filter) ([]Envelope, error) {
	s, err := r.storeFor(to.Authority)
	if err != nil {
		return nil, err
	}
	return s.Inbox(ctx, to, f)
}

// Thread returns a thread from the local Store. Thread IDs carry no
// authority, so cross-authority Thread is not routed; see the Router doc.
func (r *Router) Thread(ctx context.Context, threadID string, f Filter) ([]Envelope, error) {
	return r.local.Thread(ctx, threadID, f)
}

// Consume routes to the Store owning recipient.Authority and advances
// ConsumedAt for (id, recipient) there.
func (r *Router) Consume(ctx context.Context, id string, recipient Address) error {
	s, err := r.storeFor(recipient.Authority)
	if err != nil {
		return err
	}
	return s.Consume(ctx, id, recipient)
}

// Cancel marks an envelope dead in the local Store. IDs carry no authority,
// so cross-authority Cancel is not routed; see the Router doc.
func (r *Router) Cancel(ctx context.Context, id string) error {
	return r.local.Cancel(ctx, id)
}

// Subscribe routes to the Store owning to.Authority and streams envelopes
// for that recipient. Subscribing to a foreign authority streams from that
// authority's Store — for an HTTP-backed Store, an SSE stream scoped to the
// recipient.
func (r *Router) Subscribe(ctx context.Context, to Address, f Filter) (<-chan Envelope, error) {
	s, err := r.storeFor(to.Authority)
	if err != nil {
		return nil, err
	}
	return s.Subscribe(ctx, to, f)
}
