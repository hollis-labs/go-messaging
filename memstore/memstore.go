// Package memstore is the in-memory reference implementation of
// messaging.Store. It is primarily intended for tests and embedded
// (process-local) use. It is not durable — all state is lost when the
// process exits.
package memstore

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hollis-labs/go-messaging"
)

// Compile-time assertion: Store satisfies messaging.Store.
var _ messaging.Store = (*Store)(nil)

// Store is an in-memory messaging.Store.
type Store struct {
	mu        sync.Mutex
	envelopes map[string]*memEnvelope // keyed by envelope ID
	// subscribers is filled in later tasks (Subscribe support).
	subscribers []*subscription
	// canceled tracks IDs marked by Cancel so in-flight Request
	// waits can resolve cleanly. Added in later tasks.
	canceled map[string]bool
}

type memEnvelope struct {
	env messaging.Envelope
	// per-recipient delivery tracking: URN → DeliveredAt
	delivered map[string]time.Time
	// per-recipient consumption: URN → ConsumedAt
	consumed map[string]time.Time
}

// New constructs an empty in-memory Store.
func New() *Store {
	return &Store{
		envelopes: make(map[string]*memEnvelope),
		canceled:  make(map[string]bool),
	}
}

// Send persists an envelope, assigning a fresh UUIDv7 ID and CreatedAt.
// Rejects pre-set DeliveredAt/ConsumedAt with ErrPresetLifecycle.
func (s *Store) Send(_ context.Context, env messaging.Envelope) (messaging.Envelope, error) {
	if env.DeliveredAt != nil || env.ConsumedAt != nil {
		return messaging.Envelope{}, messaging.ErrPresetLifecycle
	}
	id, err := uuid.NewV7()
	if err != nil {
		return messaging.Envelope{}, fmt.Errorf("uuid v7: %w", err)
	}
	env.ID = id.String()
	env.CreatedAt = time.Now().UTC()
	env.DeliveredAt = nil
	env.ConsumedAt = nil

	s.mu.Lock()
	s.envelopes[env.ID] = &memEnvelope{
		env:       env,
		delivered: make(map[string]time.Time),
		consumed:  make(map[string]time.Time),
	}
	s.mu.Unlock()

	// Fan-out to live subscribers (Subscribe implemented in later task;
	// noop for now since subscribers slice is nil).
	s.fanOut(env)
	return env, nil
}

// Get retrieves a single envelope by ID.
func (s *Store) Get(_ context.Context, id string) (messaging.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.envelopes[id]
	if !ok {
		return messaging.Envelope{}, messaging.ErrNotFound
	}
	// Return a defensive copy (mostly to avoid the caller mutating shared maps).
	return copyEnvelope(m.env), nil
}

func copyEnvelope(e messaging.Envelope) messaging.Envelope {
	// Metadata + DeliveredAt/ConsumedAt pointers: envelope value copy is enough
	// for the pointer cases (we'll reassign below). Metadata map is shared by
	// default; only copy if it's mutated — memstore never mutates it, so value
	// copy is safe.
	out := e
	if e.DeliveredAt != nil {
		t := *e.DeliveredAt
		out.DeliveredAt = &t
	}
	if e.ConsumedAt != nil {
		t := *e.ConsumedAt
		out.ConsumedAt = &t
	}
	return out
}

// fanOut broadcasts a newly-created envelope to matching live subscribers.
// Non-blocking: if a subscriber's buffer is full, the envelope is dropped
// for that subscriber (memstore's delivery guarantee is Inbox, not Subscribe).
func (s *Store) fanOut(env messaging.Envelope) {
	s.mu.Lock()
	subs := make([]*subscription, len(s.subscribers))
	copy(subs, s.subscribers)
	s.mu.Unlock()

	for _, sub := range subs {
		// Recipient filter: subscriber declared who they are.
		if !sub.to.IsZero() && sub.to != env.To {
			continue
		}
		if !sub.filter.Matches(env) {
			continue
		}
		select {
		case sub.ch <- copyEnvelope(env):
		case <-sub.ctx.Done():
			// subscriber going away; skip.
		default:
			// buffer full; drop for this subscriber. Inbox is the
			// durable path — Subscribe is best-effort live hint.
		}
	}
}

// Inbox returns undelivered envelopes for `to`, chronologically by CreatedAt.
// Side effect: atomically marks returned envelopes DeliveredAt=now for `to`.
func (s *Store) Inbox(_ context.Context, to messaging.Address, f messaging.Filter) ([]messaging.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	toURN := to.URN()
	now := time.Now().UTC()

	// Collect candidates.
	var matches []*memEnvelope
	for _, m := range s.envelopes {
		if m.env.To.URN() != toURN {
			continue
		}
		if _, delivered := m.delivered[toURN]; delivered {
			continue
		}
		if !f.Matches(m.env) {
			continue
		}
		matches = append(matches, m)
	}

	// Sort chronologically by CreatedAt; tie-break on ID (UUIDv7 monotonic).
	sortByCreatedAtAndID(matches)

	// Apply limit.
	if f.Limit > 0 && len(matches) > f.Limit {
		matches = matches[:f.Limit]
	}

	// Atomically mark delivered + build result.
	out := make([]messaging.Envelope, 0, len(matches))
	for _, m := range matches {
		m.delivered[toURN] = now
		env := copyEnvelope(m.env)
		env.DeliveredAt = &now
		out = append(out, env)
	}
	return out, nil
}

func sortByCreatedAtAndID(xs []*memEnvelope) {
	sort.Slice(xs, func(i, j int) bool {
		if xs[i].env.CreatedAt.Equal(xs[j].env.CreatedAt) {
			return xs[i].env.ID < xs[j].env.ID
		}
		return xs[i].env.CreatedAt.Before(xs[j].env.CreatedAt)
	})
}

// Thread returns envelopes sharing a ThreadID, chronological order. Read-only.
func (s *Store) Thread(_ context.Context, threadID string, f messaging.Filter) ([]messaging.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var matches []*memEnvelope
	for _, m := range s.envelopes {
		if m.env.ThreadID != threadID {
			continue
		}
		if !f.Matches(m.env) {
			continue
		}
		matches = append(matches, m)
	}
	sortByCreatedAtAndID(matches)
	if f.Limit > 0 && len(matches) > f.Limit {
		matches = matches[:f.Limit]
	}
	out := make([]messaging.Envelope, 0, len(matches))
	for _, m := range matches {
		out = append(out, copyEnvelope(m.env))
	}
	return out, nil
}

// Consume advances ConsumedAt for (envelope, recipient). Idempotent.
func (s *Store) Consume(_ context.Context, id string, recipient messaging.Address) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.envelopes[id]
	if !ok {
		return messaging.ErrNotFound
	}
	rURN := recipient.URN()
	if _, already := m.consumed[rURN]; already {
		return nil // idempotent
	}
	now := time.Now().UTC()
	m.consumed[rURN] = now
	// Mirror onto the Envelope.ConsumedAt so Get reflects consumption.
	// memstore uses the most-recent consumption timestamp; multi-recipient
	// scenarios should read from Get + inspect per-recipient state via
	// impl-specific extensions (not in v0.1 contract).
	t := now
	m.env.ConsumedAt = &t
	return nil
}

// Cancel marks an envelope as dead. Resolves any in-flight Request
// subscribers waiting on InReplyTo=<id>. Idempotent.
func (s *Store) Cancel(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.envelopes[id]; !ok {
		return messaging.ErrNotFound
	}
	s.canceled[id] = true
	return nil
}

// Subscribe returns a channel that receives newly-created envelopes
// addressed to `to` and matching the filter. Closes when ctx is canceled.
func (s *Store) Subscribe(ctx context.Context, to messaging.Address, f messaging.Filter) (<-chan messaging.Envelope, error) {
	// Buffered modestly so slow consumers don't block the producer for a tick.
	// If a subscriber is too slow, fanOut drops (see comment there).
	sub := &subscription{
		to:     to,
		ch:     make(chan messaging.Envelope, 16),
		filter: f,
		ctx:    ctx,
	}

	s.mu.Lock()
	s.subscribers = append(s.subscribers, sub)
	s.mu.Unlock()

	// Janitor: when ctx is done, remove the sub and close the channel.
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, sv := range s.subscribers {
			if sv == sub {
				s.subscribers = append(s.subscribers[:i], s.subscribers[i+1:]...)
				break
			}
		}
		close(sub.ch)
	}()

	return sub.ch, nil
}

type subscription struct {
	to     messaging.Address
	ch     chan messaging.Envelope
	filter messaging.Filter
	ctx    context.Context
}
