// Package memstore is the in-memory reference implementation of
// messaging.Store. It is primarily intended for tests and embedded
// (process-local) use. It is not durable — all state is lost when the
// process exits.
package memstore

import (
	"context"
	"fmt"
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

// Placeholder — fully implemented in Task 10.
func (s *Store) fanOut(_ messaging.Envelope) {}

// Placeholder stubs — implemented in later tasks, so the Store interface is
// satisfied from Task 6 onward. Each returns a "not yet implemented" error
// so accidental early use surfaces fast.
func (s *Store) Inbox(context.Context, messaging.Address, messaging.Filter) ([]messaging.Envelope, error) {
	return nil, fmt.Errorf("memstore: Inbox not yet implemented")
}
func (s *Store) Thread(context.Context, string, messaging.Filter) ([]messaging.Envelope, error) {
	return nil, fmt.Errorf("memstore: Thread not yet implemented")
}
func (s *Store) Consume(context.Context, string, messaging.Address) error {
	return fmt.Errorf("memstore: Consume not yet implemented")
}
func (s *Store) Cancel(context.Context, string) error {
	return fmt.Errorf("memstore: Cancel not yet implemented")
}
func (s *Store) Subscribe(context.Context, messaging.Filter) (<-chan messaging.Envelope, error) {
	return nil, fmt.Errorf("memstore: Subscribe not yet implemented")
}

type subscription struct {
	// filled in Task 10
}
