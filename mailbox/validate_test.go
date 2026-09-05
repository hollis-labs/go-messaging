package mailbox

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// fakeResolver is a test double for AgentResolver. It returns a
// synthetic profile for known IDs and sql.ErrNoRows for anything
// else — mirroring the real AgentService.Get behavior, which wraps
// sql.ErrNoRows on missing. The errors.Is-based not-found
// distinction in maybeAutoRegister depends on this shape.
type fakeResolver struct {
	mu    sync.Mutex
	known map[string]bool
	hints map[string]string
}

func (f *fakeResolver) AgentExists(_ context.Context, id string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.known[id], nil
}

func (f *fakeResolver) RegisterAgent(_ context.Context, id, registerAs string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.known[id] {
		return fmt.Errorf("agent %s already exists", id)
	}
	f.known[id] = true
	f.hints[id] = registerAs
	return nil
}

func (f *fakeResolver) registrationHint(id string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hints[id]
}

func newFakeResolver(ids ...string) *fakeResolver {
	m := make(map[string]bool, len(ids))
	for _, id := range ids {
		m[id] = true
	}
	return &fakeResolver{known: m, hints: make(map[string]string)}
}

func TestValidateAgentID_User(t *testing.T) {
	// The "user" sentinel is always valid — resolver not consulted.
	if err := ValidateAgentID(context.Background(), nil, UserSentinel); err != nil {
		t.Errorf("user sentinel rejected: %v", err)
	}
}

// TestValidateAgentID_ArbitraryAgentID pins that ValidateAgentID has no
// format-specific branch: a resolver-known ID validates and an unknown one
// is rejected regardless of its shape.
func TestValidateAgentID_ArbitraryAgentID(t *testing.T) {
	r := newFakeResolver("agt-backend")
	if err := ValidateAgentID(context.Background(), r, "agt-backend"); err != nil {
		t.Errorf("agt-backend rejected: %v", err)
	}
	if err := ValidateAgentID(context.Background(), r, "agt-does-not-exist"); err == nil {
		t.Error("expected rejection for unknown agent id")
	}
}

func TestValidateAgentID_DBUUID(t *testing.T) {
	const uuid = "11111111-1111-1111-1111-111111111111"
	r := newFakeResolver(uuid)
	if err := ValidateAgentID(context.Background(), r, uuid); err != nil {
		t.Errorf("DB agent rejected: %v", err)
	}
	if err := ValidateAgentID(context.Background(), r, "ffffffff-ffff-ffff-ffff-ffffffffffff"); err == nil {
		t.Error("expected rejection for unknown UUID")
	}
}

func TestValidateAgentID_Empty(t *testing.T) {
	if err := ValidateAgentID(context.Background(), nil, ""); err == nil {
		t.Error("expected rejection for empty agent id")
	}
}

func TestValidateAgentID_NilResolver(t *testing.T) {
	// Anything other than the user sentinel requires a resolver.
	if err := ValidateAgentID(context.Background(), nil, "file-backend"); err == nil {
		t.Error("expected rejection when resolver is nil and id is not the user sentinel")
	}
}
