package mailbox

import (
	"context"
	"fmt"
)

// AgentResolver reports whether an agent ID is registered in the host
// application. The mailbox package deliberately does not prescribe the
// host's agent record shape or persistence mechanism.
type AgentResolver interface {
	AgentExists(ctx context.Context, id string) (bool, error)
}

// AgentRegistrar optionally registers an unknown sender on first send.
// registerAs is an opaque host-defined hint carried from SendInput. The
// host owns all record shape, validation, defaulting, and persistence
// policy. A nil registrar disables auto-registration.
type AgentRegistrar interface {
	RegisterAgent(ctx context.Context, agentID, registerAs string) error
}

// ValidateAgentID checks that agentID is known to the host resolver. Hosts
// that reserve synthetic identities (for example a human participant) expose
// them through the resolver just like any other addressable participant; the
// shared package assigns no privileged identifiers of its own.
func ValidateAgentID(ctx context.Context, r AgentResolver, agentID string) error {
	if agentID == "" {
		return fmt.Errorf("empty agent id")
	}
	if r == nil {
		return fmt.Errorf("cannot validate agent id %q without resolver", agentID)
	}
	exists, err := r.AgentExists(ctx, agentID)
	if err != nil {
		return fmt.Errorf("resolve agent %q: %w", agentID, err)
	}
	if !exists {
		return fmt.Errorf("agent not found: %s", agentID)
	}
	return nil
}
