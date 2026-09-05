package mailbox

import (
	"context"
	"fmt"
)

// UserSentinel is the reserved agent_id used to address the human user
// in a session. It short-circuits validation without consulting the
// AgentResolver, so callers may pass a nil resolver when addressing
// the user.
const UserSentinel = "user"

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

// ValidateAgentID checks that an agent_id is one of:
//   - the UserSentinel (always valid, resolver not consulted)
//   - an identifier known to AgentResolver
//
// Returns nil if valid, an error otherwise. The resolver is required
// for everything except the user sentinel; passing nil with any other
// ID is rejected.
func ValidateAgentID(ctx context.Context, r AgentResolver, agentID string) error {
	if agentID == "" {
		return fmt.Errorf("empty agent id")
	}
	if agentID == UserSentinel {
		return nil
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
