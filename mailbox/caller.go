package mailbox

// Caller identity formalizes the (session_id, agent_id) tuple already
// threaded through Inbox / Thread / Ack / Resolve. It is carried on
// a request context so service-layer authorization sees an identity not
// derived from the same query/body fields that name the target.
//
// Transports may stamp this identity after authenticating a request and
// retrieve it at service boundaries. The context value is identity
// plumbing, not authentication by itself.
import "context"

// CallerIdentity is the (session_id, agent_id) tuple that identifies
// the caller of a messaging operation. Empty strings mean "not set" —
// callers that want strict identity enforcement must populate both
// fields.
type CallerIdentity struct {
	SessionID string
	AgentID   string
}

// IsZero reports whether the identity is unset (both fields empty).
// Distinct from partial identity (only one field set), which IsComplete
// rejects. Preserved as "both empty" for any caller that cares about the
// strict all-empty state.
func (c CallerIdentity) IsZero() bool {
	return c.SessionID == "" && c.AgentID == ""
}

// IsComplete reports whether both fields are populated. Service-layer
// authorization compares complete (session, agent) pairs.
func (c CallerIdentity) IsComplete() bool {
	return c.SessionID != "" && c.AgentID != ""
}

// callerCtxKey is the private context key used to carry
// CallerIdentity through middleware and handlers. Intentionally
// unexported so that only messaging and the HTTP boundary can
// read/write it.
type callerCtxKey struct{}

// WithCaller returns a new context carrying the given CallerIdentity.
// Only a COMPLETE identity (both SessionID and AgentID populated) is
// stamped; zero or partial identities return the original ctx unchanged
// and fall through to any parent context identity.
func WithCaller(ctx context.Context, id CallerIdentity) context.Context {
	if !id.IsComplete() {
		return ctx
	}
	return context.WithValue(ctx, callerCtxKey{}, id)
}

// CallerFromCtx extracts the CallerIdentity carried on ctx. The
// second return is false when no identity was plumbed — callers can
// distinguish "identity known and incomplete" (never happens because
// WithCaller rejects zero/partial values) from "identity not set, fall
// back to legacy behavior."
func CallerFromCtx(ctx context.Context) (CallerIdentity, bool) {
	if ctx == nil {
		return CallerIdentity{}, false
	}
	v, ok := ctx.Value(callerCtxKey{}).(CallerIdentity)
	if !ok {
		return CallerIdentity{}, false
	}
	return v, true
}
