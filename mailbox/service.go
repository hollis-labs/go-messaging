package mailbox

// Service is the authorization + fan-out wrapper around Store. CLI,
// HTTP, and MCP callers go through Service rather than touching Store
// directly so that validation, auth checks, and subscriber fan-out
// all live in one place.
import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
)

// Service coordinates messaging validation, persistence, subscriber
// fan-out, and handoff-state mutations. It holds three collaborators:
//
//   - store: the mailbox Store for message CRUD.
//   - db:    the underlying *sql.DB for cross-table handoff txns that
//     reach into session_handoffs / session_agents — those
//     tables live outside the messaging Store interface.
//   - resolver: looks up agents by ID so SendMessage and Subscribe
//     can reject unknown addresses.
//
// Service is safe for concurrent use since Store, the *sql.DB, and
// the pubsub are each safe for concurrent use.
// NotificationSink receives a message-received hook on every
// successful SendMessage. Implementations bridge messaging events
// into the session SSE stream so subscribed UI clients see an
// incoming message chip / notification. Nil-safe — Service skips the
// sink call when this is unset (e.g. in tests).
type NotificationSink interface {
	NotifyReceived(ctx context.Context, msg *Message)
}

// WakeReactor receives a live-wake hook on every successful SendMessage
// whose Kind is eligible (see the SendMessage call site for the
// KindSubagentResult exclusion). Implementations resolve the recipient
// session's wake policy and may start a new turn rather than relying on
// inbox polling.
//
// Hosts implement this interface at their runtime boundary. Wired via
// SetWakeReactor; nil is permitted.
type WakeReactor interface {
	ReactToMessage(ctx context.Context, msg *Message)
}

type Service struct {
	store     Store
	db        *sql.DB
	resolver  AgentResolver
	registrar AgentRegistrar   // nil = auto-register disabled
	sink      NotificationSink // nil = no SSE push
	wake      WakeReactor      // nil = no live-wake side effect
	runner    AsyncRunner
	pub       *pubsub
}

// NewService constructs a Service. The Store is used for message CRUD
// and should wrap the same underlying DB as the *sql.DB so that
// handoff transactions and message reads see consistent state. The
// resolver is used by ValidateAgentID when send/subscribe arrives
// with an unknown agent ID. The registrar optionally enables
// auto-register-on-first-send; pass nil to disable.
func NewService(s Store, db *sql.DB, r AgentResolver, reg AgentRegistrar) *Service {
	return &Service{
		store:     s,
		db:        db,
		resolver:  r,
		registrar: reg,
		pub:       newPubsub(),
	}
}

// SetNotificationSink wires (or unwires) the message-received hook.
// Separate from NewService so the host can wire the sink
// after StreamManager construction without threading it through
// every caller that doesn't care.
func (svc *Service) SetNotificationSink(s NotificationSink) {
	svc.sink = s
}

// SetWakeReactor wires (or unwires) the live-wake hook. It is separate from
// NewService so hosts can resolve construction-order dependencies.
func (svc *Service) SetWakeReactor(r WakeReactor) {
	svc.wake = r
}

// SetAsyncRunner wires (or unwires) the host-owned asynchronous runner used
// for wake reactions.
func (svc *Service) SetAsyncRunner(r AsyncRunner) {
	svc.runner = r
}

// SendMessage validates both ends of the address tuple, persists the
// message via the Store, and fans the row out to any live subscribers
// on the recipient's (session, agent) key.
//
// Auto-registration: if the caller's FromAgentID is not the user
// sentinel and doesn't resolve AND the Service has a registrar wired
// in, a minimal agent_profiles row is inserted with kind='external'
// (default) or 'cli' (when input.RegisterAs == "cli"). The send then
// proceeds with the freshly-registered ID. Without a registrar the
// unknown id still rejects through ValidateAgentID below.
func (svc *Service) SendMessage(ctx context.Context, input SendInput) (*Message, error) {
	registered, err := svc.maybeAutoRegister(ctx, input.FromAgentID, input.RegisterAs)
	if err != nil {
		return nil, fmt.Errorf("%w: from_agent_id: %w", ErrValidation, err)
	}
	// Skip the from-side ValidateAgentID call when we just auto-
	// registered the id: the resolver may cache negative lookups or
	// (as in tests) be a fixed known-set that doesn't see DB writes.
	// We know the row exists because we just wrote it.
	if !registered {
		if err := ValidateAgentID(ctx, svc.resolver, input.FromAgentID); err != nil {
			return nil, fmt.Errorf("%w: from_agent_id: %w", ErrValidation, err)
		}
	}
	if err := ValidateAgentID(ctx, svc.resolver, input.ToAgentID); err != nil {
		return nil, fmt.Errorf("%w: to_agent_id: %w", ErrValidation, err)
	}
	if input.FromSessionID == "" {
		return nil, fmt.Errorf("%w: from_session_id required", ErrValidation)
	}
	if input.ToSessionID == "" {
		return nil, fmt.Errorf("%w: to_session_id required", ErrValidation)
	}
	if input.Body == "" {
		return nil, fmt.Errorf("%w: body required", ErrValidation)
	}

	out, err := svc.store.Send(ctx, input)
	if err != nil {
		return nil, err
	}
	if svc.pub != nil {
		svc.pub.publish(out)
	}
	if svc.sink != nil {
		// Best-effort notification; don't fail the send if the sink
		// can't deliver (e.g. no active SSE for that session yet).
		svc.sink.NotifyReceived(ctx, out)
	}
	// Record send in the session event log for replay.
	svc.writeSendEvents(ctx, out)

	// React to the send by optionally triggering a live
	// turn on the recipient session instead of leaving delivery to a
	// poll. Skipped for Kind=KindSubagentResult — that specific kind
	// already has its own dedicated wake path (subagent.CompletionReactor,
	// invoked directly by a host completion path after its own SendMessage
	// call, with its own busy-check and a completion-specific
	// summarizing prompt fed by the kind=subagent_result turn-start
	// injection). Reacting here too would double-trigger the same
	// completion event through two different synthetic prompts racing
	// registerGenerationIfIdle. Fire-and-forget in its own goroutine
	// completion-reactor goroutine) with a background context so a slow or
	// misbehaving
	// reactor never blocks the SendMessage caller (self-tool call, HTTP
	// handler, or background-job poster) and outlives a canceled request
	// ctx.
	//
	// msgCopy takes a shallow copy of *out before handing it to the
	// goroutine: out is also returned to SendMessage's own caller, so
	// without the copy the goroutine and the caller would share the same
	// *Message pointer — a data race if the caller mutates/reuses it
	// after SendMessage returns. A shallow copy is sufficient: the two
	// *string fields (ReadAt/ResolvedAt) are set by separate post-send
	// code paths (Ack/Resolve), not something the original caller races
	// on here.
	//
	// Spawn goes through the host runner when wired and falls back to an
	// untracked, panic-safe goroutine otherwise.
	if svc.wake != nil && out.Kind != KindSubagentResult {
		msgCopy := *out
		if svc.runner != nil {
			svc.runner.Go("wake-reactor", func(ctx context.Context) {
				svc.wake.ReactToMessage(ctx, &msgCopy)
			})
		} else {
			goSafe(context.Background(), "mailbox.wake-reactor", func(ctx context.Context) {
				svc.wake.ReactToMessage(ctx, &msgCopy)
			})
		}
	}

	return out, nil
}

// Inbox returns messages addressed to (sessionID, agentID). The
// caller's (callerSessionID, callerAgentID) is required and must
// match the inbox owner — a caller may only read its own inbox.
// Returns an error wrapping ErrForbidden on mismatch.
//
// This defensive check ensures misaddressed calls fail loudly rather than
// returning another recipient's inbox.
func (svc *Service) Inbox(ctx context.Context, sessionID, agentID string, filter InboxFilter, callerSessionID, callerAgentID string) ([]Message, error) {
	if callerSessionID != sessionID || callerAgentID != agentID {
		return nil, fmt.Errorf("%w: caller does not match inbox owner", ErrForbidden)
	}
	return svc.store.Inbox(ctx, sessionID, agentID, filter)
}

// Thread returns all messages in a thread, chronologically, filtered
// to only those where the caller is either the sender or the
// recipient. Non-participants see an empty slice — the service does
// not leak "thread exists but you cannot see it" signal.
func (svc *Service) Thread(ctx context.Context, threadID, callerSessionID, callerAgentID string) ([]Message, error) {
	rows, err := svc.store.Thread(ctx, threadID)
	if err != nil {
		return nil, err
	}
	out := make([]Message, 0, len(rows))
	for _, m := range rows {
		if (m.FromSessionID == callerSessionID && m.FromAgentID == callerAgentID) ||
			(m.ToSessionID == callerSessionID && m.ToAgentID == callerAgentID) {
			out = append(out, m)
		}
	}
	return out, nil
}

// Ack marks a message as read. The caller must be the message's
// intended recipient: (sessionID, agentID) is checked against the
// persisted (ToSessionID, ToAgentID) and mismatches return an error
// wrapping ErrForbidden.
func (svc *Service) Ack(ctx context.Context, sessionID, agentID, msgID string) error {
	if err := ValidateAgentID(ctx, svc.resolver, agentID); err != nil {
		return fmt.Errorf("%w: agent_id: %w", ErrValidation, err)
	}
	msg, err := svc.store.Get(ctx, msgID)
	if err != nil {
		return err
	}
	if msg.ToSessionID != sessionID || msg.ToAgentID != agentID {
		return fmt.Errorf("%w: caller is not message recipient", ErrForbidden)
	}
	if err := svc.store.Ack(ctx, msgID); err != nil {
		return err
	}
	svc.writeMessageEvent(ctx, sessionID, EventMessageAcked, msg)
	return nil
}

// Resolve marks a message as resolved. Same recipient-ownership check
// as Ack — mismatches return an error wrapping ErrForbidden.
func (svc *Service) Resolve(ctx context.Context, sessionID, agentID, msgID string) error {
	if err := ValidateAgentID(ctx, svc.resolver, agentID); err != nil {
		return fmt.Errorf("%w: agent_id: %w", ErrValidation, err)
	}
	msg, err := svc.store.Get(ctx, msgID)
	if err != nil {
		return err
	}
	if msg.ToSessionID != sessionID || msg.ToAgentID != agentID {
		return fmt.Errorf("%w: caller is not message recipient", ErrForbidden)
	}
	if err := svc.store.Resolve(ctx, msgID); err != nil {
		return err
	}
	svc.writeMessageEvent(ctx, sessionID, EventMessageResolved, msg)
	return nil
}

// RecentForSession returns the last N messages touching a session
// (as sender or receiver), regardless of agent. Powers handoff
// catch-up, where a newly spawned agent needs a summary of what
// happened in the session before it took over. Default and max
// bounds are enforced by Store.Recent.
func (svc *Service) RecentForSession(ctx context.Context, sessionID string, limit int) ([]Message, error) {
	return svc.store.Recent(ctx, sessionID, limit)
}

// UnreadCount returns the count of unread messages for (sessionID,
// agentID). When a CallerIdentity is carried on ctx, the caller tuple must match
// the inbox owner; mismatches return ErrForbidden to pair with the
// Inbox / Thread / Ack / Resolve caller-match pattern. When no
// identity is plumbed, the check falls open for compatibility with
// trusted in-process callers.
func (svc *Service) UnreadCount(ctx context.Context, sessionID, agentID string) (int, error) {
	if caller, ok := CallerFromCtx(ctx); ok {
		if caller.SessionID != sessionID || caller.AgentID != agentID {
			return 0, fmt.Errorf("%w: caller does not match inbox owner", ErrForbidden)
		}
	}
	return svc.store.UnreadCount(ctx, sessionID, agentID)
}

// maybeAutoRegister inserts a minimal agent_profiles row for an
// unknown fromAgentID when the Service has a registrar wired. Record shape
// and registration policy remain entirely host-owned.
// Returns (registered=true, nil) when registration succeeded,
// (registered=false, nil) when the id already resolves or auto-
// register is disabled, and (_, err) when the insert fails for a
// reason other than a lost race.
func (svc *Service) maybeAutoRegister(ctx context.Context, fromAgentID, registerAs string) (bool, error) {
	if svc.registrar == nil || fromAgentID == "" || fromAgentID == UserSentinel {
		return false, nil
	}
	if svc.resolver != nil {
		switch exists, err := svc.resolver.AgentExists(ctx, fromAgentID); {
		case err != nil:
			return false, fmt.Errorf("resolver: %w", err)
		case exists:
			return false, nil // already registered
		default:
			// Genuinely not found; fall through to register.
		}
	}
	if err := svc.registrar.RegisterAgent(ctx, fromAgentID, registerAs); err != nil {
		// Race: another request may have registered the same id
		// concurrently (SQLite UNIQUE constraint). Treat as
		// success if the resolver now sees the row.
		if svc.resolver != nil {
			if exists, gerr := svc.resolver.AgentExists(ctx, fromAgentID); gerr == nil && exists {
				slog.Info("messaging: auto-register lost race, proceeding",
					"agent_id", fromAgentID, "register_as", registerAs, "err", err)
				return true, nil
			}
		}
		return false, fmt.Errorf("auto-register: %w", err)
	}
	slog.Info("messaging: auto-registered agent on first message_send",
		"agent_id", fromAgentID, "register_as", registerAs)
	return true, nil
}

// Close drains all pubsub subscriber channels and clears the
// subscriber map. After Close, publish becomes a no-op (no
// subscribers); existing subscribers see their receive channels
// close, unblocking any pending receive. Safe to call multiple times.
//
// Hosts should call Close during graceful shutdown to unblock subscribers.
func (svc *Service) Close() error {
	if svc.pub != nil {
		svc.pub.closeAll()
	}
	return nil
}
