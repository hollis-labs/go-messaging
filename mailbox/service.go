package mailbox

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// NotificationSink receives a message-received hook on every
// successful SendMessage. Nil-safe — Service skips the sink call when
// this is unset.
type NotificationSink interface {
	NotifyReceived(ctx context.Context, msg *Message)
}

// WakeReactor receives a live-wake hook on every successful SendMessage. The
// shared package invokes the configured reactor for every successful send;
// implementations decide which message kinds should start work.
//
// Hosts implement this interface at their runtime boundary. Wired via
// SetWakeReactor; nil is permitted.
type WakeReactor interface {
	ReactToMessage(ctx context.Context, msg *Message)
}

// Service is the authorization and fan-out wrapper around Store. Transport
// callers go through Service so validation, authorization checks, persistence,
// and subscriber fan-out share one boundary. Its required collaborators are:
//
//   - store: the mailbox Store for message CRUD.
//   - resolver: looks up agents by ID so SendMessage and Subscribe can reject
//     unknown addresses.
//
// Service is safe for concurrent use, including hook reconfiguration and
// subscription shutdown, when its collaborators are safe for concurrent use.
type Service struct {
	store     Store
	resolver  AgentResolver
	registrar AgentRegistrar // nil = auto-register disabled
	pub       *pubsub

	hooksMu sync.RWMutex
	hooks   serviceHooks
}

type serviceHooks struct {
	sink     NotificationSink
	wake     WakeReactor
	runner   AsyncRunner
	events   EventStore
	handoffs HandoffCoordinator
}

// NewService constructs a Service. The resolver validates every addressable
// agent ID. The registrar optionally enables host-owned registration on first
// send; pass nil to disable.
func NewService(s Store, r AgentResolver, reg AgentRegistrar) *Service {
	return &Service{
		store:     s,
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
	svc.hooksMu.Lock()
	svc.hooks.sink = s
	svc.hooksMu.Unlock()
}

// SetWakeReactor wires (or unwires) the live-wake hook. It is separate from
// NewService so hosts can resolve construction-order dependencies.
func (svc *Service) SetWakeReactor(r WakeReactor) {
	svc.hooksMu.Lock()
	svc.hooks.wake = r
	svc.hooksMu.Unlock()
}

// SetAsyncRunner wires (or unwires) the host-owned asynchronous runner used
// for wake reactions.
func (svc *Service) SetAsyncRunner(r AsyncRunner) {
	svc.hooksMu.Lock()
	svc.hooks.runner = r
	svc.hooksMu.Unlock()
}

// SetEventStore wires (or unwires) host persistence for mailbox events.
func (svc *Service) SetEventStore(events EventStore) {
	svc.hooksMu.Lock()
	svc.hooks.events = events
	svc.hooksMu.Unlock()
}

// SetHandoffCoordinator wires (or unwires) the host-owned handoff workflow.
func (svc *Service) SetHandoffCoordinator(coordinator HandoffCoordinator) {
	svc.hooksMu.Lock()
	svc.hooks.handoffs = coordinator
	svc.hooksMu.Unlock()
}

func (svc *Service) snapshotHooks() serviceHooks {
	svc.hooksMu.RLock()
	hooks := svc.hooks
	svc.hooksMu.RUnlock()
	return hooks
}

// SendMessage validates both ends of the address tuple, persists the
// message via the Store, and fans the row out to any live subscribers
// on the recipient's (session, agent) key. The returned *Message is owned by
// the caller and does not alias subscriber or asynchronous-hook deliveries.
//
// Auto-registration: if the caller's FromAgentID does not resolve and the
// Service has a registrar wired in, the opaque RegisterAs hint is delegated
// to the host. Without a registrar, unknown IDs reject through
// ValidateAgentID.
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
	hooks := svc.snapshotHooks()
	if hooks.sink != nil {
		// Best-effort notification; don't fail the send if the sink
		// cannot deliver.
		hooks.sink.NotifyReceived(ctx, cloneMessage(out))
	}
	writeSendEvents(ctx, hooks.events, out)

	// A wake reactor is a host-owned policy seam. Invoke it for every message;
	// filtering message kinds in this package would bake one host's runtime
	// topology into a shared mailbox primitive. Fire-and-forget with a
	// background/runner-owned context so it does not inherit a canceled send
	// request.
	//
	// The reactor receives a deep copy because out belongs to the caller and
	// Message includes pointer fields. Sharing either level would allow caller
	// mutation to race the asynchronous hook.
	//
	// Spawn goes through the host runner when wired and falls back to an
	// untracked, panic-safe goroutine otherwise.
	if hooks.wake != nil {
		msgCopy := cloneMessage(out)
		if hooks.runner != nil {
			hooks.runner.Go("wake-reactor", func(ctx context.Context) {
				hooks.wake.ReactToMessage(ctx, msgCopy)
			})
		} else {
			goSafe(context.Background(), "mailbox.wake-reactor", func(ctx context.Context) {
				hooks.wake.ReactToMessage(ctx, msgCopy)
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
	writeMessageEvent(ctx, svc.snapshotHooks().events, sessionID, EventMessageAcked, msg)
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
	writeMessageEvent(ctx, svc.snapshotHooks().events, sessionID, EventMessageResolved, msg)
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

// maybeAutoRegister delegates an unknown fromAgentID to the optional host
// registrar. Record shape and registration policy remain entirely host-owned.
// Returns (registered=true, nil) when registration succeeded,
// (registered=false, nil) when the id already resolves or auto-
// register is disabled, and (_, err) when the insert fails for a
// reason other than a lost race.
func (svc *Service) maybeAutoRegister(ctx context.Context, fromAgentID, registerAs string) (bool, error) {
	if svc.registrar == nil || fromAgentID == "" {
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

// Close drains all pubsub subscriber channels, clears the subscriber map, and
// waits for subscription janitors to exit. After Close, publish becomes a
// no-op; existing subscribers see their receive channels close, unblocking any
// pending receive. Safe to call multiple times and concurrently.
//
// Hosts should call Close during graceful shutdown to unblock subscribers.
func (svc *Service) Close() error {
	if svc.pub != nil {
		svc.pub.closeAll()
	}
	return nil
}
