package mailbox

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

// newTestMessagingStore spins up a fresh file-backed SQLite store in a
// temp dir, runs all embedded migrations, and returns a SQLiteStore
// wrapping its DB plus a helper for tests that need to seed rows directly.
// A file-backed DB (rather than :memory:) avoids
// any surprise around shared-cache semantics on modernc.org/sqlite.
func newTestMessagingStore(t *testing.T) (*SQLiteStore, *testDB) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), testSchema); err != nil {
		_ = db.Close()
		t.Fatalf("create test schema: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	parent := &testDB{DB: db}
	return NewSQLiteStore(db), parent
}

// newTestService wires up a Service backed by a fresh messaging store
// and a fakeResolver that knows the supplied agent IDs. Returns the
// Service plus the SQLiteStore (for message seeding) plus the parent
// test database helper (for session/session-agent seeding in handoff tests).
//
// Passes nil as the registrar so tests that expect
// unknown from_agent_id to reject through ValidateAgentID keep that
// behavior. Registration tests construct their own Service with a
// non-nil registrar.
func newTestService(t *testing.T, knownAgents ...string) (*Service, *SQLiteStore, *testDB) {
	t.Helper()
	ms, parent := newTestMessagingStore(t)
	r := newFakeResolver(knownAgents...)
	return NewService(ms, parent.DB, r, nil), ms, parent
}

// baseInput builds a minimal valid SendInput for tests that only care
// about the validation or routing behavior.
func baseInput(from, to string) SendInput {
	return SendInput{
		FromSessionID: "sess-1",
		FromAgentID:   from,
		ToSessionID:   "sess-1",
		ToAgentID:     to,
		Body:          "hello",
	}
}

// --- Auto-register on first message_send ---

// TestService_SendMessage_AutoRegistersUnknownFrom covers the happy
// path: an unknown from_agent_id with a wired registrar gets inserted
// as kind='external' and the send proceeds.
func TestService_SendMessage_AutoRegistersUnknownFrom(t *testing.T) {
	ms, parent := newTestMessagingStore(t)
	r := newFakeResolver("file-backend")
	svc := NewService(ms, parent.DB, r, r)

	in := SendInput{
		FromSessionID: "sess-1",
		FromAgentID:   "new-agent",
		ToSessionID:   "sess-1",
		ToAgentID:     "file-backend",
		Body:          "hello",
	}
	out, err := svc.SendMessage(context.Background(), in)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if out.FromAgentID != "new-agent" {
		t.Errorf("FromAgentID = %q, want %q", out.FromAgentID, "new-agent")
	}
	if exists, err := r.AgentExists(context.Background(), "new-agent"); err != nil || !exists {
		t.Fatalf("registered sender not visible: exists=%v err=%v", exists, err)
	}
	if got := r.registrationHint("new-agent"); got != "" {
		t.Errorf("registration hint = %q, want empty", got)
	}
}

// TestService_SendMessage_AutoRegisterAsCLI covers the case where the caller
// flags register_as=cli: the inserted profile's kind is 'cli'.
func TestService_SendMessage_AutoRegisterAsCLI(t *testing.T) {
	ms, parent := newTestMessagingStore(t)
	r := newFakeResolver("file-backend")
	svc := NewService(ms, parent.DB, r, r)

	in := SendInput{
		FromSessionID: "sess-1",
		FromAgentID:   "cli-host-12345",
		ToSessionID:   "sess-1",
		ToAgentID:     "file-backend",
		Body:          "hello",
		RegisterAs:    "cli",
	}
	if _, err := svc.SendMessage(context.Background(), in); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if got := r.registrationHint("cli-host-12345"); got != "cli" {
		t.Errorf("registration hint = %q, want %q", got, "cli")
	}
}

// TestService_SendMessage_NoRegistrarStillRejects covers the disabled
// path: if Service has no registrar, unknown from_agent_id still
// errors through ValidateAgentID.
func TestService_SendMessage_NoRegistrarStillRejects(t *testing.T) {
	ms, parent := newTestMessagingStore(t)
	r := newFakeResolver("file-backend")
	svc := NewService(ms, parent.DB, r, nil)

	_, err := svc.SendMessage(context.Background(), SendInput{
		FromSessionID: "sess-1",
		FromAgentID:   "ghost-agent",
		ToSessionID:   "sess-1",
		ToAgentID:     "file-backend",
		Body:          "hi",
	})
	if err == nil {
		t.Fatal("expected rejection for unknown from_agent_id without registrar, got nil")
	}
	if !errors.Is(err, ErrValidation) {
		t.Errorf("got err=%v, want errors.Is(_, ErrValidation)", err)
	}
}

// TestService_SendMessage_AutoRegisterSkipsUserSentinel covers the
// carve-out: the user sentinel never produces a profile row even with
// a registrar wired in.
func TestService_SendMessage_AutoRegisterSkipsUserSentinel(t *testing.T) {
	ms, parent := newTestMessagingStore(t)
	r := newFakeResolver("file-backend")
	svc := NewService(ms, parent.DB, r, r)

	if _, err := svc.SendMessage(context.Background(), SendInput{
		FromSessionID: "sess-1",
		FromAgentID:   UserSentinel,
		ToSessionID:   "sess-1",
		ToAgentID:     "file-backend",
		Body:          "hi",
	}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if exists, err := r.AgentExists(context.Background(), UserSentinel); err != nil || exists {
		t.Error("user sentinel should not produce an agent_profiles row")
	}
}

func TestService_SendMessage_RejectsUnknownTo(t *testing.T) {
	svc, _, _ := newTestService(t, "file-backend")
	msg := baseInput("file-backend", "file-nonexistent")

	_, err := svc.SendMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error for unknown to_agent_id, got nil")
	}
	if !strings.Contains(err.Error(), "to_agent_id") {
		t.Errorf("expected error to mention to_agent_id, got: %v", err)
	}
}

func TestService_SendMessage_RejectsUnknownFrom(t *testing.T) {
	svc, _, _ := newTestService(t, "file-backend")
	msg := baseInput("file-nonexistent", "file-backend")

	_, err := svc.SendMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error for unknown from_agent_id, got nil")
	}
	if !strings.Contains(err.Error(), "from_agent_id") {
		t.Errorf("expected error to mention from_agent_id, got: %v", err)
	}
}

func TestService_SendMessage_AcceptsUserSentinel(t *testing.T) {
	// Resolver knows file-backend. The user sentinel short-circuits validation,
	// so it must work on either side of the send.
	svc, _, _ := newTestService(t, "file-backend")

	// user -> file-backend
	msg1 := baseInput(UserSentinel, "file-backend")
	out, err := svc.SendMessage(context.Background(), msg1)
	if err != nil {
		t.Fatalf("user -> file-backend: unexpected error: %v", err)
	}
	if out == nil || out.ID == "" {
		t.Fatal("expected persisted message with non-empty ID")
	}
	if out.FromAgentID != UserSentinel || out.ToAgentID != "file-backend" {
		t.Errorf("addressing not preserved: from=%q to=%q", out.FromAgentID, out.ToAgentID)
	}

	// file-backend -> user
	msg2 := baseInput("file-backend", UserSentinel)
	out2, err := svc.SendMessage(context.Background(), msg2)
	if err != nil {
		t.Fatalf("file-backend -> user: unexpected error: %v", err)
	}
	if out2 == nil || out2.ID == "" {
		t.Fatal("expected persisted message with non-empty ID")
	}
}

func TestService_SendMessage_RequiresFields(t *testing.T) {
	svc, _, _ := newTestService(t, "file-backend")

	// SendInput is a value type — no nil case; an empty input trips
	// the field validation below instead.

	// missing FromSessionID
	m := baseInput("file-backend", UserSentinel)
	m.FromSessionID = ""
	if _, err := svc.SendMessage(context.Background(), m); err == nil || !strings.Contains(err.Error(), "from_session_id") {
		t.Errorf("expected from_session_id error, got: %v", err)
	}

	// missing ToSessionID
	m = baseInput("file-backend", UserSentinel)
	m.ToSessionID = ""
	if _, err := svc.SendMessage(context.Background(), m); err == nil || !strings.Contains(err.Error(), "to_session_id") {
		t.Errorf("expected to_session_id error, got: %v", err)
	}

	// missing Body
	m = baseInput("file-backend", UserSentinel)
	m.Body = ""
	if _, err := svc.SendMessage(context.Background(), m); err == nil || !strings.Contains(err.Error(), "body") {
		t.Errorf("expected body error, got: %v", err)
	}
}

func TestService_Inbox(t *testing.T) {
	svc, s, _ := newTestService(t, "file-backend")

	// Seed 2 messages directly via store (bypassing validation) so we can
	// test the read path in isolation.
	for i := 0; i < 2; i++ {
		if _, err := s.Send(context.Background(), SendInput{
			FromSessionID: "sess-other",
			FromAgentID:   UserSentinel,
			ToSessionID:   "sess-1",
			ToAgentID:     "file-backend",
			Body:          "msg",
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	msgs, err := svc.Inbox(context.Background(), "sess-1", "file-backend", InboxFilter{}, "sess-1", "file-backend")
	if err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	if len(msgs) != 2 {
		t.Errorf("expected 2 messages, got %d", len(msgs))
	}
}

func TestService_Thread(t *testing.T) {
	svc, s, _ := newTestService(t, "file-backend")

	// Seed 3 messages sharing a thread_id. The store's Thread query
	// tiebreaks on rowid ASC so same-tick inserts remain deterministic.
	threadID := "thread-xyz"
	for i := 0; i < 3; i++ {
		if _, err := s.Send(context.Background(), SendInput{
			FromSessionID: "sess-1",
			FromAgentID:   UserSentinel,
			ToSessionID:   "sess-1",
			ToAgentID:     "file-backend",
			ThreadID:      threadID,
			Body:          "msg",
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	msgs, err := svc.Thread(context.Background(), threadID, "sess-1", "file-backend")
	if err != nil {
		t.Fatalf("Thread: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	// Chronological order: created_at ASC.
	for i := 1; i < len(msgs); i++ {
		if msgs[i].CreatedAt < msgs[i-1].CreatedAt {
			t.Errorf("thread not chronological: %q before %q", msgs[i-1].CreatedAt, msgs[i].CreatedAt)
		}
	}
}

func TestService_Ack(t *testing.T) {
	svc, s, _ := newTestService(t, "file-backend")
	seeded, err := s.Send(context.Background(), SendInput{
		FromSessionID: "sess-1",
		FromAgentID:   UserSentinel,
		ToSessionID:   "sess-1",
		ToAgentID:     "file-backend",
		Body:          "hi",
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := svc.Ack(context.Background(), "sess-1", "file-backend", seeded.ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	got, err := s.Get(context.Background(), seeded.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != "read" {
		t.Errorf("expected status=read, got %q", got.Status)
	}
}

func TestService_Ack_RejectsUnknownAgent(t *testing.T) {
	svc, s, _ := newTestService(t, "file-backend")
	seeded, err := s.Send(context.Background(), SendInput{
		FromSessionID: "sess-1",
		FromAgentID:   UserSentinel,
		ToSessionID:   "sess-1",
		ToAgentID:     "file-backend",
		Body:          "hi",
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := svc.Ack(context.Background(), "sess-1", "file-ghost", seeded.ID); err == nil {
		t.Error("expected rejection for unknown caller agent")
	}
}

func TestService_Resolve(t *testing.T) {
	svc, s, _ := newTestService(t, "file-backend")
	seeded, err := s.Send(context.Background(), SendInput{
		FromSessionID: "sess-1",
		FromAgentID:   UserSentinel,
		ToSessionID:   "sess-1",
		ToAgentID:     "file-backend",
		Body:          "hi",
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := svc.Resolve(context.Background(), "sess-1", "file-backend", seeded.ID); err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	got, err := s.Get(context.Background(), seeded.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != "resolved" {
		t.Errorf("expected status=resolved, got %q", got.Status)
	}
}

func TestService_RecentForSession(t *testing.T) {
	svc, s, _ := newTestService(t, "file-backend")

	// Seed 4 messages in sess-1. Use the user sentinel on both sides so we
	// don't rely on store agent rows existing.
	for i := 0; i < 4; i++ {
		if _, err := s.Send(context.Background(), SendInput{
			FromSessionID: "sess-1",
			FromAgentID:   UserSentinel,
			ToSessionID:   "sess-1",
			ToAgentID:     "file-backend",
			Body:          "msg",
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	msgs, err := svc.RecentForSession(context.Background(), "sess-1", 2)
	if err != nil {
		t.Fatalf("RecentForSession: %v", err)
	}
	if len(msgs) != 2 {
		t.Errorf("expected 2 messages, got %d", len(msgs))
	}
	// Store returns latest N reversed to chronological order; check it's
	// sorted ascending by created_at (ties broken deterministically by the
	// store's rowid tiebreaker, which still yields ascending created_at).
	for i := 1; i < len(msgs); i++ {
		if msgs[i].CreatedAt < msgs[i-1].CreatedAt {
			t.Errorf("recent not chronological: %q before %q", msgs[i-1].CreatedAt, msgs[i].CreatedAt)
		}
	}
}

// --- F01 Critical: Ack/Resolve ownership check ---

// TestService_Ack_ForbidsNonRecipient covers F01: only the addressed
// recipient can ack a message. A caller with a valid agent ID but
// mismatched (session, agent) vs. msg.To* must get ErrForbidden.
func TestService_Ack_ForbidsNonRecipient(t *testing.T) {
	// Resolver knows both agents so validation passes; ownership check
	// is what must reject.
	svc, s, _ := newTestService(t, "file-backend", "file-frontend")
	seeded, err := s.Send(context.Background(), SendInput{
		FromSessionID: "sess-1",
		FromAgentID:   UserSentinel,
		ToSessionID:   "sess-1",
		ToAgentID:     "file-backend",
		Body:          "hi",
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	cases := []struct {
		name      string
		session   string
		agent     string
		wantErrIs error
	}{
		{"recipient matches", "sess-1", "file-backend", nil},
		{"different agent same session", "sess-1", "file-frontend", ErrForbidden},
		{"different session same agent", "sess-other", "file-backend", ErrForbidden},
		{"different session and agent", "sess-other", "file-frontend", ErrForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Re-seed so we get a fresh unread msg each subtest.
			fresh, err := s.Send(context.Background(), SendInput{
				FromSessionID: "sess-1",
				FromAgentID:   UserSentinel,
				ToSessionID:   "sess-1",
				ToAgentID:     "file-backend",
				Body:          "hi",
			})
			if err != nil {
				t.Fatalf("seed: %v", err)
			}
			err = svc.Ack(context.Background(), tc.session, tc.agent, fresh.ID)
			if tc.wantErrIs == nil && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErrIs != nil && !errors.Is(err, tc.wantErrIs) {
				t.Fatalf("got err=%v, want errors.Is(_, %v)", err, tc.wantErrIs)
			}
		})
	}
	_ = seeded
}

// TestService_Resolve_ForbidsNonRecipient covers F01 on Resolve — same
// shape as Ack.
func TestService_Resolve_ForbidsNonRecipient(t *testing.T) {
	svc, s, _ := newTestService(t, "file-backend", "file-frontend")

	cases := []struct {
		name      string
		session   string
		agent     string
		wantErrIs error
	}{
		{"recipient matches", "sess-1", "file-backend", nil},
		{"wrong agent", "sess-1", "file-frontend", ErrForbidden},
		{"wrong session", "sess-other", "file-backend", ErrForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fresh, err := s.Send(context.Background(), SendInput{
				FromSessionID: "sess-1",
				FromAgentID:   UserSentinel,
				ToSessionID:   "sess-1",
				ToAgentID:     "file-backend",
				Body:          "hi",
			})
			if err != nil {
				t.Fatalf("seed: %v", err)
			}
			err = svc.Resolve(context.Background(), tc.session, tc.agent, fresh.ID)
			if tc.wantErrIs == nil && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErrIs != nil && !errors.Is(err, tc.wantErrIs) {
				t.Fatalf("got err=%v, want errors.Is(_, %v)", err, tc.wantErrIs)
			}
		})
	}
}

// --- F02 High: Thread access filtered by participant ---

// TestService_Thread_FiltersByParticipant covers F02. Seed a thread with
// messages between backend and frontend; assert that each agent sees
// only messages they are a participant of, and an unrelated agent sees
// an empty slice (no existence leak).
func TestService_Thread_FiltersByParticipant(t *testing.T) {
	svc, s, _ := newTestService(t, "file-backend", "file-frontend", "file-stranger")

	threadID := "thread-participants"
	// Two messages: backend→frontend, frontend→backend.
	for _, p := range []struct{ from, to string }{
		{"file-backend", "file-frontend"},
		{"file-frontend", "file-backend"},
	} {
		if _, err := s.Send(context.Background(), SendInput{
			FromSessionID: "sess-1", FromAgentID: p.from,
			ToSessionID: "sess-1", ToAgentID: p.to,
			ThreadID: threadID, Body: "msg",
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	// A third message completely unrelated to backend/frontend — stranger
	// as both sender and recipient in a different session — sharing the
	// threadID. Neither backend nor frontend should see it.
	if _, err := s.Send(context.Background(), SendInput{
		FromSessionID: "sess-2", FromAgentID: "file-stranger",
		ToSessionID: "sess-2", ToAgentID: "file-stranger",
		ThreadID: threadID, Body: "stranger-only",
	}); err != nil {
		t.Fatalf("seed stranger: %v", err)
	}

	cases := []struct {
		name    string
		session string
		agent   string
		wantLen int
	}{
		{"backend sees 2 messages", "sess-1", "file-backend", 2},
		{"frontend sees 2 messages", "sess-1", "file-frontend", 2},
		{"stranger sees only their own message", "sess-2", "file-stranger", 1},
		{"non-participant sees empty", "sess-1", "file-unknown", 0},
		{"participant in wrong session sees empty", "sess-999", "file-backend", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msgs, err := svc.Thread(context.Background(), threadID, tc.session, tc.agent)
			if err != nil {
				t.Fatalf("Thread: %v", err)
			}
			if len(msgs) != tc.wantLen {
				t.Errorf("len=%d, want %d", len(msgs), tc.wantLen)
			}
		})
	}
}

// --- F03 High: Inbox caller-match ---

// TestService_Inbox_RequiresCallerMatch covers F03. Any mismatch between
// (sessionID, agentID) target and (callerSessionID, callerAgentID)
// returns ErrForbidden without touching the store.
func TestService_Inbox_RequiresCallerMatch(t *testing.T) {
	svc, _, _ := newTestService(t, "file-backend")

	cases := []struct {
		name          string
		target        [2]string
		caller        [2]string
		wantForbidden bool
	}{
		{"caller matches target", [2]string{"sess-1", "file-backend"}, [2]string{"sess-1", "file-backend"}, false},
		{"different agent", [2]string{"sess-1", "file-backend"}, [2]string{"sess-1", "file-other"}, true},
		{"different session", [2]string{"sess-1", "file-backend"}, [2]string{"sess-2", "file-backend"}, true},
		{"empty caller", [2]string{"sess-1", "file-backend"}, [2]string{"", ""}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.Inbox(
				context.Background(),
				tc.target[0], tc.target[1],
				InboxFilter{},
				tc.caller[0], tc.caller[1],
			)
			if tc.wantForbidden {
				if !errors.Is(err, ErrForbidden) {
					t.Errorf("got err=%v, want errors.Is(_, ErrForbidden)", err)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// --- F05 Medium: RecentForSession limit cap + aligned defaults ---

// TestService_RecentForSession_LimitCap covers F05. A caller asking for
// 100000 rows gets clamped to MaxRecentLimit; a caller asking for a
// non-positive limit gets the default 20 (service and store agree).
func TestService_RecentForSession_LimitCap(t *testing.T) {
	svc, s, _ := newTestService(t, "file-backend")

	// Seed 150 messages so both cap (100) and default (20) paths can
	// actually fill.
	for i := 0; i < 150; i++ {
		if _, err := s.Send(context.Background(), SendInput{
			FromSessionID: "sess-1",
			FromAgentID:   UserSentinel,
			ToSessionID:   "sess-1",
			ToAgentID:     "file-backend",
			Body:          "msg",
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	cases := []struct {
		name      string
		limit     int
		wantCount int
	}{
		{"huge limit clamped to MaxRecentLimit", 100000, MaxRecentLimit},
		{"exactly at cap", MaxRecentLimit, MaxRecentLimit},
		{"under cap", 50, 50},
		{"negative uses default 20", -1, 20},
		{"zero uses default 20", 0, 20},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msgs, err := svc.RecentForSession(context.Background(), "sess-1", tc.limit)
			if err != nil {
				t.Fatalf("RecentForSession: %v", err)
			}
			if len(msgs) != tc.wantCount {
				t.Errorf("len=%d, want %d", len(msgs), tc.wantCount)
			}
		})
	}
}

func TestService_RecentForSession_DefaultLimit(t *testing.T) {
	// With limit=0 the Service layer substitutes its default of 20. We
	// verify by seeding more than 20 messages and confirming exactly 20
	// come back — proving the Service default (20), not the store's
	// default (50), took effect.
	svc, s, _ := newTestService(t, "file-backend")

	for i := 0; i < 25; i++ {
		if _, err := s.Send(context.Background(), SendInput{
			FromSessionID: "sess-1",
			FromAgentID:   UserSentinel,
			ToSessionID:   "sess-1",
			ToAgentID:     "file-backend",
			Body:          "msg",
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	msgs, err := svc.RecentForSession(context.Background(), "sess-1", 0)
	if err != nil {
		t.Fatalf("RecentForSession: %v", err)
	}
	if len(msgs) != 20 {
		t.Errorf("expected default limit of 20 messages, got %d", len(msgs))
	}
}

// TestService_UnreadCount_NoCallerFallsOpen confirms that without a
// CallerIdentity, UnreadCount remains a trusted in-process pass-through.
func TestService_UnreadCount_NoCallerFallsOpen(t *testing.T) {
	svc, s, _ := newTestService(t, "file-backend")

	for i := 0; i < 3; i++ {
		if _, err := s.Send(context.Background(), SendInput{
			FromSessionID: "sess-other",
			FromAgentID:   UserSentinel,
			ToSessionID:   "sess-1",
			ToAgentID:     "file-backend",
			Body:          "msg",
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	count, err := svc.UnreadCount(context.Background(), "sess-1", "file-backend")
	if err != nil {
		t.Fatalf("UnreadCount: %v", err)
	}
	if count != 3 {
		t.Errorf("expected unread count 3, got %d", count)
	}
}

// TestService_UnreadCount_CallerMatch confirms that when
// ctx carries a CallerIdentity matching the target tuple, the service
// returns the count normally.
func TestService_UnreadCount_CallerMatch(t *testing.T) {
	svc, s, _ := newTestService(t, "file-backend")

	if _, err := s.Send(context.Background(), SendInput{
		FromSessionID: "sess-other",
		FromAgentID:   UserSentinel,
		ToSessionID:   "sess-1",
		ToAgentID:     "file-backend",
		Body:          "msg",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ctx := WithCaller(context.Background(), CallerIdentity{
		SessionID: "sess-1",
		AgentID:   "file-backend",
	})
	count, err := svc.UnreadCount(ctx, "sess-1", "file-backend")
	if err != nil {
		t.Fatalf("UnreadCount with matching caller: %v", err)
	}
	if count != 1 {
		t.Errorf("expected unread count 1, got %d", count)
	}
}

// TestService_UnreadCount_CallerMismatchForbidden confirms that a
// context caller that does not match the target inbox owner returns
// ErrForbidden.
func TestService_UnreadCount_CallerMismatchForbidden(t *testing.T) {
	svc, _, _ := newTestService(t, "file-backend", "file-frontend")

	ctx := WithCaller(context.Background(), CallerIdentity{
		SessionID: "sess-1",
		AgentID:   "file-frontend", // not the target inbox owner
	})
	_, err := svc.UnreadCount(ctx, "sess-1", "file-backend")
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("expected ErrForbidden, got %v", err)
	}
}
