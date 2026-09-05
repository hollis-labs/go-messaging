package mailbox

import (
	"context"
	"errors"
	"testing"
)

// TestSQLiteStore_Send_Defaults verifies that Send populates defaults
// for missing Type / Metadata / Priority / Status / ThreadID / CreatedAt
// and returns the authoritative persisted row.
func TestSQLiteStore_Send_Defaults(t *testing.T) {
	s, _ := newTestMessagingStore(t)

	out, err := s.Send(context.Background(), SendInput{
		FromSessionID: "sess-1",
		FromAgentID:   "file-a",
		ToSessionID:   "sess-1",
		ToAgentID:     "file-b",
		Body:          "hello",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if out == nil {
		t.Fatal("Send returned nil message")
	}
	if out.ID == "" {
		t.Error("expected non-empty ID")
	}
	if out.Status != StatusUnread {
		t.Errorf("status = %q, want %q", out.Status, StatusUnread)
	}
	if out.Type != TypeMessage {
		t.Errorf("type = %q, want %q", out.Type, TypeMessage)
	}
	if out.Priority != 2 {
		t.Errorf("priority = %d, want 2", out.Priority)
	}
	if out.ThreadID == "" {
		t.Error("expected ThreadID to default to message ID")
	}
	if out.CreatedAt == "" {
		t.Error("expected CreatedAt to be set")
	}
}

// TestSQLiteStore_Inbox_FiltersBySessionAndAgent verifies that Inbox
// scopes by (to_session_id, to_agent_id) and ignores messages addressed
// to other agents or sessions.
func TestSQLiteStore_Inbox_FiltersBySessionAndAgent(t *testing.T) {
	s, _ := newTestMessagingStore(t)
	ctx := context.Background()

	// Two messages for (sess-1, file-a).
	for _, from := range []string{"file-b", "file-c"} {
		if _, err := s.Send(ctx, SendInput{
			FromSessionID: "sess-1", FromAgentID: from,
			ToSessionID: "sess-1", ToAgentID: "file-a",
			Body: "hi",
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	// One message for (sess-1, file-b).
	if _, err := s.Send(ctx, SendInput{
		FromSessionID: "sess-1", FromAgentID: "file-a",
		ToSessionID: "sess-1", ToAgentID: "file-b",
		Body: "hi",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	msgs, err := s.Inbox(ctx, "sess-1", "file-a", InboxFilter{})
	if err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("got %d messages, want 2", len(msgs))
	}
	for _, m := range msgs {
		if m.ToSessionID != "sess-1" || m.ToAgentID != "file-a" {
			t.Errorf("unexpected recipient: session=%q agent=%q", m.ToSessionID, m.ToAgentID)
		}
	}
}

// TestSQLiteStore_Inbox_FiltersByStatus verifies that the status filter
// narrows results to unread or read messages.
func TestSQLiteStore_Inbox_FiltersByStatus(t *testing.T) {
	s, _ := newTestMessagingStore(t)
	ctx := context.Background()

	m1, err := s.Send(ctx, SendInput{
		FromSessionID: "sess-1", FromAgentID: "file-b",
		ToSessionID: "sess-1", ToAgentID: "file-a",
		Body: "m1",
	})
	if err != nil {
		t.Fatalf("seed m1: %v", err)
	}
	if _, err := s.Send(ctx, SendInput{
		FromSessionID: "sess-1", FromAgentID: "file-b",
		ToSessionID: "sess-1", ToAgentID: "file-a",
		Body: "m2",
	}); err != nil {
		t.Fatalf("seed m2: %v", err)
	}

	if err := s.Ack(ctx, m1.ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	unread, err := s.Inbox(ctx, "sess-1", "file-a", InboxFilter{Status: StatusUnread})
	if err != nil {
		t.Fatalf("Inbox unread: %v", err)
	}
	if len(unread) != 1 {
		t.Errorf("unread count = %d, want 1", len(unread))
	}

	read, err := s.Inbox(ctx, "sess-1", "file-a", InboxFilter{Status: StatusRead})
	if err != nil {
		t.Fatalf("Inbox read: %v", err)
	}
	if len(read) != 1 {
		t.Errorf("read count = %d, want 1", len(read))
	}
}

// TestSQLiteStore_Recent verifies that recent messages are returned in
// chronological order (oldest first) after the DESC-then-reverse
// transformation.
func TestSQLiteStore_Recent(t *testing.T) {
	s, _ := newTestMessagingStore(t)
	ctx := context.Background()

	bodies := []string{"one", "two", "three", "four", "five"}
	for _, b := range bodies {
		if _, err := s.Send(ctx, SendInput{
			FromSessionID: "sess-1", FromAgentID: "file-a",
			ToSessionID: "sess-1", ToAgentID: "file-b",
			Body: b,
		}); err != nil {
			t.Fatalf("seed %q: %v", b, err)
		}
	}

	got, err := s.Recent(ctx, "sess-1", 3)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d messages, want 3", len(got))
	}

	// Expect the 3 most recent in chronological order: three, four, five.
	want := []string{"three", "four", "five"}
	for i, m := range got {
		if m.Body != want[i] {
			t.Errorf("msg[%d].body = %q, want %q", i, m.Body, want[i])
		}
	}
}

// TestSQLiteStore_Recent_LimitCap verifies that a huge limit is clamped
// down to MaxRecentLimit and a non-positive limit falls back to the
// default. Covers F05 at the store layer.
func TestSQLiteStore_Recent_LimitCap(t *testing.T) {
	s, _ := newTestMessagingStore(t)
	ctx := context.Background()

	// Seed 150 rows so both cap (100) and default (20) paths can fill.
	for i := 0; i < 150; i++ {
		if _, err := s.Send(ctx, SendInput{
			FromSessionID: "sess-1", FromAgentID: "file-a",
			ToSessionID: "sess-1", ToAgentID: "file-b",
			Body: "msg",
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	got, err := s.Recent(ctx, "sess-1", 100000)
	if err != nil {
		t.Fatalf("Recent huge: %v", err)
	}
	if len(got) != MaxRecentLimit {
		t.Errorf("huge limit: got %d rows, want %d", len(got), MaxRecentLimit)
	}

	got, err = s.Recent(ctx, "sess-1", 0)
	if err != nil {
		t.Fatalf("Recent zero: %v", err)
	}
	if len(got) != DefaultRecentLimit {
		t.Errorf("zero limit: got %d rows, want %d", len(got), DefaultRecentLimit)
	}
}

// TestSQLiteStore_UnreadCount verifies that the count is scoped to
// (to_session_id, to_agent_id) and only counts unread messages,
// ignoring messages the agent sent.
func TestSQLiteStore_UnreadCount(t *testing.T) {
	s, _ := newTestMessagingStore(t)
	ctx := context.Background()

	// file-a receives 2 messages in sess-1.
	for _, from := range []string{"file-b", "file-c"} {
		if _, err := s.Send(ctx, SendInput{
			FromSessionID: "sess-1", FromAgentID: from,
			ToSessionID: "sess-1", ToAgentID: "file-a",
			Body: "msg",
		}); err != nil {
			t.Fatalf("seed to-a: %v", err)
		}
	}
	// file-a sends a message (should not count).
	if _, err := s.Send(ctx, SendInput{
		FromSessionID: "sess-1", FromAgentID: "file-a",
		ToSessionID: "sess-1", ToAgentID: "file-b",
		Body: "from a",
	}); err != nil {
		t.Fatalf("seed from-a: %v", err)
	}
	// A message in a different session (should not count).
	if _, err := s.Send(ctx, SendInput{
		FromSessionID: "sess-2", FromAgentID: "file-b",
		ToSessionID: "sess-2", ToAgentID: "file-a",
		Body: "other sess",
	}); err != nil {
		t.Fatalf("seed other-sess: %v", err)
	}

	n, err := s.UnreadCount(ctx, "sess-1", "file-a")
	if err != nil {
		t.Fatalf("UnreadCount: %v", err)
	}
	if n != 2 {
		t.Errorf("unread count = %d, want 2", n)
	}
}

// TestSQLiteStore_Get_NotFound verifies Get wraps ErrNotFound for a
// missing ID so the HTTP layer can map to 404.
func TestSQLiteStore_Get_NotFound(t *testing.T) {
	s, _ := newTestMessagingStore(t)
	_, err := s.Get(context.Background(), "no-such-id")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("got err=%v, want errors.Is(_, ErrNotFound)", err)
	}
}

// --- Channels ---

// TestSQLiteStore_Send_DefaultsChannel verifies Send populates
// Channel to ChannelChat when SendInput.Channel is empty.
func TestSQLiteStore_Send_DefaultsChannel(t *testing.T) {
	s, _ := newTestMessagingStore(t)
	out, err := s.Send(context.Background(), SendInput{
		FromSessionID: "sess-1", FromAgentID: "file-a",
		ToSessionID: "sess-1", ToAgentID: "file-b",
		Body: "hi",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if out.Channel != ChannelChat {
		t.Errorf("Channel = %q, want %q", out.Channel, ChannelChat)
	}
}

// TestSQLiteStore_Send_InvalidChannelRejected covers the CHECK
// constraint: a channel outside (chat|inbox|alert) is rejected at
// insert.
func TestSQLiteStore_Send_InvalidChannelRejected(t *testing.T) {
	s, _ := newTestMessagingStore(t)
	_, err := s.Send(context.Background(), SendInput{
		FromSessionID: "sess-1", FromAgentID: "file-a",
		ToSessionID: "sess-1", ToAgentID: "file-b",
		Channel: "telepathy",
		Body:    "hi",
	})
	if err == nil {
		t.Fatal("expected CHECK constraint rejection for unknown channel, got nil")
	}
}

// --- MessageKind typed payloads ---

// TestSQLiteStore_Send_DefaultsKindAndPayload verifies Send
// populates Kind to KindNotification and PayloadJSON to "{}" when the
// input leaves them empty.
func TestSQLiteStore_Send_DefaultsKindAndPayload(t *testing.T) {
	s, _ := newTestMessagingStore(t)
	out, err := s.Send(context.Background(), SendInput{
		FromSessionID: "sess-1", FromAgentID: "file-a",
		ToSessionID: "sess-1", ToAgentID: "file-b",
		Body: "hi",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if out.Kind != KindNotification {
		t.Errorf("Kind = %q, want %q", out.Kind, KindNotification)
	}
	if out.PayloadJSON != "{}" {
		t.Errorf("PayloadJSON = %q, want %q", out.PayloadJSON, "{}")
	}
}

// TestSQLiteStore_Send_KindRoundTrip verifies each of the
// wire kinds persists with its payload and comes back intact via Get.
func TestSQLiteStore_Send_KindRoundTrip(t *testing.T) {
	s, _ := newTestMessagingStore(t)
	ctx := context.Background()

	cases := []struct {
		kind    string
		payload string
	}{
		{KindRequest, `{"question":"what time is it?"}`},
		{KindReply, `{"answer":"noon","in_reply_to":"abc-123"}`},
		{KindNotification, `{"summary":"build green"}`},
		{KindHandoff, `{"target_agent":"file-frontend"}`},
		{KindSubagentResult, `{"summary":"subagent finished"}`},
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			out, err := s.Send(ctx, SendInput{
				FromSessionID: "sess-1", FromAgentID: "file-a",
				ToSessionID: "sess-1", ToAgentID: "file-b",
				Kind: tc.kind, PayloadJSON: tc.payload,
				Body: "msg",
			})
			if err != nil {
				t.Fatalf("Send: %v", err)
			}
			got, err := s.Get(ctx, out.ID)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got.Kind != tc.kind {
				t.Errorf("Kind = %q, want %q", got.Kind, tc.kind)
			}
			if got.PayloadJSON != tc.payload {
				t.Errorf("PayloadJSON = %q, want %q", got.PayloadJSON, tc.payload)
			}
		})
	}
}

// TestSQLiteStore_Send_InvalidKindRejected covers the CHECK constraint
// — a kind outside (request|reply|notification|handoff) fails at
// insert.
func TestSQLiteStore_Send_InvalidKindRejected(t *testing.T) {
	s, _ := newTestMessagingStore(t)
	_, err := s.Send(context.Background(), SendInput{
		FromSessionID: "sess-1", FromAgentID: "file-a",
		ToSessionID: "sess-1", ToAgentID: "file-b",
		Kind: "telepathic-blast",
		Body: "hi",
	})
	if err == nil {
		t.Fatal("expected CHECK constraint rejection for unknown kind, got nil")
	}
}

// TestSQLiteStore_Inbox_FiltersByKind verifies the kind filter
// narrows inbox results to matching-kind rows only.
func TestSQLiteStore_Inbox_FiltersByKind(t *testing.T) {
	s, _ := newTestMessagingStore(t)
	ctx := context.Background()

	kinds := []string{KindRequest, KindReply, KindNotification, KindNotification}
	for _, k := range kinds {
		if _, err := s.Send(ctx, SendInput{
			FromSessionID: "sess-1", FromAgentID: "file-b",
			ToSessionID: "sess-1", ToAgentID: "file-a",
			Kind: k, Body: "msg",
		}); err != nil {
			t.Fatalf("seed %s: %v", k, err)
		}
	}

	cases := []struct {
		kind string
		want int
	}{
		{"", 4},
		{KindRequest, 1},
		{KindReply, 1},
		{KindNotification, 2},
		{KindHandoff, 0},
		{KindSubagentResult, 0},
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			msgs, err := s.Inbox(ctx, "sess-1", "file-a", InboxFilter{Kind: tc.kind})
			if err != nil {
				t.Fatalf("Inbox: %v", err)
			}
			if len(msgs) != tc.want {
				t.Errorf("kind=%q: got %d, want %d", tc.kind, len(msgs), tc.want)
			}
		})
	}
}

// TestSQLiteStore_Inbox_SubagentResultKind verifies a
// kind=subagent_result row is both insertable (past the widened CHECK
// constraint) and retrievable via the kind filter, alongside unrelated
// kinds that must not match.
func TestSQLiteStore_Inbox_SubagentResultKind(t *testing.T) {
	s, _ := newTestMessagingStore(t)
	ctx := context.Background()

	if _, err := s.Send(ctx, SendInput{
		FromSessionID: "sess-1", FromAgentID: "file-researcher",
		ToSessionID: "sess-1", ToAgentID: "file-a",
		Kind: KindSubagentResult, Channel: ChannelInbox,
		Body: "subagent file-researcher ended (completed)", PayloadJSON: `{"summary":"done"}`,
	}); err != nil {
		t.Fatalf("seed subagent_result: %v", err)
	}
	if _, err := s.Send(ctx, SendInput{
		FromSessionID: "sess-1", FromAgentID: "file-b",
		ToSessionID: "sess-1", ToAgentID: "file-a",
		Kind: KindReply, Body: "unrelated reply",
	}); err != nil {
		t.Fatalf("seed reply: %v", err)
	}

	msgs, err := s.Inbox(ctx, "sess-1", "file-a", InboxFilter{Kind: KindSubagentResult})
	if err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d subagent_result messages, want 1", len(msgs))
	}
	if msgs[0].FromAgentID != "file-researcher" {
		t.Errorf("FromAgentID = %q, want %q", msgs[0].FromAgentID, "file-researcher")
	}
}

// TestSQLiteStore_Inbox_FiltersByChannel verifies the channel filter
// narrows inbox results to matching-channel rows only.
func TestSQLiteStore_Inbox_FiltersByChannel(t *testing.T) {
	s, _ := newTestMessagingStore(t)
	ctx := context.Background()

	// Seed: 2 chat, 1 inbox, 1 alert — all to (sess-1, file-a).
	channels := []string{ChannelChat, ChannelChat, ChannelInbox, ChannelAlert}
	for _, ch := range channels {
		if _, err := s.Send(ctx, SendInput{
			FromSessionID: "sess-1", FromAgentID: "file-b",
			ToSessionID: "sess-1", ToAgentID: "file-a",
			Channel: ch, Body: "msg",
		}); err != nil {
			t.Fatalf("seed %s: %v", ch, err)
		}
	}

	cases := []struct {
		channel string
		want    int
	}{
		{"", 4},
		{ChannelChat, 2},
		{ChannelInbox, 1},
		{ChannelAlert, 1},
	}
	for _, tc := range cases {
		t.Run(tc.channel, func(t *testing.T) {
			msgs, err := s.Inbox(ctx, "sess-1", "file-a", InboxFilter{Channel: tc.channel})
			if err != nil {
				t.Fatalf("Inbox: %v", err)
			}
			if len(msgs) != tc.want {
				t.Errorf("channel=%q: got %d, want %d", tc.channel, len(msgs), tc.want)
			}
		})
	}
}
