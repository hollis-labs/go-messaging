package mailbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
)

type memoryEventStore struct {
	mu     sync.Mutex
	events []SessionEvent
}

func (s *memoryEventStore) Append(_ context.Context, event SessionEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return nil
}

func (s *memoryEventStore) Recent(_ context.Context, sessionID string, limit int) ([]SessionEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	matching := make([]SessionEvent, 0, len(s.events))
	for _, event := range s.events {
		if event.SessionID == sessionID {
			matching = append(matching, event)
		}
	}
	start := len(matching) - limit
	if start < 0 {
		start = 0
	}
	return append([]SessionEvent(nil), matching[start:]...), nil
}

func (s *memoryEventStore) snapshot() []SessionEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]SessionEvent(nil), s.events...)
}

func newEventTestService(t *testing.T, knownAgents ...string) (*Service, *SQLiteStore, *memoryEventStore) {
	t.Helper()
	svc, store := newTestService(t, knownAgents...)
	events := &memoryEventStore{}
	svc.SetEventStore(events)
	return svc, store, events
}

func TestService_SendWritesSessionEvents(t *testing.T) {
	svc, _, _ := newEventTestService(t, "file-a")
	sent, err := svc.SendMessage(context.Background(), SendInput{
		FromSessionID: "sess-1", FromAgentID: testHumanID,
		ToSessionID: "sess-2", ToAgentID: "file-a", Body: "hi",
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	fromEvents, err := svc.SessionEvents(context.Background(), "sess-1", 0)
	if err != nil {
		t.Fatalf("SessionEvents sender: %v", err)
	}
	toEvents, err := svc.SessionEvents(context.Background(), "sess-2", 0)
	if err != nil {
		t.Fatalf("SessionEvents recipient: %v", err)
	}
	if len(fromEvents) != 1 || fromEvents[0].EventType != EventMessageSent {
		t.Fatalf("sender events = %+v, want one message_sent", fromEvents)
	}
	if len(toEvents) != 1 || toEvents[0].EventType != EventMessageReceived {
		t.Fatalf("recipient events = %+v, want one message_received", toEvents)
	}
	var pointer messagePointer
	if err := json.Unmarshal([]byte(toEvents[0].EnvelopePointerJSON), &pointer); err != nil {
		t.Fatalf("unmarshal envelope pointer: %v", err)
	}
	if pointer.MessageID != sent.ID {
		t.Errorf("pointer message_id = %q, want %q", pointer.MessageID, sent.ID)
	}
}

func TestService_AckAndResolveWriteEvents(t *testing.T) {
	for _, tc := range []struct {
		name      string
		eventType string
		mutate    func(*Service, context.Context, string) error
	}{
		{"ack", EventMessageAcked, func(svc *Service, ctx context.Context, id string) error {
			return svc.Ack(ctx, "sess-1", "file-backend", id)
		}},
		{"resolve", EventMessageResolved, func(svc *Service, ctx context.Context, id string) error {
			return svc.Resolve(ctx, "sess-1", "file-backend", id)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, store, events := newEventTestService(t, "file-backend")
			ctx := context.Background()
			seeded, err := store.Send(ctx, SendInput{
				FromSessionID: "sess-1", FromAgentID: testHumanID,
				ToSessionID: "sess-1", ToAgentID: "file-backend", Body: "hi",
			})
			if err != nil {
				t.Fatalf("seed: %v", err)
			}
			if err := tc.mutate(svc, ctx, seeded.ID); err != nil {
				t.Fatalf("mutate: %v", err)
			}
			got := events.snapshot()
			if len(got) != 1 || got[0].EventType != tc.eventType {
				t.Fatalf("events = %+v, want one %s", got, tc.eventType)
			}
		})
	}
}

func TestService_SessionEvents_MostRecentPageChronological(t *testing.T) {
	svc, _, events := newEventTestService(t)
	for i := 1; i <= 3; i++ {
		if err := events.Append(context.Background(), SessionEvent{
			ID: fmt.Sprintf("event-%d", i), SessionID: "sess-1", CreatedAt: fmt.Sprintf("%d", i),
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	got, err := svc.SessionEvents(context.Background(), "sess-1", 2)
	if err != nil {
		t.Fatalf("SessionEvents: %v", err)
	}
	if len(got) != 2 || got[0].ID != "event-2" || got[1].ID != "event-3" {
		t.Fatalf("recent page = %+v, want event-2,event-3", got)
	}
}

type recordingEventStore struct {
	mu        sync.Mutex
	lastLimit int
}

func (*recordingEventStore) Append(context.Context, SessionEvent) error { return nil }
func (s *recordingEventStore) Recent(_ context.Context, _ string, limit int) ([]SessionEvent, error) {
	s.mu.Lock()
	s.lastLimit = limit
	s.mu.Unlock()
	return nil, nil
}

func TestService_SessionEvents_LimitsAndConfiguration(t *testing.T) {
	svc, _ := newTestService(t)
	if _, err := svc.SessionEvents(context.Background(), "sess-1", 1); !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("missing store error = %v, want ErrNotConfigured", err)
	}
	if _, err := svc.SessionEvents(context.Background(), "", 1); !errors.Is(err, ErrValidation) {
		t.Fatalf("empty session error = %v, want ErrValidation", err)
	}

	recorder := &recordingEventStore{}
	svc.SetEventStore(recorder)
	for _, tc := range []struct{ input, want int }{{0, 100}, {-1, 100}, {17, 17}, {100000, 500}} {
		if _, err := svc.SessionEvents(context.Background(), "sess-1", tc.input); err != nil {
			t.Fatalf("SessionEvents(%d): %v", tc.input, err)
		}
		recorder.mu.Lock()
		got := recorder.lastLimit
		recorder.mu.Unlock()
		if got != tc.want {
			t.Errorf("input %d delegated limit %d, want %d", tc.input, got, tc.want)
		}
	}
}
