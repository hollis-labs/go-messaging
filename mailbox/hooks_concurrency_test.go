package mailbox

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

type hookTestStore struct{ next atomic.Uint64 }

func (s *hookTestStore) Send(_ context.Context, input SendInput) (*Message, error) {
	readAt := "2026-09-04T12:00:00Z"
	resolvedAt := "2026-09-04T12:01:00Z"
	return &Message{
		ID:            fmt.Sprintf("message-%d", s.next.Add(1)),
		FromSessionID: input.FromSessionID, FromAgentID: input.FromAgentID,
		ToSessionID: input.ToSessionID, ToAgentID: input.ToAgentID,
		Body: input.Body, Channel: ChannelChat, Kind: KindNotification,
		ReadAt: &readAt, ResolvedAt: &resolvedAt,
	}, nil
}
func (*hookTestStore) Get(context.Context, string) (*Message, error) { panic("unexpected Get") }
func (*hookTestStore) Inbox(context.Context, string, string, InboxFilter) ([]Message, error) {
	panic("unexpected Inbox")
}
func (*hookTestStore) Thread(context.Context, string) ([]Message, error) { panic("unexpected Thread") }
func (*hookTestStore) Recent(context.Context, string, int) ([]Message, error) {
	panic("unexpected Recent")
}
func (*hookTestStore) Ack(context.Context, string) error     { panic("unexpected Ack") }
func (*hookTestStore) Resolve(context.Context, string) error { panic("unexpected Resolve") }
func (*hookTestStore) UnreadCount(context.Context, string, string) (int, error) {
	panic("unexpected UnreadCount")
}

type noOpSink struct{}

func (*noOpSink) NotifyReceived(context.Context, *Message) {}

type noOpWake struct{}

func (*noOpWake) ReactToMessage(context.Context, *Message) {}

type inlineRunner struct{}

func (*inlineRunner) Go(_ string, fn func(context.Context)) { fn(context.Background()) }

type ownershipCapture struct {
	notification *Message
	wake         *Message
}

func (c *ownershipCapture) NotifyReceived(_ context.Context, msg *Message) {
	c.notification = msg
}

func (c *ownershipCapture) ReactToMessage(_ context.Context, msg *Message) {
	c.wake = msg
}

func TestService_HooksReceiveIndependentlyOwnedDeepCopies(t *testing.T) {
	svc := NewService(&hookTestStore{}, newFakeResolver(testHumanID, "file-a"), nil)
	capture := &ownershipCapture{}
	svc.SetNotificationSink(capture)
	svc.SetWakeReactor(capture)
	svc.SetAsyncRunner(&inlineRunner{})

	sent, err := svc.SendMessage(context.Background(), baseInput(testHumanID, "file-a"))
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	notification := capture.notification
	wake := capture.wake
	if notification == nil || wake == nil {
		t.Fatalf("hook deliveries: notification=%p wake=%p", notification, wake)
	}
	if sent == notification || sent == wake || notification == wake {
		t.Errorf("hook messages alias: sender=%p notification=%p wake=%p", sent, notification, wake)
	}
	if sent.ReadAt == notification.ReadAt || sent.ReadAt == wake.ReadAt || notification.ReadAt == wake.ReadAt {
		t.Errorf("hook ReadAt pointers alias: sender=%p notification=%p wake=%p", sent.ReadAt, notification.ReadAt, wake.ReadAt)
	}
	if sent.ResolvedAt == notification.ResolvedAt || sent.ResolvedAt == wake.ResolvedAt || notification.ResolvedAt == wake.ResolvedAt {
		t.Errorf("hook ResolvedAt pointers alias: sender=%p notification=%p wake=%p", sent.ResolvedAt, notification.ResolvedAt, wake.ResolvedAt)
	}

	*notification.ReadAt = "notification-mutated"
	*wake.ResolvedAt = "wake-mutated"
	if *sent.ReadAt != "2026-09-04T12:00:00Z" || *wake.ReadAt != "2026-09-04T12:00:00Z" {
		t.Errorf("notification mutation escaped its copy: sender=%q wake=%q", *sent.ReadAt, *wake.ReadAt)
	}
	if *sent.ResolvedAt != "2026-09-04T12:01:00Z" || *notification.ResolvedAt != "2026-09-04T12:01:00Z" {
		t.Errorf("wake mutation escaped its copy: sender=%q notification=%q", *sent.ResolvedAt, *notification.ResolvedAt)
	}
}

// TestService_HookReconfigurationConcurrentWithReaders is a race-detector
// regression for every setter/read path in Service. Hooks remain valid for the
// lifetime of the test; only their publication changes concurrently.
func TestService_HookReconfigurationConcurrentWithReaders(t *testing.T) {
	resolver := newFakeResolver(testHumanID, "file-a", "file-b")
	svc := NewService(&hookTestStore{}, resolver, nil)
	sinks := [2]NotificationSink{&noOpSink{}, &noOpSink{}}
	wakes := [2]WakeReactor{&noOpWake{}, &noOpWake{}}
	runners := [2]AsyncRunner{&inlineRunner{}, &inlineRunner{}}
	events := [2]*memoryEventStore{{}, {}}
	handoffs := [2]*fakeHandoffCoordinator{{}, {}}
	svc.SetNotificationSink(sinks[0])
	svc.SetWakeReactor(wakes[0])
	svc.SetAsyncRunner(runners[0])
	svc.SetEventStore(events[0])
	svc.SetHandoffCoordinator(handoffs[0])

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(5)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 10000; i++ {
			index := i % 2
			svc.SetNotificationSink(sinks[index])
			svc.SetWakeReactor(wakes[index])
			svc.SetAsyncRunner(runners[index])
			svc.SetEventStore(events[index])
			svc.SetHandoffCoordinator(handoffs[index])
		}
	}()
	for worker := 0; worker < 4; worker++ {
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 500; i++ {
				if _, err := svc.SendMessage(context.Background(), baseInput(testHumanID, "file-a")); err != nil {
					t.Errorf("SendMessage: %v", err)
					return
				}
				if _, err := svc.SessionEvents(context.Background(), "sess-1", 10); err != nil {
					t.Errorf("SessionEvents: %v", err)
					return
				}
				if _, err := svc.RequestHandoff(context.Background(), "sess-1", "file-a", "file-b", "host"); err != nil {
					t.Errorf("RequestHandoff: %v", err)
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
}
