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
	return &Message{
		ID:            fmt.Sprintf("message-%d", s.next.Add(1)),
		FromSessionID: input.FromSessionID, FromAgentID: input.FromAgentID,
		ToSessionID: input.ToSessionID, ToAgentID: input.ToAgentID,
		Body: input.Body, Channel: ChannelChat, Kind: KindNotification,
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
