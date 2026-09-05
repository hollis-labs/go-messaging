package mailbox

// Tests for SendMessage's host-owned wake seam. The shared package invokes
// the hook for every mailbox kind and leaves all filtering to the host.

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeWakeReactor records ReactToMessage calls so tests can assert
// whether SendMessage triggered the live-wake side effect. SendMessage
// fires the reactor in its own goroutine (see the SendMessage call
// site's comment), so callers must poll rather than assert immediately.
type fakeWakeReactor struct {
	mu    sync.Mutex
	calls []*Message
}

func (f *fakeWakeReactor) ReactToMessage(_ context.Context, msg *Message) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, msg)
}

func (f *fakeWakeReactor) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeWakeReactor) call(index int) Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return *f.calls[index]
}

func (f *fakeWakeReactor) waitForCount(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if f.count() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := f.count(); got != want {
		t.Fatalf("wake reactor call count = %d, want %d", got, want)
	}
}

func TestService_SendMessage_WakesReactorForOrdinaryMessage(t *testing.T) {
	svc, _ := newTestService(t, "file-backend")
	wake := &fakeWakeReactor{}
	svc.SetWakeReactor(wake)

	in := baseInput(testHumanID, "file-backend")
	in.Kind = KindNotification
	out, err := svc.SendMessage(context.Background(), in)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	wake.waitForCount(t, 1)
	call := wake.call(0)
	if call.ID != out.ID {
		t.Errorf("reactor received message id %q, want %q", call.ID, out.ID)
	}
}

// TestService_SendMessage_WakesReactorForEveryMailboxKind proves there is no
// package-owned wake policy hidden behind a particular kind.
func TestService_SendMessage_WakesReactorForEveryMailboxKind(t *testing.T) {
	for _, kind := range []string{KindRequest, KindReply, KindNotification, KindHandoff} {
		t.Run(kind, func(t *testing.T) {
			svc, _ := newTestService(t, "file-backend")
			wake := &fakeWakeReactor{}
			svc.SetWakeReactor(wake)

			in := baseInput(testHumanID, "file-backend")
			in.Kind = kind
			out, err := svc.SendMessage(context.Background(), in)
			if err != nil {
				t.Fatalf("SendMessage: %v", err)
			}

			wake.waitForCount(t, 1)
			call := wake.call(0)
			if call.ID != out.ID {
				t.Errorf("reactor received message id %q, want %q", call.ID, out.ID)
			}
			if call.Kind != kind {
				t.Errorf("reactor received kind %q, want %q", call.Kind, kind)
			}
		})
	}
}

func TestService_SendMessage_NilWakeReactor_NoOp(t *testing.T) {
	// No SetWakeReactor call — svc.wake stays nil. Regression guard: a
	// send must not panic or behave differently when no reactor is wired
	// (the test-double-without-wiring shape).
	svc, _ := newTestService(t, "file-backend")

	if _, err := svc.SendMessage(context.Background(), baseInput(testHumanID, "file-backend")); err != nil {
		t.Fatalf("SendMessage with nil wake reactor: %v", err)
	}
}
