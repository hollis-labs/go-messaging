package httpstore_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	messaging "github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/httpstore"
	"github.com/hollis-labs/go-messaging/httpstore/httpstoretest"
	"github.com/hollis-labs/go-messaging/memstore"
	"github.com/hollis-labs/go-messaging/messagingtest"
)

var (
	caller = httpstoretest.ConformanceIdentity
	alice  = messaging.Address{Kind: messaging.KindAgent, Authority: "test", ID: "alice"}
	bob    = messaging.Address{Kind: messaging.KindAgent, Authority: "test", ID: "bob"}
)

// newStore returns a Store for profile p talking to a fresh memstore behind
// the reference server, plus the memstore for direct inspection.
func newStore(t *testing.T, p httpstore.Profile, serverOpts []httpstoretest.Option, opts ...httpstore.Option) (*httpstore.Store, *memstore.Store) {
	t.Helper()
	ms := memstore.New()
	srv := httpstoretest.NewServer(t, ms, nil, p, serverOpts...)
	s, err := httpstore.New(srv.URL, append([]httpstore.Option{httpstore.WithProfile(p)}, opts...)...)
	if err != nil {
		t.Fatalf("httpstore.New: %v", err)
	}
	return s, ms
}

func notice(from, to messaging.Address) messaging.Envelope {
	return messaging.Envelope{Kind: messaging.MsgKindNotice, From: from, To: to}
}

// TestTetherProfile_Contract runs the full Store contract (all sub-tests)
// and the Router contract over HTTP against the reference server, with the
// Tether profile and a fixed identity for Get/Thread.
func TestTetherProfile_Contract(t *testing.T) {
	factory := func(t *testing.T) messaging.Store {
		s, _ := newStore(t, httpstore.TetherProfile(), nil, httpstore.WithIdentity(caller))
		return s
	}
	messagingtest.RunContract(t, factory)
}

func TestTetherProfile_RouterContract(t *testing.T) {
	messagingtest.RunRouterContract(t, func(t *testing.T) messaging.Store {
		s, _ := newStore(t, httpstore.TetherProfile(), nil, httpstore.WithIdentity(caller))
		return s
	})
}

// TestRunConformance covers the wiring downstream packages call: both
// profiles, the Torque one restricted with Without.
func TestRunConformance(t *testing.T) {
	t.Run("tether", func(t *testing.T) { httpstoretest.RunConformance(t, httpstore.TetherProfile()) })
	t.Run("torque", func(t *testing.T) { httpstoretest.RunConformance(t, httpstore.TorqueFederationProfile()) })
}

// TestTorqueProfile_Contract runs the Store contract minus the operations
// the federation hop does not carry.
func TestTorqueProfile_Contract(t *testing.T) {
	factory := func(t *testing.T) messaging.Store {
		s, _ := newStore(t, httpstore.TorqueFederationProfile(), nil)
		return s
	}
	messagingtest.RunContract(t, factory, messagingtest.Without("Inbox", "Subscribe"))
}

// TestTorqueProfile_ThreadChronological replaces the contract's "Thread
// chronological" sub-test, which is skipped because it asserts through
// Inbox: order, thread scoping and that Thread has no delivery side effect
// (checked on the backing store).
func TestTorqueProfile_ThreadChronological(t *testing.T) {
	s, ms := newStore(t, httpstore.TorqueFederationProfile(), nil)
	ctx := context.Background()
	to := messaging.Address{Kind: messaging.KindAgent, Authority: "test", ID: "r4"}

	var sent []messaging.Envelope
	for i := 0; i < 3; i++ {
		e := notice(alice, to)
		e.ThreadID = "TT"
		out, err := s.Send(ctx, e)
		if err != nil {
			t.Fatal(err)
		}
		sent = append(sent, out)
		time.Sleep(time.Millisecond)
	}
	other := notice(alice, to)
	other.ThreadID = "OTHER"
	if _, err := s.Send(ctx, other); err != nil {
		t.Fatal(err)
	}

	got, err := s.Thread(ctx, "TT", messaging.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(sent) {
		t.Fatalf("Thread returned %d envelopes, want %d", len(got), len(sent))
	}
	for i := range got {
		if got[i].ID != sent[i].ID {
			t.Errorf("position %d: got %s, want %s (chronological order)", i, got[i].ID, sent[i].ID)
		}
	}
	inbox, err := ms.Inbox(ctx, to, messaging.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(inbox) != 4 {
		t.Errorf("Thread mutated delivery state: backing Inbox has %d envelopes, want all 4 still undelivered", len(inbox))
	}
}

// TestTorqueProfile_Unsupported: Inbox and Subscribe fail with ErrUnsupported
// (which is an ErrStoreUnavailable) and never touch the network.
func TestTorqueProfile_Unsupported(t *testing.T) {
	var hits int
	s, _ := newStore(t, httpstore.TorqueFederationProfile(), nil,
		httpstore.WithRequestHook(func(*http.Request, httpstore.Op) error { hits++; return nil }))
	ctx := context.Background()

	_, err := s.Inbox(ctx, bob, messaging.Filter{})
	if !errors.Is(err, httpstore.ErrUnsupported) || !errors.Is(err, messaging.ErrStoreUnavailable) {
		t.Errorf("Inbox: got %v, want ErrUnsupported and ErrStoreUnavailable", err)
	}
	ch, err := s.Subscribe(ctx, bob, messaging.Filter{})
	if !errors.Is(err, httpstore.ErrUnsupported) || !errors.Is(err, messaging.ErrStoreUnavailable) {
		t.Errorf("Subscribe: got %v, want ErrUnsupported and ErrStoreUnavailable", err)
	}
	if ch != nil {
		t.Error("Subscribe returned a channel alongside its error")
	}
	if hits != 0 {
		t.Errorf("unsupported operations sent %d request(s), want none", hits)
	}
}
