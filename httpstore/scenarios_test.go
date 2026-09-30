package httpstore_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	messaging "github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/httpstore"
	"github.com/hollis-labs/go-messaging/httpstore/httpstoretest"
	"github.com/hollis-labs/go-messaging/memstore"
)

// The scenarios below are ported from the five private copies this package
// replaces, and run against httpstoretest with WithStrictIdentity so the
// server refuses what a real Tether daemon refuses:
//
//   go-tether-client      TestHTTPStore_GetAndThread_RequireSelfURN, _SendsAsQueryParam
//   Tether peerstore      peerstore_test.go (7 scenarios), peerstore_real_daemon_test.go (3)
//   Torque httpstore      httpstore_test.go (7 scenarios; Torque profile)
//   new                   Subscribe sends `as` (no copy had a test for it)

var strict = []httpstoretest.Option{httpstoretest.WithStrictIdentity()}

func tetherStrict(t *testing.T, opts ...httpstore.Option) (*httpstore.Store, *memstore.Store, *recorder, string) {
	t.Helper()
	ms := memstore.New()
	rc := &recorder{}
	srv := httptest.NewServer(rc.wrap(httpstoretest.Handler(ms, nil, httpstore.TetherProfile(), strict...)))
	t.Cleanup(srv.Close)
	s, err := httpstore.New(srv.URL, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return s, ms, rc, srv.URL
}

// --- go-tether-client ------------------------------------------------------

// Get and Thread carry no address to derive ?as= from, so without
// WithIdentity they must fail client-side, before any request.
func TestScenario_GetAndThreadRequireIdentity(t *testing.T) {
	s, _, rc, _ := tetherStrict(t) // no WithIdentity
	if _, err := s.Get(context.Background(), "some-id"); !errors.Is(err, httpstore.ErrIdentityRequired) {
		t.Errorf("Get: got %v, want ErrIdentityRequired", err)
	}
	if _, err := s.Thread(context.Background(), "some-thread", messaging.Filter{}); !errors.Is(err, httpstore.ErrIdentityRequired) {
		t.Errorf("Thread: got %v, want ErrIdentityRequired", err)
	}
	if n := rc.count(); n != 0 {
		t.Errorf("Get/Thread reached the server %d time(s); they must fail client-side", n)
	}
}

// A profile that does not assert identity needs no WithIdentity.
func TestScenario_NoIdentityNeededWithoutAssertAs(t *testing.T) {
	s, ms := newStore(t, httpstore.TorqueFederationProfile(), nil)
	sent, err := ms.Send(context.Background(), notice(alice, bob))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(context.Background(), sent.ID); err != nil {
		t.Errorf("Get without identity on a non-asserting profile: %v", err)
	}
}

// Every read attaches the claim Tether requires: Get/Thread the configured
// identity, Inbox/Subscribe/Consume the recipient they were called with.
// The strict server would answer 400 to a missing claim, so success also
// proves it was sent.
func TestScenario_SendsAsQueryParam(t *testing.T) {
	self := alice
	s, _, rc, _ := tetherStrict(t, httpstore.WithIdentity(self))
	ctx := context.Background()

	e := notice(self, bob)
	e.ThreadID = "th"
	sent, err := s.Send(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, sent.ID); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := rc.last(t).Query.Get("as"); got != self.URN() {
		t.Errorf("Get: as=%q, want %q", got, self.URN())
	}
	if _, err := s.Thread(ctx, "th", messaging.Filter{}); err != nil {
		t.Fatalf("Thread: %v", err)
	}
	if got := rc.last(t).Query.Get("as"); got != self.URN() {
		t.Errorf("Thread: as=%q, want %q", got, self.URN())
	}
	if _, err := s.Inbox(ctx, bob, messaging.Filter{}); err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	if got := rc.last(t).Query.Get("as"); got != bob.URN() {
		t.Errorf("Inbox: as=%q, want the recipient %q", got, bob.URN())
	}
	if err := s.Consume(ctx, sent.ID, bob); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if got := rc.last(t).Query.Get("as"); got != bob.URN() {
		t.Errorf("Consume: as=%q, want the recipient %q", got, bob.URN())
	}
}

// --- new: the scenario no copy tested --------------------------------------

// Subscribe must claim ?as=<to>: Tether has refused a stream without it
// since T11, and the copies that lacked it (Tether's own peerstore) 400.
func TestSubscribe_SendsAs(t *testing.T) {
	s, _, rc, url := tetherStrict(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := s.Subscribe(ctx, bob, messaging.Filter{Kind: []messaging.Kind{messaging.MsgKindNotice}})
	if err != nil {
		t.Fatalf("Subscribe against a strict server: %v", err)
	}
	req := rc.last(t)
	if req.Path != "/messages/subscribe" {
		t.Fatalf("path = %q, want /messages/subscribe", req.Path)
	}
	if got := req.Query.Get("as"); got != bob.URN() {
		t.Errorf("as = %q, want %q", got, bob.URN())
	}
	if got := req.Query.Get("to"); got != bob.URN() {
		t.Errorf("to = %q, want %q", got, bob.URN())
	}

	// Live delivery works end to end under the strict rules.
	if _, err := s.Send(ctx, notice(alice, bob)); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-ch:
		if got.To != bob {
			t.Errorf("delivered to %v", got.To)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no envelope within 2s")
	}

	// And the strict server really does refuse a stream without the claim,
	// which is what makes the test above meaningful.
	resp, err := http.Get(url + "/messages/subscribe?to=" + bob.URN())
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("strict server answered %d to a subscribe without as, want 400", resp.StatusCode)
	}
}

// --- Tether peerstore_test.go ----------------------------------------------

func TestScenario_PeerStoreSend(t *testing.T) {
	s, ms, _, _ := tetherStrict(t)
	sent, err := s.Send(context.Background(), notice(alice, bob))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if sent.ID == "" {
		t.Fatal("Send did not return a server-assigned id")
	}
	if _, err := ms.Get(context.Background(), sent.ID); err != nil {
		t.Fatalf("remote store missing the sent envelope: %v", err)
	}
}

// Tether's copy refused Get/Thread outright because it had no identity. With
// WithIdentity they work; without it they fail honestly (see
// TestScenario_GetAndThreadRequireIdentity).
func TestScenario_PeerStoreGetAndThreadWithIdentity(t *testing.T) {
	s, _, _, _ := tetherStrict(t, httpstore.WithIdentity(alice))
	ctx := context.Background()
	e := notice(alice, bob)
	e.ThreadID = "thread-1"
	sent, err := s.Send(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, sent.ID)
	if err != nil || got.ID != sent.ID {
		t.Fatalf("Get: %+v, %v", got, err)
	}
	th, err := s.Thread(ctx, "thread-1", messaging.Filter{})
	if err != nil || len(th) != 1 {
		t.Fatalf("Thread: %d envelopes, %v", len(th), err)
	}
}

func TestScenario_PeerStoreInbox(t *testing.T) {
	s, _, _, _ := tetherStrict(t)
	ctx := context.Background()
	e := notice(alice, bob)
	e.ThreadID = "thread-1"
	if _, err := s.Send(ctx, e); err != nil {
		t.Fatal(err)
	}
	inbox, err := s.Inbox(ctx, bob, messaging.Filter{})
	if err != nil || len(inbox) != 1 {
		t.Fatalf("Inbox: %d envelopes, %v", len(inbox), err)
	}
}

func TestScenario_PeerStoreConsumeAndCancel(t *testing.T) {
	s, _, _, _ := tetherStrict(t)
	ctx := context.Background()
	sent, err := s.Send(ctx, notice(alice, bob))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Consume(ctx, sent.ID, bob); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if err := s.Cancel(ctx, sent.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
}

func TestScenario_PeerStoreErrorMapping(t *testing.T) {
	s, _, _, _ := tetherStrict(t)
	ctx := context.Background()
	sent, err := s.Send(ctx, notice(alice, bob))
	if err != nil {
		t.Fatal(err)
	}
	// Consume as someone the envelope is not addressed to: 409.
	err = s.Consume(ctx, sent.ID, alice)
	if !errors.Is(err, httpstore.ErrWrongRecipient) {
		t.Fatalf("Consume as the wrong recipient: got %v, want ErrWrongRecipient", err)
	}
	var se *httpstore.StatusError
	if !errors.As(err, &se) || se.Code != http.StatusConflict {
		t.Errorf("errors.As(*StatusError) = %v (%+v), want HTTP 409", errors.As(err, &se), se)
	}
	// An unknown id is a 404, not a wrong recipient.
	if err := s.Consume(ctx, "no-such", bob); !errors.Is(err, messaging.ErrNotFound) {
		t.Errorf("Consume of an unknown id: got %v, want ErrNotFound", err)
	}
}

func TestScenario_PeerStoreRejectsPresetLifecycle(t *testing.T) {
	s, _, rc, _ := tetherStrict(t)
	now := time.Now()
	for name, mutate := range map[string]func(*messaging.Envelope){
		"DeliveredAt": func(e *messaging.Envelope) { e.DeliveredAt = &now },
		"ConsumedAt":  func(e *messaging.Envelope) { e.ConsumedAt = &now },
	} {
		e := notice(alice, bob)
		mutate(&e)
		if _, err := s.Send(context.Background(), e); !errors.Is(err, messaging.ErrPresetLifecycle) {
			t.Errorf("Send with preset %s: got %v, want ErrPresetLifecycle", name, err)
		}
	}
	if n := rc.count(); n != 0 {
		t.Errorf("preset lifecycle reached the server %d time(s); it must be rejected client-side", n)
	}
}

// A server that speaks the event stream by hand, as Tether does.
func TestScenario_PeerStoreSubscribe(t *testing.T) {
	want := notice(alice, bob)
	want.ID = "evt-1"
	want.CreatedAt = time.Now().UTC()

	mux := http.NewServeMux()
	mux.HandleFunc("/messages/subscribe", func(w http.ResponseWriter, r *http.Request) {
		flusher := w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()
		b, _ := jsonMarshal(want)
		_, _ = io.WriteString(w, ": ping\n\nevent: message\ndata: "+string(b)+"\n\n")
		flusher.Flush()
		<-r.Context().Done()
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	s, err := httpstore.New(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := s.Subscribe(ctx, bob, messaging.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got, ok := <-ch:
		if !ok {
			t.Fatal("channel closed before delivering an event")
		}
		if got.ID != want.ID {
			t.Fatalf("delivered id %q, want %q", got.ID, want.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for an envelope")
	}
}

// --- Tether peerstore_real_daemon_test.go ----------------------------------

// The real daemon 400s an Inbox without ?as=. The strict server is our
// stand-in: the client must send it, and a raw request without it fails.
func TestScenario_RealDaemon_InboxRequiredAsIsSent(t *testing.T) {
	s, _, rc, url := tetherStrict(t)
	ctx := context.Background()
	if _, err := s.Send(ctx, notice(alice, bob)); err != nil {
		t.Fatal(err)
	}
	inbox, err := s.Inbox(ctx, bob, messaging.Filter{})
	if err != nil || len(inbox) != 1 {
		t.Fatalf("Inbox against a strict server: %d envelopes, %v", len(inbox), err)
	}
	if rc.last(t).Query.Get("as") != bob.URN() {
		t.Error("Inbox did not send as=<to>")
	}
	resp, err := http.Get(url + "/messages/inbox?to=" + bob.URN())
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("strict server answered %d to an inbox without as, want 400", resp.StatusCode)
	}
}

// A present-but-wrong ?as= on Get is rejected 403, and that authorization
// failure reads as ErrStoreUnavailable (never ErrNotFound), with the
// server's reason still reachable.
func TestScenario_RealDaemon_GetRejectsPresentButWrongAs(t *testing.T) {
	owner, imposter := bob, messaging.Address{Kind: messaging.KindAgent, Authority: "test", ID: "imposter"}
	s, ms, _, url := tetherStrict(t, httpstore.WithIdentity(imposter))
	sent, err := ms.Send(context.Background(), notice(alice, owner))
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.Get(context.Background(), sent.ID)
	if !errors.Is(err, messaging.ErrStoreUnavailable) || errors.Is(err, messaging.ErrNotFound) {
		t.Fatalf("Get as an imposter: got %v, want ErrStoreUnavailable and not ErrNotFound", err)
	}
	var se *httpstore.StatusError
	if !errors.As(err, &se) || se.Code != http.StatusForbidden || !strings.Contains(se.Message, "sender or recipient") {
		t.Errorf("StatusError = %+v, want 403 naming the sender-or-recipient rule", se)
	}

	// The same envelope is readable by a party.
	party, err := httpstore.New(url, httpstore.WithIdentity(owner))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := party.Get(context.Background(), sent.ID); err != nil {
		t.Errorf("Get as the recipient: %v", err)
	}
}

// Consume across a Router hop as the wrong recipient is observable and
// distinguishable, and the real recipient can still consume afterwards.
func TestScenario_RealDaemon_FederatedConsumeWrongRecipient(t *testing.T) {
	peer, _, _, _ := tetherStrict(t)
	r := messaging.NewRouter(memstore.New(), "tether")
	if err := r.Register("torque", peer); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	owner := messaging.Address{Kind: messaging.KindAgent, Authority: "torque", ID: "real-owner"}
	imposter := messaging.Address{Kind: messaging.KindAgent, Authority: "torque", ID: "imposter"}
	sender := messaging.Address{Kind: messaging.KindAgent, Authority: "tether", ID: "sender"}

	sent, err := r.Send(ctx, notice(sender, owner))
	if err != nil {
		t.Fatalf("Send routed to the peer authority: %v", err)
	}
	if err := r.Consume(ctx, sent.ID, imposter); !errors.Is(err, httpstore.ErrWrongRecipient) {
		t.Fatalf("Consume across the hop as the wrong recipient: got %v, want ErrWrongRecipient", err)
	}
	if err := r.Consume(ctx, sent.ID, owner); err != nil {
		t.Fatalf("Consume as the real recipient: %v", err)
	}
}

// A thread read with a non-party identity is not an error: the server
// returns only the turns involving that identity. Documented behaviour.
func TestScenario_ThreadIsScopedToTheClaimedIdentity(t *testing.T) {
	stranger := messaging.Address{Kind: messaging.KindAgent, Authority: "test", ID: "stranger"}
	party, _, _, url := tetherStrict(t, httpstore.WithIdentity(alice))
	e := notice(alice, bob)
	e.ThreadID = "T"
	if _, err := party.Send(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	outsider, err := httpstore.New(url, httpstore.WithIdentity(stranger))
	if err != nil {
		t.Fatal(err)
	}
	got, err := outsider.Thread(context.Background(), "T", messaging.Filter{})
	if err != nil || len(got) != 0 {
		t.Errorf("non-party Thread = %d envelopes, %v; want an empty (thinned) result and no error", len(got), err)
	}
	got, err = party.Thread(context.Background(), "T", messaging.Filter{})
	if err != nil || len(got) != 1 {
		t.Errorf("party Thread = %d envelopes, %v; want 1", len(got), err)
	}
}

// --- Torque httpstore_test.go (Torque profile) -----------------------------

func TestScenario_TorqueRoundTrip(t *testing.T) {
	ms := memstore.New()
	rc := &recorder{}
	srv := httptest.NewServer(rc.wrap(httpstoretest.Handler(ms, nil, httpstore.TorqueFederationProfile(), strict...)))
	t.Cleanup(srv.Close)
	s, err := httpstore.New(srv.URL, httpstore.WithProfile(httpstore.TorqueFederationProfile()))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	env := notice(messaging.Address{Kind: messaging.KindAgent, Authority: "peer-a", ID: "agent-1"},
		messaging.Address{Kind: messaging.KindAgent, Authority: "peer-b", ID: "agent-2"})
	env.ThreadID = "thread-xyz"
	sent, err := s.Send(ctx, env)
	if err != nil || sent.ID == "" {
		t.Fatalf("Send: %+v, %v", sent, err)
	}
	if req := rc.last(t); req.Method != "POST" || req.Path != "/federation/v1/messages" {
		t.Errorf("Send went to %s %s", req.Method, req.Path)
	}
	got, err := s.Get(ctx, sent.ID)
	if err != nil || got.ID != sent.ID || got.ThreadID != "thread-xyz" {
		t.Fatalf("Get: %+v, %v", got, err)
	}
	if req := rc.last(t); len(req.Query) != 0 {
		t.Errorf("Torque Get carried query %v; identity is the client certificate, not a parameter", req.Query)
	}
	th, err := s.Thread(ctx, "thread-xyz", messaging.Filter{})
	if err != nil || len(th) != 1 || th[0].ID != sent.ID {
		t.Fatalf("Thread: %+v, %v", th, err)
	}
	if err := s.Consume(ctx, sent.ID, env.To); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if req := rc.last(t); req.Path != "/federation/v1/messages/"+sent.ID+"/consume" ||
		string(req.Body) != `{"recipient":"`+env.To.URN()+`"}` || req.Query.Get("as") != "" {
		t.Errorf("Consume request = %s %s body=%s query=%v", req.Method, req.Path, req.Body, req.Query)
	}
	if err := s.Cancel(ctx, sent.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if got, _ := ms.Get(ctx, sent.ID); got.ConsumedAt == nil {
		t.Error("backing store did not record the consume")
	}
}

func TestScenario_TorqueGetNotFound(t *testing.T) {
	s, _ := newStore(t, httpstore.TorqueFederationProfile(), nil)
	if _, err := s.Get(context.Background(), "does-not-exist"); !errors.Is(err, messaging.ErrNotFound) {
		t.Fatalf("Get of an absent envelope: got %v, want ErrNotFound", err)
	}
}

func TestScenario_TorqueSendPresetLifecycle(t *testing.T) {
	srv, rc := stub(t, http.StatusCreated, "", "")
	s, err := httpstore.New(srv.URL, httpstore.WithProfile(httpstore.TorqueFederationProfile()))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	e := notice(alice, bob)
	e.DeliveredAt = &now
	if _, err := s.Send(context.Background(), e); !errors.Is(err, messaging.ErrPresetLifecycle) {
		t.Fatalf("got %v, want ErrPresetLifecycle", err)
	}
	if rc.count() != 0 {
		t.Fatal("a preset lifecycle field must not reach the peer")
	}
}

func TestScenario_TorqueUnreachablePeer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close() // nothing listens now
	s, err := httpstore.New(url, httpstore.WithProfile(httpstore.TorqueFederationProfile()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(context.Background(), "id"); !errors.Is(err, messaging.ErrStoreUnavailable) {
		t.Fatalf("Get against a dead peer: got %v, want ErrStoreUnavailable", err)
	}
}

func TestScenario_TorqueNewValidation(t *testing.T) {
	for _, bad := range []string{"", "   ", "peer.example:8443", "ftp://peer.example", "https://", "/just/a/path",
		"https://peer.example?x=1", "https://peer.example#frag"} {
		if _, err := httpstore.New(bad, httpstore.WithProfile(httpstore.TorqueFederationProfile())); err == nil {
			t.Errorf("New(%q): want error, got nil", bad)
		}
	}
	for _, ok := range []string{"https://peer.example:8443", "http://127.0.0.1:1", "https://peer.example/prefix/", "http://unix"} {
		if _, err := httpstore.New(ok); err != nil {
			t.Errorf("New(%q): unexpected error %v", ok, err)
		}
	}
}
