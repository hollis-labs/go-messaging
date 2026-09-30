package httpstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	messaging "github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/httpstore"
	"github.com/hollis-labs/go-messaging/httpstore/httpstoretest"
	"github.com/hollis-labs/go-messaging/memstore"
)

// responder answers the first request addressed to `to` on ms after delay.
func responder(t *testing.T, ms *memstore.Store, to messaging.Address, delay time.Duration, payload string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sub, err := ms.Subscribe(ctx, to, messaging.Filter{Kind: []messaging.Kind{messaging.MsgKindRequest}})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for req := range sub {
			time.Sleep(delay)
			_, _ = messaging.NewDispatcher(ms).Reply(ctx, req, json.RawMessage(payload))
			return
		}
	}()
}

func TestDispatcher_BlockingRequest(t *testing.T) {
	ms := memstore.New()
	rc := &recorder{}
	srv := httptest.NewServer(rc.wrap(httpstoretest.Handler(ms, nil, httpstore.TetherProfile())))
	t.Cleanup(srv.Close)
	d, err := httpstore.NewDispatcher(srv.URL, httpstore.WithIdentity(alice))
	if err != nil {
		t.Fatal(err)
	}
	responder(t, ms, bob, 0, `"pong"`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	e := notice(alice, bob) // Kind is forced to request
	e.ID = "caller-chosen"
	resp, err := d.Request(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	if string(resp.Payload) != `"pong"` || resp.Kind != messaging.MsgKindResponse {
		t.Errorf("response = %+v", resp)
	}

	var reqs []recorded
	for _, r := range rc.all() {
		reqs = append(reqs, r)
		if r.Path == "/messages/subscribe" {
			t.Error("a blocking Request must be one POST, not Subscribe-then-Send")
		}
	}
	var post recorded
	for _, r := range reqs {
		if r.Path == "/messages/request" {
			post = r
		}
	}
	if post.Method != http.MethodPost {
		t.Fatalf("no POST /messages/request among %d requests", len(reqs))
	}
	d1, err := time.ParseDuration(post.Query.Get("timeout"))
	if err != nil || d1 <= 0 || d1 > 5*time.Second {
		t.Errorf("timeout = %q (%v), want the remaining deadline as a Go duration", post.Query.Get("timeout"), err)
	}
	var sent messaging.Envelope
	if err := json.Unmarshal(post.Body, &sent); err != nil {
		t.Fatal(err)
	}
	if sent.Kind != messaging.MsgKindRequest || sent.ID != "" {
		t.Errorf("wire request kind=%q id=%q; want request and no id", sent.Kind, sent.ID)
	}
}

// The blocking request is not cut off by a client Timeout or WithTimeout.
func TestDispatcher_BlockingRequestOutlivesClientTimeouts(t *testing.T) {
	ms := memstore.New()
	srv := httpstoretest.NewServer(t, ms, nil, httpstore.TetherProfile())
	d, err := httpstore.NewDispatcher(srv.URL, httpstore.WithIdentity(alice),
		httpstore.WithHTTPClient(&http.Client{Timeout: 100 * time.Millisecond}), httpstore.WithTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	responder(t, ms, bob, 400*time.Millisecond, `"late"`)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := d.Request(ctx, notice(alice, bob))
	if err != nil || string(resp.Payload) != `"late"` {
		t.Fatalf("got %+v, %v", resp, err)
	}
}

func TestDispatcher_RequestTimeout(t *testing.T) {
	t.Run("deadline passes with no responder", func(t *testing.T) {
		srv := httpstoretest.NewServer(t, memstore.New(), nil, httpstore.TetherProfile())
		d, _ := httpstore.NewDispatcher(srv.URL, httpstore.WithIdentity(alice))
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		if _, err := d.Request(ctx, notice(alice, bob)); !errors.Is(err, messaging.ErrRequestTimeout) {
			t.Errorf("got %v, want ErrRequestTimeout", err)
		}
	})
	t.Run("server 504 without a client deadline", func(t *testing.T) {
		srv, _ := stub(t, http.StatusGatewayTimeout, "application/json", `{"error":{"code":"request_timeout","message":"request timed out"}}`)
		d, _ := httpstore.NewDispatcher(srv.URL, httpstore.WithIdentity(alice))
		_, err := d.Request(context.Background(), notice(alice, bob))
		var se *httpstore.StatusError
		if !errors.Is(err, messaging.ErrRequestTimeout) || !errors.As(err, &se) || se.Code != 504 {
			t.Errorf("got %v", err)
		}
	})
	t.Run("already expired context sends nothing", func(t *testing.T) {
		srv, rc := stub(t, http.StatusOK, "application/json", "{}")
		d, _ := httpstore.NewDispatcher(srv.URL, httpstore.WithIdentity(alice))
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		if _, err := d.Request(ctx, notice(alice, bob)); !errors.Is(err, messaging.ErrRequestTimeout) {
			t.Errorf("got %v", err)
		}
		if rc.count() != 0 {
			t.Error("an expired context still sent a request")
		}
	})
	t.Run("cancellation is not a timeout", func(t *testing.T) {
		srv := httpstoretest.NewServer(t, memstore.New(), nil, httpstore.TetherProfile())
		d, _ := httpstore.NewDispatcher(srv.URL, httpstore.WithIdentity(alice))
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(50*time.Millisecond, cancel)
		_, err := d.Request(ctx, notice(alice, bob))
		if errors.Is(err, messaging.ErrRequestTimeout) || !errors.Is(err, context.Canceled) {
			t.Errorf("got %v, want context.Canceled and not ErrRequestTimeout", err)
		}
	})
}

func TestDispatcher_PresetLifecycleRejectedClientSide(t *testing.T) {
	srv, rc := stub(t, http.StatusOK, "application/json", "{}")
	d, _ := httpstore.NewDispatcher(srv.URL, httpstore.WithIdentity(alice))
	now := time.Now()
	e := notice(alice, bob)
	e.ConsumedAt = &now
	if _, err := d.Request(context.Background(), e); !errors.Is(err, messaging.ErrPresetLifecycle) {
		t.Errorf("got %v", err)
	}
	if rc.count() != 0 {
		t.Error("request reached the server")
	}
}

// Without BlockingRequest, Request is the generic Subscribe-then-Send helper
// over the same Store.
func TestDispatcher_GenericRequestWhenNotBlocking(t *testing.T) {
	p := httpstore.Profile{Name: "generic"}
	ms := memstore.New()
	rc := &recorder{}
	srv := httptest.NewServer(rc.wrap(httpstoretest.Handler(ms, nil, p)))
	t.Cleanup(srv.Close)
	d, err := httpstore.NewDispatcher(srv.URL, httpstore.WithProfile(p))
	if err != nil {
		t.Fatal(err)
	}
	responder(t, ms, bob, 0, `"pong"`)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := d.Request(ctx, notice(alice, bob))
	if err != nil || string(resp.Payload) != `"pong"` {
		t.Fatalf("got %+v, %v", resp, err)
	}
	var sawSubscribe bool
	for _, r := range rc.all() {
		if r.Path == "/messages/subscribe" {
			sawSubscribe = true
		}
		if r.Path == "/messages/request" {
			t.Error("a non-blocking profile must not use /request")
		}
	}
	if !sawSubscribe {
		t.Error("generic Request should subscribe for the response")
	}
}

func TestDispatcher_UnsupportedRequest(t *testing.T) {
	t.Run("torque profile has no Subscribe to wait on", func(t *testing.T) {
		srv, rc := stub(t, http.StatusOK, "application/json", "{}")
		d, _ := httpstore.NewDispatcher(srv.URL, httpstore.WithProfile(httpstore.TorqueFederationProfile()))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := d.Request(ctx, notice(alice, bob))
		if !errors.Is(err, httpstore.ErrUnsupported) || !errors.Is(err, messaging.ErrStoreUnavailable) {
			t.Errorf("got %v", err)
		}
		if rc.count() != 0 {
			t.Error("Request sent something")
		}
	})
	t.Run("OpRequest listed unsupported", func(t *testing.T) {
		p := httpstore.TetherProfile()
		p.Unsupported = []httpstore.Op{httpstore.OpRequest}
		srv, _ := stub(t, http.StatusOK, "application/json", "{}")
		d, _ := httpstore.NewDispatcher(srv.URL, httpstore.WithProfile(p), httpstore.WithIdentity(alice))
		if _, err := d.Request(context.Background(), notice(alice, bob)); !errors.Is(err, httpstore.ErrUnsupported) {
			t.Errorf("got %v", err)
		}
	})
}

func TestDispatcher_Reply(t *testing.T) {
	ms := memstore.New()
	srv := httpstoretest.NewServer(t, ms, nil, httpstore.TetherProfile())
	d, _ := httpstore.NewDispatcher(srv.URL, httpstore.WithIdentity(alice))
	ctx := context.Background()
	req := notice(alice, bob)
	req.ThreadID = "T"
	req.Kind = messaging.MsgKindRequest
	parent, err := d.Send(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := d.Reply(ctx, parent, json.RawMessage(`"ok"`))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Kind != messaging.MsgKindResponse || resp.From != bob || resp.To != alice || resp.InReplyTo != parent.ID || resp.ThreadID != "T" {
		t.Errorf("reply = %+v", resp)
	}
}

func TestNewDispatcherValidatesURL(t *testing.T) {
	if _, err := httpstore.NewDispatcher("ftp://x"); err == nil {
		t.Error("want an error for a non-http scheme")
	}
}
