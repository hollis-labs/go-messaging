package httpstore_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	messaging "github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/httpstore"
	"github.com/hollis-labs/go-messaging/httpstore/httpstoretest"
	"github.com/hollis-labs/go-messaging/memstore"
)

var fullFilter = messaging.Filter{
	Kind:     []messaging.Kind{messaging.MsgKindRequest, messaging.MsgKindNotice},
	Channel:  []messaging.Channel{"chat", "alert"},
	ThreadID: "T1",
	Limit:    5,
}

// emptyList answers any request with an empty {"messages":[]}.
func emptyList(t *testing.T) (*httptest.Server, *recorder) {
	t.Helper()
	return stub(t, http.StatusOK, "application/json", `{"messages":[]}`)
}

func TestEncoding_TetherCommaJoined(t *testing.T) {
	srv, rc := emptyList(t)
	s, err := httpstore.New(srv.URL, httpstore.WithIdentity(alice)) // Tether profile
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if _, err := s.Inbox(ctx, bob, fullFilter); err != nil {
		t.Fatal(err)
	}
	q := rc.last(t).Query
	// One kind parameter holding a comma list, not two parameters.
	if got := q["kind"]; !reflect.DeepEqual(got, []string{"request,notice"}) {
		t.Errorf("kind = %q, want one comma-joined value", got)
	}
	// Channel is sent (harmless: Tether ignores it today), same style.
	if got := q["channel"]; !reflect.DeepEqual(got, []string{"chat,alert"}) {
		t.Errorf("channel = %q, want one comma-joined value", got)
	}
	for k, want := range map[string]string{"thread_id": "T1", "limit": "5", "to": bob.URN(), "as": bob.URN()} {
		if got := q.Get(k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}

	if _, err := s.Thread(ctx, "T1", fullFilter); err != nil {
		t.Fatal(err)
	}
	req := rc.last(t)
	if req.Path != "/messages/thread/T1" {
		t.Errorf("thread path = %q", req.Path)
	}
	if !reflect.DeepEqual(req.Query["kind"], []string{"request,notice"}) || req.Query.Get("limit") != "5" || req.Query.Get("as") != alice.URN() {
		t.Errorf("thread query = %v", req.Query)
	}
}

func TestEncoding_TorqueRepeated(t *testing.T) {
	srv, rc := emptyList(t)
	s, err := httpstore.New(srv.URL, httpstore.WithProfile(httpstore.TorqueFederationProfile()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Thread(context.Background(), "T1", fullFilter); err != nil {
		t.Fatal(err)
	}
	req := rc.last(t)
	if req.Path != "/federation/v1/messages/thread/T1" {
		t.Errorf("path = %q", req.Path)
	}
	if got := req.Query["kind"]; !reflect.DeepEqual(got, []string{"request", "notice"}) {
		t.Errorf("kind = %q, want repeated parameters", got)
	}
	if got := req.Query["channel"]; !reflect.DeepEqual(got, []string{"chat", "alert"}) {
		t.Errorf("channel = %q, want repeated parameters", got)
	}
	if req.Query.Get("limit") != "5" || req.Query.Get("thread_id") != "T1" {
		t.Errorf("query = %v", req.Query)
	}
	if _, has := req.Query["as"]; has {
		t.Errorf("Torque profile must not send as: %v", req.Query)
	}
}

// Repeated encoding also applies to Inbox/Subscribe on a profile that has them.
func TestEncoding_RepeatedOnInboxAndSubscribe(t *testing.T) {
	p := httpstore.Profile{Name: "repeated", KindsRepeated: true}
	srv, rc := emptyList(t)
	s, err := httpstore.New(srv.URL, httpstore.WithProfile(p))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Inbox(context.Background(), bob, fullFilter); err != nil {
		t.Fatal(err)
	}
	if got := rc.last(t).Query["kind"]; !reflect.DeepEqual(got, []string{"request", "notice"}) {
		t.Errorf("inbox kind = %q", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The stub answers 200 with a JSON body and no stream; the stream just ends.
	ch, err := s.Subscribe(ctx, bob, fullFilter)
	if err != nil {
		t.Fatal(err)
	}
	for range ch {
	}
	q := rc.last(t).Query
	if !reflect.DeepEqual(q["kind"], []string{"request", "notice"}) || !reflect.DeepEqual(q["channel"], []string{"chat", "alert"}) {
		t.Errorf("subscribe query = %v", q)
	}
	if _, has := q["limit"]; has {
		t.Errorf("Subscribe must not send limit: %v", q)
	}
	if q.Get("thread_id") != "T1" {
		t.Errorf("subscribe thread_id = %q", q.Get("thread_id"))
	}
	if _, has := q["as"]; has {
		t.Errorf("a profile without AssertAs must not send as: %v", q)
	}
}

func TestEncoding_EmptyFilterSendsNothingExtra(t *testing.T) {
	srv, rc := emptyList(t)
	s, err := httpstore.New(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	f := messaging.Filter{Kind: []messaging.Kind{""}, Channel: []messaging.Channel{""}}
	if _, err := s.Inbox(context.Background(), bob, f); err != nil {
		t.Fatal(err)
	}
	q := rc.last(t).Query
	for _, k := range []string{"kind", "channel", "thread_id", "limit"} {
		if _, has := q[k]; has {
			t.Errorf("empty filter sent %s=%q", k, q[k])
		}
	}
}

// IDs are path-escaped: a hostile id cannot add path segments or a query.
func TestEncoding_IDsArePathEscaped(t *testing.T) {
	srv, rc := stub(t, http.StatusNoContent, "", "")
	s, err := httpstore.New(srv.URL, httpstore.WithIdentity(alice))
	if err != nil {
		t.Fatal(err)
	}
	const id = "a b?c#d%e/f"
	if err := s.Cancel(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	req := rc.last(t)
	if want := "/messages/a%20b%3Fc%23d%25e%2Ff/cancel"; req.Path != want {
		t.Errorf("path = %q, want %q", req.Path, want)
	}
	if req.RawQ != "" {
		t.Errorf("id leaked into the query: %q", req.RawQ)
	}
}

// The client must never truncate an Inbox result. The server has already
// marked every envelope it returned as delivered, so cutting the list would
// lose the rest for good. Tether's inbox ignores limit and returns them all.
func TestInbox_NeverTruncatedClientSide(t *testing.T) {
	// Against a stub that ignores limit and returns three.
	body := `{"messages":[{"id":"1","kind":"notice"},{"id":"2","kind":"notice"},{"id":"3","kind":"notice"}]}`
	srv, rc := stub(t, http.StatusOK, "application/json", body)
	for _, p := range []httpstore.Profile{httpstore.TetherProfile(), {Name: "repeated", KindsRepeated: true}} {
		s, err := httpstore.New(srv.URL, httpstore.WithProfile(p))
		if err != nil {
			t.Fatal(err)
		}
		got, err := s.Inbox(context.Background(), bob, messaging.Filter{Limit: 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 {
			t.Errorf("profile %s: Inbox with Limit=1 returned %d envelopes, want all 3 the server returned", p.Name, len(got))
		}
	}
	if rc.last(t).Query.Get("limit") != "1" {
		t.Error("the limit should still be sent to the server")
	}

	// Against the reference server (a Tether-faithful inbox that ignores
	// limit): everything it marked delivered came back.
	ms := memstore.New()
	rsrv := httpstoretest.NewServer(t, ms, nil, httpstore.TetherProfile())
	s, err := httpstore.New(rsrv.URL, httpstore.WithIdentity(alice))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.Send(context.Background(), notice(alice, bob)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Inbox(context.Background(), bob, messaging.Filter{Limit: 1})
	if err != nil || len(got) != 3 {
		t.Fatalf("Inbox = %d envelopes, %v; want all 3", len(got), err)
	}
	if again, _ := s.Inbox(context.Background(), bob, messaging.Filter{}); len(again) != 0 {
		t.Errorf("second Inbox returned %d; the first must have drained everything it marked delivered", len(again))
	}
}

// Send clears the server-assigned fields before the request.
func TestSend_ClearsServerAssignedFields(t *testing.T) {
	srv, rc := stub(t, http.StatusCreated, "application/json", `{"id":"srv-1","kind":"notice"}`)
	s, err := httpstore.New(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	e := notice(alice, bob)
	e.ID = "caller-chosen"
	e.CreatedAt = e.CreatedAt.AddDate(2000, 0, 0)
	if _, err := s.Send(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	body := string(rc.last(t).Body)
	if want := `"id":""`; !strings.Contains(body, want) {
		t.Errorf("body %s should carry an empty id", body)
	}
	if want := `"created_at":"0001-01-01T00:00:00Z"`; !strings.Contains(body, want) {
		t.Errorf("body %s should carry a zero created_at", body)
	}
}
