package httpstoretest_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	messaging "github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/httpstore"
	"github.com/hollis-labs/go-messaging/httpstore/httpstoretest"
	"github.com/hollis-labs/go-messaging/memstore"
)

var (
	alice = messaging.Address{Kind: messaging.KindAgent, Authority: "t", ID: "alice"}
	bob   = messaging.Address{Kind: messaging.KindAgent, Authority: "t", ID: "bob"}
	carol = messaging.Address{Kind: messaging.KindAgent, Authority: "t", ID: "carol"}
)

func do(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// seeded returns a server (over a memstore holding one alice->bob envelope in
// thread T) and that envelope's id.
func seeded(t *testing.T, p httpstore.Profile, opts ...httpstoretest.Option) (base, id string) {
	t.Helper()
	ms := memstore.New()
	e, err := ms.Send(context.Background(), messaging.Envelope{Kind: messaging.MsgKindNotice, From: alice, To: bob, ThreadID: "T"})
	if err != nil {
		t.Fatal(err)
	}
	srv := httpstoretest.NewServer(t, ms, nil, p, opts...)
	return srv.URL + "/" + strings.Trim(p.BasePath, "/"), e.ID
}

func q(v ...string) string {
	u := url.Values{}
	for i := 0; i+1 < len(v); i += 2 {
		u.Add(v[i], v[i+1])
	}
	return u.Encode()
}

func TestStrictIdentity_TetherRules(t *testing.T) {
	base, id := seeded(t, httpstore.TetherProfile(), httpstoretest.WithStrictIdentity())
	cases := []struct {
		name, method, path, body string
		want                     int
	}{
		{"get without as", "GET", "/" + id, "", 400},
		{"get as a stranger", "GET", "/" + id + "?" + q("as", carol.URN()), "", 403},
		{"get as the sender", "GET", "/" + id + "?" + q("as", alice.URN()), "", 200},
		{"get as the recipient", "GET", "/" + id + "?" + q("as", bob.URN()), "", 200},
		{"get unknown id reaches 404 before 403", "GET", "/nope?" + q("as", carol.URN()), "", 404},
		{"inbox without to", "GET", "/inbox?" + q("as", bob.URN()), "", 400},
		{"inbox without as", "GET", "/inbox?" + q("to", bob.URN()), "", 400},
		{"inbox as someone else", "GET", "/inbox?" + q("to", bob.URN(), "as", alice.URN()), "", 403},
		{"inbox with a bad urn", "GET", "/inbox?" + q("to", "x", "as", "x"), "", 400},
		{"thread without as", "GET", "/thread/T", "", 400},
		{"thread with as", "GET", "/thread/T?" + q("as", alice.URN()), "", 200},
		{"consume without as", "POST", "/" + id + "/consume", "", 400},
		{"consume as the wrong recipient", "POST", "/" + id + "/consume?" + q("as", carol.URN()), "", 409},
		{"consume unknown id", "POST", "/nope/consume?" + q("as", bob.URN()), "", 404},
		{"consume as the recipient", "POST", "/" + id + "/consume?" + q("as", bob.URN()), "", 204},
		{"cancel needs no as", "POST", "/" + id + "/cancel", "", 204},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, body := do(t, tc.method, base+tc.path, tc.body); got != tc.want {
				t.Errorf("status = %d, want %d (%s)", got, tc.want, body)
			}
		})
	}
	t.Run("subscribe without as", func(t *testing.T) {
		if got, _ := do(t, "GET", base+"/subscribe?"+q("to", bob.URN()), ""); got != 400 {
			t.Errorf("status = %d, want 400", got)
		}
	})
	t.Run("subscribe as someone else", func(t *testing.T) {
		if got, _ := do(t, "GET", base+"/subscribe?"+q("to", bob.URN(), "as", alice.URN()), ""); got != 403 {
			t.Errorf("status = %d, want 403", got)
		}
	})
}

func TestLenientServerIgnoresIdentityClaims(t *testing.T) {
	base, id := seeded(t, httpstore.TetherProfile())
	for _, tc := range []struct{ method, path string }{
		{"GET", "/" + id},
		{"GET", "/" + id + "?" + q("as", carol.URN())},
		{"GET", "/inbox?" + q("to", bob.URN())},
		{"GET", "/thread/T"},
		{"POST", "/" + id + "/cancel"},
	} {
		if got, body := do(t, tc.method, base+tc.path, ""); got != 200 && got != 204 {
			t.Errorf("%s %s = %d (%s), want success", tc.method, tc.path, got, body)
		}
	}
}

func TestStrictThreadIsScopedToParties(t *testing.T) {
	base, _ := seeded(t, httpstore.TetherProfile(), httpstoretest.WithStrictIdentity())
	count := func(as messaging.Address) int {
		_, body := do(t, "GET", base+"/thread/T?"+q("as", as.URN()), "")
		var out struct{ Messages []messaging.Envelope }
		if err := json.Unmarshal([]byte(body), &out); err != nil {
			t.Fatal(err)
		}
		return len(out.Messages)
	}
	if n := count(alice); n != 1 {
		t.Errorf("party sees %d", n)
	}
	if n := count(carol); n != 0 {
		t.Errorf("stranger sees %d, want a thinned (empty) thread", n)
	}
}

// Tether's handlers ignore these parameters; so must the reference server,
// or a client that leaned on them would pass here and fail in production.
func TestTetherProfileIgnoresInboxLimitAndChannel(t *testing.T) {
	ms := memstore.New()
	for i := 0; i < 3; i++ {
		if _, err := ms.Send(context.Background(), messaging.Envelope{Kind: messaging.MsgKindNotice, Channel: "chat", From: alice, To: bob}); err != nil {
			t.Fatal(err)
		}
	}
	srv := httpstoretest.NewServer(t, ms, nil, httpstore.TetherProfile())
	_, body := do(t, "GET", srv.URL+"/messages/inbox?"+q("to", bob.URN(), "limit", "1", "channel", "other"), "")
	var out struct{ Messages []messaging.Envelope }
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Messages) != 3 {
		t.Errorf("inbox returned %d, want all 3 (limit and channel ignored)", len(out.Messages))
	}
}

// A profile that does not assert identity honours limit, channel and repeated kind.
func TestFilteringOnANonAssertingProfile(t *testing.T) {
	ms := memstore.New()
	for _, k := range []messaging.Kind{messaging.MsgKindNotice, messaging.MsgKindRequest, messaging.MsgKindHandoff} {
		if _, err := ms.Send(context.Background(), messaging.Envelope{Kind: k, Channel: "chat", From: alice, To: bob, ThreadID: "T"}); err != nil {
			t.Fatal(err)
		}
	}
	p := httpstore.TorqueFederationProfile()
	srv := httpstoretest.NewServer(t, ms, nil, p)
	get := func(query string) int {
		_, body := do(t, "GET", srv.URL+p.BasePath+"/thread/T?"+query, "")
		var out struct{ Messages []messaging.Envelope }
		if err := json.Unmarshal([]byte(body), &out); err != nil {
			t.Fatal(err)
		}
		return len(out.Messages)
	}
	if n := get("kind=notice&kind=request"); n != 2 {
		t.Errorf("repeated kind: %d, want 2", n)
	}
	if n := get("kind=notice,request"); n != 0 {
		t.Errorf("comma kind on a repeated profile: %d, want 0 (one unknown kind)", n)
	}
	if n := get("channel=other"); n != 0 {
		t.Errorf("channel filter: %d, want 0", n)
	}
	if n := get("limit=1"); n != 1 {
		t.Errorf("limit: %d, want 1", n)
	}
}

func TestUnsupportedRoutesAnswer404NotAnotherRoute(t *testing.T) {
	p := httpstore.TorqueFederationProfile()
	base, _ := seeded(t, p)
	for _, path := range []string{"/inbox?" + q("to", bob.URN()), "/subscribe?" + q("to", bob.URN())} {
		got, body := do(t, "GET", base+path, "")
		if got != 404 {
			t.Errorf("GET %s = %d (%s); an unmounted route must be 404, not served as Get(id)", path, got, body)
		}
		if !strings.HasPrefix(body, `{"error":"`) {
			t.Errorf("body %q is not Torque's flat error shape", body)
		}
	}
	if got, _ := do(t, "POST", base+"/request", "{}"); got != 404 {
		t.Errorf("POST /request on a non-blocking profile = %d, want 404", got)
	}
}

func TestErrorShapes(t *testing.T) {
	tb, _ := seeded(t, httpstore.TetherProfile())
	_, body := do(t, "GET", tb+"/nope", "")
	var nested struct {
		Error struct{ Code, Message string }
	}
	if err := json.Unmarshal([]byte(body), &nested); err != nil || nested.Error.Code != "not_found" || nested.Error.Message == "" {
		t.Errorf("Tether body %q is not the nested shape", body)
	}
	fb, _ := seeded(t, httpstore.TorqueFederationProfile())
	_, body = do(t, "GET", fb+"/nope", "")
	var flat struct{ Error string }
	if err := json.Unmarshal([]byte(body), &flat); err != nil || flat.Error == "" {
		t.Errorf("Torque body %q is not the flat shape", body)
	}
}

func TestTorqueConsumeNeedsRecipientInBody(t *testing.T) {
	base, id := seeded(t, httpstore.TorqueFederationProfile())
	if got, _ := do(t, "POST", base+"/"+id+"/consume", ""); got != 422 {
		t.Errorf("no body = %d, want 422", got)
	}
	if got, _ := do(t, "POST", base+"/"+id+"/consume", `{"recipient":""}`); got != 422 {
		t.Errorf("empty recipient = %d, want 422", got)
	}
	if got, _ := do(t, "POST", base+"/"+id+"/consume", `{"recipient":"`+bob.URN()+`"}`); got != 204 {
		t.Errorf("valid body = %d, want 204", got)
	}
}

func TestSendRules(t *testing.T) {
	base, _ := seeded(t, httpstore.TetherProfile())
	if got, _ := do(t, "POST", base, "{not json"); got != 400 {
		t.Errorf("bad JSON = %d, want 400", got)
	}
	got, body := do(t, "POST", base, `{"kind":"notice","from":"`+alice.URN()+`","to":"`+bob.URN()+`","delivered_at":"2026-01-01T00:00:00Z"}`)
	if got != 422 || !strings.Contains(body, "preset_lifecycle") {
		t.Errorf("preset lifecycle = %d %s, want 422 preset_lifecycle", got, body)
	}
	got, body = do(t, "POST", base, `{"id":"mine","kind":"notice","from":"`+alice.URN()+`","to":"`+bob.URN()+`"}`)
	if got != 201 || strings.Contains(body, `"id":"mine"`) {
		t.Errorf("send = %d %s; the server must assign the id", got, body)
	}
}

func TestBlockingRequestRoute(t *testing.T) {
	ms := memstore.New()
	srv := httpstoretest.NewServer(t, ms, nil, httpstore.TetherProfile())
	start := time.Now()
	got, body := do(t, "POST", srv.URL+"/messages/request?timeout=100ms",
		`{"from":"`+alice.URN()+`","to":"`+bob.URN()+`"}`)
	if got != 504 || !strings.Contains(body, `"code":"timeout"`) {
		t.Errorf("no responder = %d %s, want 504 timeout", got, body)
	}
	if time.Since(start) > 5*time.Second {
		t.Error("the timeout parameter was not honoured")
	}
	if got, _ := do(t, "POST", srv.URL+"/messages/request", "nope"); got != 400 {
		t.Errorf("bad body = %d, want 400", got)
	}
	if got, _ := do(t, "POST", srv.URL+"/messages/request?timeout=soon", "{}"); got != 400 {
		t.Errorf("bad timeout = %d, want 400", got)
	}
}

func TestNewServerClosesWithTheTest(t *testing.T) {
	var url string
	t.Run("inner", func(t *testing.T) {
		url = httpstoretest.NewServer(t, memstore.New(), nil, httpstore.TetherProfile()).URL
		if got, _ := do(t, "GET", url+"/messages/x?as=y", ""); got != 404 {
			t.Errorf("status = %d", got)
		}
	})
	if _, err := http.Get(url + "/messages/x"); err == nil {
		t.Error("server still up after the test that created it ended")
	}
}

func TestHandlerServesFromCustomBasePath(t *testing.T) {
	p := httpstore.Profile{Name: "custom", BasePath: "/api/v2/msgs/"}
	srv := httptest.NewServer(httpstoretest.Handler(memstore.New(), nil, p))
	t.Cleanup(srv.Close)
	if got, _ := do(t, "GET", srv.URL+"/api/v2/msgs/inbox?"+q("to", bob.URN()), ""); got != 200 {
		t.Errorf("status = %d", got)
	}
	if got, _ := do(t, "GET", srv.URL+"/messages/inbox?"+q("to", bob.URN()), ""); got != 404 {
		t.Errorf("the default prefix should not be served: %d", got)
	}
}
