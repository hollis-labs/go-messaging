package httpstore_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"sync"
	"testing"
	"time"
)

// recorded is one request as the server saw it.
type recorded struct {
	Method  string
	Path    string // escaped path, as on the wire
	Query   url.Values
	RawQ    string
	Body    []byte
	Header  http.Header
	HadCert bool
}

// recorder wraps a handler and records every request it serves.
type recorder struct {
	mu   sync.Mutex
	reqs []recorded
}

func (rc *recorder) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		rc.mu.Lock()
		rc.reqs = append(rc.reqs, recorded{
			Method:  r.Method,
			Path:    r.URL.EscapedPath(),
			Query:   r.URL.Query(),
			RawQ:    r.URL.RawQuery,
			Body:    body,
			Header:  r.Header.Clone(),
			HadCert: r.TLS != nil && len(r.TLS.PeerCertificates) > 0,
		})
		rc.mu.Unlock()
		next.ServeHTTP(w, r)
	})
}

func (rc *recorder) all() []recorded {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return append([]recorded(nil), rc.reqs...)
}

func (rc *recorder) last(t *testing.T) recorded {
	t.Helper()
	all := rc.all()
	if len(all) == 0 {
		t.Fatal("server saw no request")
	}
	return all[len(all)-1]
}

func (rc *recorder) count() int { return len(rc.all()) }

// stub starts a server that answers every request with fixed status/body
// and records what it was asked.
func stub(t *testing.T, status int, contentType, body string) (*httptest.Server, *recorder) {
	t.Helper()
	rc := &recorder{}
	srv := httptest.NewServer(rc.wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	})))
	t.Cleanup(srv.Close)
	return srv, rc
}

// noKeepAlive is a client whose connections die with the request, so
// goroutine counts settle without idle-connection noise.
func noKeepAlive() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DisableKeepAlives = true
	return &http.Client{Transport: tr}
}

// waitFor polls cond for up to 3 s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func goroutines() int { return runtime.NumGoroutine() }

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }
