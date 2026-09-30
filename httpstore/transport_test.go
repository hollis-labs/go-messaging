package httpstore_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	messaging "github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/httpstore"
	"github.com/hollis-labs/go-messaging/httpstore/httpstoretest"
	"github.com/hollis-labs/go-messaging/memstore"
)

// clientCert makes a throwaway self-signed client certificate. The tests
// below use only the standard library's crypto/tls; pinning and peer
// authorization are not this package's job.
func clientCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "peer-a"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// TestMTLSSeam shows WithHTTPClient is enough to run the store over mutual
// TLS: the server demands a client certificate, the store is handed a client
// that has one, and nothing in httpstore knows.
func TestMTLSSeam(t *testing.T) {
	var mu sync.Mutex
	var seen [][]byte // client leaf certificates the server observed
	inner := httpstoretest.Handler(memstore.New(), nil, httpstore.TorqueFederationProfile())
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			mu.Lock()
			seen = append(seen, r.TLS.PeerCertificates[0].Raw)
			mu.Unlock()
		}
		inner.ServeHTTP(w, r)
	}))
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAnyClientCert, MinVersion: tls.VersionTLS12}
	srv.Config.ErrorLog = log.New(io.Discard, "", 0) // failed handshakes are the point
	srv.StartTLS()
	t.Cleanup(srv.Close)

	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	cert := clientCert(t)
	mtls := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool, MinVersion: tls.VersionTLS12},
	}}
	t.Cleanup(mtls.CloseIdleConnections)

	s, err := httpstore.New(srv.URL, httpstore.WithProfile(httpstore.TorqueFederationProfile()), httpstore.WithHTTPClient(mtls))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	sent, err := s.Send(ctx, notice(alice, bob))
	if err != nil {
		t.Fatalf("Send over mTLS: %v", err)
	}
	if _, err := s.Get(ctx, sent.ID); err != nil {
		t.Fatalf("Get over mTLS: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) == 0 || !bytes.Equal(seen[0], cert.Certificate[0]) {
		t.Errorf("server did not observe the client certificate the store's client presented (saw %d)", len(seen))
	}

	// Without a client certificate the handshake fails, and that reads as
	// an unavailable store, not a missing envelope.
	noCert := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}
	t.Cleanup(noCert.CloseIdleConnections)
	bare, err := httpstore.New(srv.URL, httpstore.WithProfile(httpstore.TorqueFederationProfile()), httpstore.WithHTTPClient(noCert))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bare.Get(ctx, sent.ID); !errors.Is(err, messaging.ErrStoreUnavailable) || errors.Is(err, messaging.ErrNotFound) {
		t.Errorf("Get without a client certificate: got %v, want ErrStoreUnavailable", err)
	}
	// Nor does the default client trust the server's certificate.
	def, err := httpstore.New(srv.URL, httpstore.WithIdentity(alice))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := def.Get(ctx, sent.ID); !errors.Is(err, messaging.ErrStoreUnavailable) {
		t.Errorf("Get with an untrusting client: got %v, want ErrStoreUnavailable", err)
	}
}

// The stream client is a copy of the one you supply: it must keep the TLS
// transport, or a live subscription would silently lose mTLS.
func TestMTLSSeam_SubscribeKeepsTransport(t *testing.T) {
	ms := memstore.New()
	srv := httptest.NewUnstartedServer(httpstoretest.Handler(ms, nil, httpstore.TetherProfile()))
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAnyClientCert, MinVersion: tls.VersionTLS12}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	mtls := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{Certificates: []tls.Certificate{clientCert(t)}, RootCAs: pool, MinVersion: tls.VersionTLS12},
	}}
	t.Cleanup(mtls.CloseIdleConnections)
	s, err := httpstore.New(srv.URL, httpstore.WithHTTPClient(mtls))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := s.Subscribe(ctx, bob, messaging.Filter{})
	if err != nil {
		t.Fatalf("Subscribe over mTLS: %v", err)
	}
	if _, err := s.Send(ctx, notice(alice, bob)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("no envelope over the mTLS stream")
	}
}

func TestRequestHook(t *testing.T) {
	t.Run("sees every operation and can decorate the request", func(t *testing.T) {
		rc := &recorder{}
		ms := memstore.New()
		srv := httptest.NewServer(rc.wrap(httpstoretest.Handler(ms, nil, httpstore.TetherProfile())))
		t.Cleanup(srv.Close)

		var mu sync.Mutex
		var ops []httpstore.Op
		s, err := httpstore.New(srv.URL,
			httpstore.WithIdentity(alice),
			httpstore.WithRequestHook(func(r *http.Request, op httpstore.Op) error {
				mu.Lock()
				ops = append(ops, op)
				mu.Unlock()
				r.Header.Set("Authorization", "Bearer t0k3n")
				return nil
			}),
			httpstore.WithRequestHook(func(r *http.Request, _ httpstore.Op) error {
				r.Header.Add("X-Trace", "second") // hooks run in order, both apply
				return nil
			}))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ch, err := s.Subscribe(ctx, bob, messaging.Filter{})
		if err != nil {
			t.Fatal(err)
		}
		sent, err := s.Send(ctx, notice(alice, bob))
		if err != nil {
			t.Fatal(err)
		}
		<-ch
		_, _ = s.Get(ctx, sent.ID)
		_, _ = s.Thread(ctx, "x", messaging.Filter{})
		_, _ = s.Inbox(ctx, bob, messaging.Filter{})
		_ = s.Consume(ctx, sent.ID, bob)
		_ = s.Cancel(ctx, sent.ID)

		mu.Lock()
		got := map[httpstore.Op]bool{}
		for _, op := range ops {
			got[op] = true
		}
		mu.Unlock()
		for _, op := range []httpstore.Op{httpstore.OpSend, httpstore.OpGet, httpstore.OpInbox, httpstore.OpThread,
			httpstore.OpConsume, httpstore.OpCancel, httpstore.OpSubscribe} {
			if !got[op] {
				t.Errorf("hook never saw op %q", op)
			}
		}
		for _, req := range rc.all() {
			if req.Header.Get("Authorization") != "Bearer t0k3n" || req.Header.Get("X-Trace") != "second" {
				t.Errorf("%s %s: hook headers missing: %v", req.Method, req.Path, req.Header)
			}
		}
	})

	t.Run("an error aborts the call before the network", func(t *testing.T) {
		srv, rc := stub(t, http.StatusOK, "application/json", "{}")
		boom := errors.New("no token available")
		s, err := httpstore.New(srv.URL, httpstore.WithIdentity(alice),
			httpstore.WithRequestHook(func(*http.Request, httpstore.Op) error { return boom }))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Get(context.Background(), "x"); !errors.Is(err, boom) {
			t.Errorf("Get: got %v, want the hook's error", err)
		}
		if _, err := s.Subscribe(context.Background(), bob, messaging.Filter{}); !errors.Is(err, boom) {
			t.Errorf("Subscribe: got %v, want the hook's error", err)
		}
		if rc.count() != 0 {
			t.Errorf("a failed hook still sent %d request(s)", rc.count())
		}
	})
}

func TestOptions(t *testing.T) {
	t.Run("WithBasePath overrides the profile in either order", func(t *testing.T) {
		srv, rc := stub(t, http.StatusNoContent, "", "")
		for name, opts := range map[string][]httpstore.Option{
			"before": {httpstore.WithBasePath("/api/v9/msgs"), httpstore.WithProfile(httpstore.TorqueFederationProfile())},
			"after":  {httpstore.WithProfile(httpstore.TorqueFederationProfile()), httpstore.WithBasePath("/api/v9/msgs/")},
		} {
			s, err := httpstore.New(srv.URL, opts...)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Cancel(context.Background(), "x"); err != nil {
				t.Fatal(err)
			}
			if got := rc.last(t).Path; got != "/api/v9/msgs/x/cancel" {
				t.Errorf("%s: path = %q", name, got)
			}
		}
	})

	t.Run("a URL path prefix is kept", func(t *testing.T) {
		srv, rc := stub(t, http.StatusNoContent, "", "")
		s, err := httpstore.New(srv.URL + "/tether/")
		if err != nil {
			t.Fatal(err)
		}
		_ = s.Cancel(context.Background(), "x")
		if got := rc.last(t).Path; got != "/tether/messages/x/cancel" {
			t.Errorf("path = %q", got)
		}
	})

	t.Run("nil client and nil hook are ignored", func(t *testing.T) {
		srv, _ := stub(t, http.StatusNoContent, "", "")
		s, err := httpstore.New(srv.URL, httpstore.WithHTTPClient(nil), httpstore.WithRequestHook(nil))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Cancel(context.Background(), "x"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("the profile is copied", func(t *testing.T) {
		p := httpstore.TorqueFederationProfile()
		srv, _ := stub(t, http.StatusOK, "application/json", `{"messages":[]}`)
		s, err := httpstore.New(srv.URL, httpstore.WithProfile(p))
		if err != nil {
			t.Fatal(err)
		}
		p.Unsupported[0] = httpstore.OpCancel // mutate the caller's slice afterwards
		if _, err := s.Inbox(context.Background(), bob, messaging.Filter{}); !errors.Is(err, httpstore.ErrUnsupported) {
			t.Errorf("Inbox: got %v; the store's profile must not follow later edits", err)
		}
	})

	t.Run("zero addresses are rejected before the network", func(t *testing.T) {
		srv, rc := stub(t, http.StatusOK, "application/json", `{"messages":[]}`)
		s, err := httpstore.New(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		if _, err := s.Inbox(ctx, messaging.Address{}, messaging.Filter{}); err == nil {
			t.Error("Inbox with a zero recipient: want an error")
		}
		if _, err := s.Subscribe(ctx, messaging.Address{}, messaging.Filter{}); err == nil {
			t.Error("Subscribe with a zero recipient: want an error")
		}
		if err := s.Consume(ctx, "x", messaging.Address{}); err == nil {
			t.Error("Consume with a zero recipient: want an error")
		}
		if rc.count() != 0 {
			t.Errorf("zero addresses sent %d request(s)", rc.count())
		}
	})
}

func TestTimeouts(t *testing.T) {
	slow := func(t *testing.T, d time.Duration) *httptest.Server {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-time.After(d):
				w.WriteHeader(http.StatusNoContent)
			case <-r.Context().Done():
			}
		}))
		t.Cleanup(srv.Close)
		return srv
	}

	t.Run("WithTimeout bounds a non-streaming call", func(t *testing.T) {
		srv := slow(t, 2*time.Second)
		s, err := httpstore.New(srv.URL, httpstore.WithTimeout(50*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		err = s.Cancel(context.Background(), "x")
		if !errors.Is(err, messaging.ErrStoreUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("got %v, want ErrStoreUnavailable wrapping DeadlineExceeded", err)
		}
		if time.Since(start) > time.Second {
			t.Errorf("took %v; the 50ms bound did not apply", time.Since(start))
		}
	})

	t.Run("WithTimeout(0) leaves only the context", func(t *testing.T) {
		srv := slow(t, 150*time.Millisecond)
		s, err := httpstore.New(srv.URL, httpstore.WithTimeout(0))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Cancel(context.Background(), "x"); err != nil {
			t.Errorf("got %v", err)
		}
	})

	t.Run("the caller's context still wins", func(t *testing.T) {
		srv := slow(t, 2*time.Second)
		s, err := httpstore.New(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if err := s.Cancel(ctx, "x"); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("got %v, want DeadlineExceeded", err)
		}
	})

	t.Run("a client Timeout does not sever a stream", func(t *testing.T) {
		ms := memstore.New()
		srv := httpstoretest.NewServer(t, ms, nil, httpstore.TetherProfile())
		short := &http.Client{Timeout: 100 * time.Millisecond}
		s, err := httpstore.New(srv.URL, httpstore.WithHTTPClient(short), httpstore.WithTimeout(100*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ch, err := s.Subscribe(ctx, bob, messaging.Filter{})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(400 * time.Millisecond) // well past both timeouts
		if _, err := ms.Send(ctx, notice(alice, bob)); err != nil {
			t.Fatal(err)
		}
		select {
		case _, ok := <-ch:
			if !ok {
				t.Fatal("stream was closed by a timeout")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("no envelope after the timeouts elapsed")
		}
	})
}
