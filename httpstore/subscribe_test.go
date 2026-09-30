package httpstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	messaging "github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/httpstore"
	"github.com/hollis-labs/go-messaging/httpstore/httpstoretest"
	"github.com/hollis-labs/go-messaging/memstore"
)

// sseServer runs h as the /messages/subscribe handler, after the response
// headers have been flushed.
func sseServer(t *testing.T, h func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/messages/subscribe", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		h(w, r)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func envJSON(t *testing.T, id string) string {
	t.Helper()
	b, err := json.Marshal(messaging.Envelope{ID: id, Kind: messaging.MsgKindNotice, From: alice, To: bob})
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func recv(t *testing.T, ch <-chan messaging.Envelope) (messaging.Envelope, bool) {
	t.Helper()
	select {
	case e, ok := <-ch:
		return e, ok
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting on the subscription channel")
		return messaging.Envelope{}, false
	}
}

// Cancelling the context closes the channel, and repeated subscribe/cancel
// cycles leave no goroutines behind (the stream reader, the body, the
// transport's connection goroutines).
func TestSubscribe_CancelClosesChannelAndLeaksNothing(t *testing.T) {
	ms := memstore.New()
	srv := httpstoretest.NewServer(t, ms, nil, httpstore.TetherProfile())
	client := noKeepAlive()
	s, err := httpstore.New(srv.URL, httpstore.WithHTTPClient(client))
	if err != nil {
		t.Fatal(err)
	}
	// Warm up once so lazily started runtime goroutines are in the baseline.
	{
		ctx, cancel := context.WithCancel(context.Background())
		ch, err := s.Subscribe(ctx, bob, messaging.Filter{})
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		for range ch {
		}
	}
	waitFor(t, "warm-up goroutines to exit", func() bool { return goroutines() < 1000 })
	time.Sleep(50 * time.Millisecond)
	before := goroutines()

	for i := 0; i < 25; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		ch, err := s.Subscribe(ctx, bob, messaging.Filter{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ms.Send(ctx, notice(alice, bob)); err != nil {
			t.Fatal(err)
		}
		if _, ok := recv(t, ch); !ok {
			t.Fatal("channel closed early")
		}
		cancel()
		select {
		case _, ok := <-ch:
			for ok { // drain anything buffered, then it must close
				_, ok = <-ch
			}
		case <-time.After(3 * time.Second):
			t.Fatal("channel did not close after cancel")
		}
	}
	waitFor(t, fmt.Sprintf("goroutines to return to %d (now %d)", before, goroutines()), func() bool { return goroutines() <= before })
}

// A consumer that stops reading and then cancels must not strand the reader
// goroutine on a full channel.
func TestSubscribe_CancelWhileConsumerNotReading(t *testing.T) {
	line := "data: " + envJSON(t, "x") + "\n\n"
	srv := sseServer(t, func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < 200; i++ { // far more than the channel buffer
			if _, err := io.WriteString(w, line); err != nil {
				return
			}
		}
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	s, err := httpstore.New(srv.URL, httpstore.WithHTTPClient(noKeepAlive()))
	if err != nil {
		t.Fatal(err)
	}
	before := goroutines()
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := s.Subscribe(ctx, bob, messaging.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // reader fills the buffer and blocks
	cancel()
	waitFor(t, "channel to close", func() bool {
		select {
		case _, ok := <-ch:
			return !ok
		default:
			return false
		}
	})
	waitFor(t, "goroutines to settle", func() bool { return goroutines() <= before+1 })
}

func TestSubscribe_StreamEndClosesChannel(t *testing.T) {
	srv := sseServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "data: "+envJSON(t, "only")+"\n\n")
	})
	s, err := httpstore.New(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := s.Subscribe(context.Background(), bob, messaging.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if e, ok := recv(t, ch); !ok || e.ID != "only" {
		t.Fatalf("got %+v ok=%v", e, ok)
	}
	if _, ok := recv(t, ch); ok {
		t.Fatal("channel should close when the server ends the stream")
	}
}

// A failure to connect is returned by Subscribe itself, with no channel.
func TestSubscribe_ConnectFailureIsSynchronous(t *testing.T) {
	t.Run("403 is unavailable", func(t *testing.T) {
		srv, _ := stub(t, http.StatusForbidden, "application/json", `{"error":"nope"}`)
		s, _ := httpstore.New(srv.URL)
		ch, err := s.Subscribe(context.Background(), bob, messaging.Filter{})
		if ch != nil || !errors.Is(err, messaging.ErrStoreUnavailable) {
			t.Errorf("got %v, %v", ch, err)
		}
	})
	t.Run("400 is a StatusError", func(t *testing.T) {
		srv, _ := stub(t, http.StatusBadRequest, "application/json", `{"error":{"code":"invalid_request","message":"as is required"}}`)
		s, _ := httpstore.New(srv.URL)
		ch, err := s.Subscribe(context.Background(), bob, messaging.Filter{})
		var se *httpstore.StatusError
		if ch != nil || !errors.As(err, &se) || se.Code != 400 {
			t.Errorf("got %v, %v", ch, err)
		}
	})
	t.Run("nothing listening", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		url := srv.URL
		srv.Close()
		s, _ := httpstore.New(url)
		if _, err := s.Subscribe(context.Background(), bob, messaging.Filter{}); !errors.Is(err, messaging.ErrStoreUnavailable) {
			t.Errorf("got %v", err)
		}
	})
}

// Real-world framing variants all decode: CRLF, no space after the colon,
// data split over several lines, comments and other event names ignored.
func TestSubscribe_FramingVariants(t *testing.T) {
	split := envJSON(t, "split")
	i := strings.Index(split, ",")
	srv := sseServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, ": ping\r\n\r\n")
		_, _ = io.WriteString(w, "event: message\r\ndata:"+envJSON(t, "crlf-nospace")+"\r\n\r\n")
		_, _ = io.WriteString(w, "event: heartbeat\ndata: {\"not\":\"an envelope\"}\n\n")
		_, _ = io.WriteString(w, "data: "+split[:i+1]+"\ndata: "+split[i+1:]+"\n\n") // one JSON document over two lines
		_, _ = io.WriteString(w, "id: 5\nretry: 1000\ndata: "+envJSON(t, "cr-only")+"\r\r")
	})
	var mu sync.Mutex
	var errs []error
	s, err := httpstore.New(srv.URL, httpstore.WithOnFrameError(func(err error) {
		mu.Lock()
		errs = append(errs, err)
		mu.Unlock()
	}))
	if err != nil {
		t.Fatal(err)
	}
	ch, err := s.Subscribe(context.Background(), bob, messaging.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for e := range ch {
		ids = append(ids, e.ID)
	}
	if want := "crlf-nospace,split,cr-only"; strings.Join(ids, ",") != want {
		t.Errorf("delivered %v, want %s", ids, want)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(errs) != 0 {
		t.Errorf("unexpected frame errors: %v", errs)
	}
}

// Problems in the stream are reported, not swallowed, and do not stop it.
func TestSubscribe_BadFramesAreReported(t *testing.T) {
	huge := strings.Repeat("x", 1<<20+64)
	srv := sseServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "data: this is not json\n\n")
		_, _ = io.WriteString(w, "data: "+envJSON(t, "good-1")+"\n\n")
		_, _ = io.WriteString(w, "data: "+huge+"\n\n")
		_, _ = io.WriteString(w, "data: "+envJSON(t, "good-2")+"\n\n")
	})
	var mu sync.Mutex
	var errs []error
	s, err := httpstore.New(srv.URL, httpstore.WithOnFrameError(func(err error) {
		mu.Lock()
		errs = append(errs, err)
		mu.Unlock()
	}))
	if err != nil {
		t.Fatal(err)
	}
	ch, err := s.Subscribe(context.Background(), bob, messaging.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for e := range ch {
		ids = append(ids, e.ID)
	}
	if want := "good-1,good-2"; strings.Join(ids, ",") != want {
		t.Errorf("delivered %v, want %s", ids, want)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(errs) != 2 {
		t.Fatalf("frame errors = %v, want the undecodable frame and the oversized one", errs)
	}
	var syn *json.SyntaxError
	if !errors.As(errs[0], &syn) {
		t.Errorf("first error %v, want a JSON syntax error", errs[0])
	}
	if !strings.Contains(errs[1].Error(), "larger than") {
		t.Errorf("second error %v, want the size-cap error", errs[1])
	}
}

// Without a callback the same problems are simply skipped (no panic).
func TestSubscribe_BadFramesWithoutCallback(t *testing.T) {
	srv := sseServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "data: nope\n\ndata: "+envJSON(t, "ok")+"\n\n")
	})
	s, _ := httpstore.New(srv.URL)
	ch, err := s.Subscribe(context.Background(), bob, messaging.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if e, ok := recv(t, ch); !ok || e.ID != "ok" {
		t.Errorf("got %+v ok=%v", e, ok)
	}
}

// A stream that dies mid-event is reported as such, and the channel closes.
func TestSubscribe_AbruptEndIsReported(t *testing.T) {
	srv := sseServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "data: "+envJSON(t, "before")+"\n\ndata: {\"id\":")
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	})
	var mu sync.Mutex
	var errs []error
	s, err := httpstore.New(srv.URL, httpstore.WithOnFrameError(func(err error) {
		mu.Lock()
		errs = append(errs, err)
		mu.Unlock()
	}))
	if err != nil {
		t.Fatal(err)
	}
	ch, err := s.Subscribe(context.Background(), bob, messaging.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if e, ok := recv(t, ch); !ok || e.ID != "before" {
		t.Fatalf("got %+v ok=%v", e, ok)
	}
	if _, ok := recv(t, ch); ok {
		t.Fatal("channel should close when the connection dies")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(errs) != 1 || !errors.Is(errs[0], io.ErrUnexpectedEOF) {
		t.Errorf("errors = %v, want one wrapping io.ErrUnexpectedEOF", errs)
	}
}
