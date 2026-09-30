package httpstoretest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	messaging "github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/httpstore"
)

// Option configures Handler and NewServer.
type Option func(*config)

type config struct{ strict bool }

// WithStrictIdentity makes the server enforce the identity rules of the
// profile it serves. See the package documentation for the exact list.
func WithStrictIdentity() Option { return func(c *config) { c.strict = true } }

type server struct {
	store  messaging.Store
	disp   messaging.Dispatcher
	p      httpstore.Profile
	strict bool
}

// Handler serves store over the routes and dialect of profile p. disp is
// used for the blocking request route (a profile with BlockingRequest); nil
// means messaging.NewDispatcher(store).
func Handler(store messaging.Store, disp messaging.Dispatcher, p httpstore.Profile, opts ...Option) http.Handler {
	var cfg config
	for _, o := range opts {
		o(&cfg)
	}
	if disp == nil {
		disp = messaging.NewDispatcher(store)
	}
	s := &server{store: store, disp: disp, p: p, strict: cfg.strict}
	bp := "/" + strings.Trim(p.BasePath, "/")
	if bp == "/" {
		bp = "/messages"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		s.fail(w, http.StatusNotFound, "not_found", "no such route")
	})
	route := func(op httpstore.Op, pattern string, h http.HandlerFunc) {
		if !p.Supports(op) {
			h = func(w http.ResponseWriter, r *http.Request) {
				s.fail(w, http.StatusNotFound, "not_found", string(op)+" is not served")
			}
		}
		mux.HandleFunc(pattern, h)
	}
	route(httpstore.OpSend, "POST "+bp, s.send)
	route(httpstore.OpInbox, "GET "+bp+"/inbox", s.inbox)
	route(httpstore.OpSubscribe, "GET "+bp+"/subscribe", s.subscribe)
	route(httpstore.OpThread, "GET "+bp+"/thread/{threadID}", s.thread)
	route(httpstore.OpGet, "GET "+bp+"/{id}", s.get)
	route(httpstore.OpConsume, "POST "+bp+"/{id}/consume", s.consume)
	route(httpstore.OpCancel, "POST "+bp+"/{id}/cancel", s.cancel)
	if p.BlockingRequest {
		route(httpstore.OpRequest, "POST "+bp+"/request", s.request)
	}
	return mux
}

// NewServer starts an httptest.Server running Handler and closes it when t
// ends.
func NewServer(t testing.TB, store messaging.Store, disp messaging.Dispatcher, p httpstore.Profile, opts ...Option) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(Handler(store, disp, p, opts...))
	t.Cleanup(srv.Close)
	return srv
}

// asserts reports whether strict identity rules apply on this server.
func (s *server) asserts() bool { return s.strict && s.p.AssertAs }

func (s *server) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *server) fail(w http.ResponseWriter, status int, code, msg string) {
	if s.p.ConsumeBody { // Torque's flat shape
		s.writeJSON(w, status, map[string]string{"error": msg})
		return
	}
	s.writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": msg}})
}

// storeErr maps a Store error to the status Tether and Torque use.
func (s *server) storeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, messaging.ErrNotFound):
		s.fail(w, http.StatusNotFound, "not_found", "message not found")
	case errors.Is(err, messaging.ErrPresetLifecycle):
		s.fail(w, http.StatusUnprocessableEntity, "preset_lifecycle", err.Error())
	default:
		s.fail(w, http.StatusInternalServerError, "internal_error", err.Error())
	}
}

func (s *server) messages(w http.ResponseWriter, envs []messaging.Envelope) {
	if envs == nil {
		envs = []messaging.Envelope{}
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"messages": envs})
}

// list reads a comma-joined or repeated multi-value parameter.
func (s *server) list(r *http.Request, name string) []string {
	var out []string
	if s.p.KindsRepeated {
		for _, v := range r.URL.Query()[name] {
			if v != "" {
				out = append(out, v)
			}
		}
		return out
	}
	for _, v := range strings.Split(r.URL.Query().Get(name), ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// filter parses the query. Under an identity-asserting profile it reads only
// what Tether's handlers read: kind and thread_id (Inbox, Subscribe) or kind
// (Thread); channel and limit are ignored. Otherwise it reads everything.
func (s *server) filter(r *http.Request, thread, limit, channel bool) messaging.Filter {
	var f messaging.Filter
	for _, k := range s.list(r, "kind") {
		f.Kind = append(f.Kind, messaging.Kind(k))
	}
	if channel && !s.p.AssertAs {
		for _, c := range s.list(r, "channel") {
			f.Channel = append(f.Channel, messaging.Channel(c))
		}
	}
	if thread {
		f.ThreadID = r.URL.Query().Get("thread_id")
	}
	if limit && !s.p.AssertAs {
		if n, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && n > 0 {
			f.Limit = n
		}
	}
	return f
}

func (s *server) send(w http.ResponseWriter, r *http.Request) {
	var env messaging.Envelope
	if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
		s.fail(w, http.StatusBadRequest, "invalid_request", "invalid body: "+err.Error())
		return
	}
	env.ID = ""
	out, err := s.store.Send(r.Context(), env)
	if err != nil {
		s.storeErr(w, err)
		return
	}
	s.writeJSON(w, http.StatusCreated, out)
}

func (s *server) get(w http.ResponseWriter, r *http.Request) {
	as := r.URL.Query().Get("as")
	if s.asserts() && as == "" {
		s.fail(w, http.StatusBadRequest, "invalid_request", "as is required")
		return
	}
	env, err := s.store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		s.storeErr(w, err)
		return
	}
	if s.asserts() && as != env.From.URN() && as != env.To.URN() {
		s.fail(w, http.StatusForbidden, "forbidden", "as must be the message's sender or recipient")
		return
	}
	s.writeJSON(w, http.StatusOK, env)
}

// recipientClaim validates ?to= and, under strict rules, ?as=.
func (s *server) recipientClaim(w http.ResponseWriter, r *http.Request) (messaging.Address, bool) {
	q := r.URL.Query()
	toURN := q.Get("to")
	if toURN == "" {
		s.fail(w, http.StatusBadRequest, "invalid_request", "to query param required")
		return messaging.Address{}, false
	}
	if s.asserts() {
		as := q.Get("as")
		if as == "" {
			s.fail(w, http.StatusBadRequest, "invalid_request", "as is required")
			return messaging.Address{}, false
		}
		if as != toURN {
			s.fail(w, http.StatusForbidden, "forbidden", "as must match to")
			return messaging.Address{}, false
		}
	}
	to, err := messaging.ParseURN(toURN)
	if err != nil {
		s.fail(w, http.StatusBadRequest, "invalid_request", "invalid to URN: "+err.Error())
		return messaging.Address{}, false
	}
	return to, true
}

func (s *server) inbox(w http.ResponseWriter, r *http.Request) {
	to, ok := s.recipientClaim(w, r)
	if !ok {
		return
	}
	envs, err := s.store.Inbox(r.Context(), to, s.filter(r, true, true, true))
	if err != nil {
		s.storeErr(w, err)
		return
	}
	s.messages(w, envs)
}

func (s *server) thread(w http.ResponseWriter, r *http.Request) {
	as := r.URL.Query().Get("as")
	if s.asserts() && as == "" {
		s.fail(w, http.StatusBadRequest, "invalid_request", "as is required")
		return
	}
	envs, err := s.store.Thread(r.Context(), r.PathValue("threadID"), s.filter(r, false, true, true))
	if err != nil {
		s.storeErr(w, err)
		return
	}
	if s.asserts() {
		scoped := envs[:0:0]
		for _, env := range envs {
			if as == env.From.URN() || as == env.To.URN() {
				scoped = append(scoped, env)
			}
		}
		envs = scoped
	}
	s.messages(w, envs)
}

func (s *server) consume(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var urn string
	if s.p.ConsumeBody {
		var body struct {
			Recipient string `json:"recipient"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Recipient == "" {
			s.fail(w, http.StatusUnprocessableEntity, "invalid_request", "recipient required")
			return
		}
		urn = body.Recipient
	} else {
		urn = r.URL.Query().Get("as")
		if urn == "" {
			s.fail(w, http.StatusBadRequest, "invalid_request", "as query param required")
			return
		}
	}
	recipient, err := messaging.ParseURN(urn)
	if err != nil {
		s.fail(w, http.StatusBadRequest, "invalid_request", "invalid recipient URN: "+err.Error())
		return
	}
	if s.asserts() {
		env, err := s.store.Get(r.Context(), id)
		if err != nil {
			s.storeErr(w, err)
			return
		}
		if env.To != recipient {
			s.fail(w, http.StatusConflict, "conflict", "caller is not the intended recipient")
			return
		}
	}
	if err := s.store.Consume(r.Context(), id, recipient); err != nil {
		s.storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) cancel(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Cancel(r.Context(), r.PathValue("id")); err != nil {
		s.storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// pingInterval is how often an idle stream gets a comment line, as Tether's.
const pingInterval = 15 * time.Second

func (s *server) subscribe(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		s.fail(w, http.StatusInternalServerError, "internal_error", "streaming not supported")
		return
	}
	to, ok := s.recipientClaim(w, r)
	if !ok {
		return
	}
	// Register with the Store before the response headers go out, so a Send
	// made as soon as the client's Subscribe returns is observed.
	ch, err := s.store.Subscribe(r.Context(), to, s.filter(r, true, false, true))
	if err != nil {
		s.storeErr(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ping := time.NewTicker(pingInterval)
	defer ping.Stop()
	for {
		select {
		case env, open := <-ch:
			if !open {
				return
			}
			b, err := json.Marshal(env)
			if err != nil {
				continue
			}
			_, _ = fmt.Fprintf(w, "event: message\ndata: %s\n\n", b)
			flusher.Flush()
		case <-ping.C:
			_, _ = fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func (s *server) request(w http.ResponseWriter, r *http.Request) {
	timeout := 30 * time.Second
	if v := r.URL.Query().Get("timeout"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			timeout = d
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	var env messaging.Envelope
	if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
		s.fail(w, http.StatusBadRequest, "invalid_request", "invalid body: "+err.Error())
		return
	}
	out, err := s.disp.Request(ctx, env)
	if err != nil {
		if errors.Is(err, messaging.ErrRequestTimeout) || errors.Is(err, context.DeadlineExceeded) {
			s.fail(w, http.StatusGatewayTimeout, "request_timeout", "request timed out")
			return
		}
		s.storeErr(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, out)
}
