package httpstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	messaging "github.com/hollis-labs/go-messaging"
)

// maxResponseBytes bounds a decoded response body. A body over it fails to
// decode rather than being silently truncated.
const maxResponseBytes = 64 << 20

// Store is a messaging.Store backed by a remote HTTP server. It is immutable
// after New and safe for concurrent use.
type Store struct {
	base         string // scheme://host[/prefix]/basepath, no trailing slash
	profile      Profile
	client       *http.Client // bounded per call by timeout
	stream       *http.Client // copy of client with Timeout zero
	identity     messaging.Address
	hooks        []func(*http.Request, Op) error
	timeout      time.Duration
	onFrameError func(error)
}

var _ messaging.Store = (*Store)(nil)

// New builds a Store for the server at baseURL (scheme://host[:port], with
// an optional path prefix). It rejects an empty or relative URL, a scheme
// other than http or https, a missing host, and a URL carrying a query or
// fragment. The default profile is TetherProfile().
func New(baseURL string, opts ...Option) (*Store, error) {
	cfg := config{profile: TetherProfile(), timeout: DefaultTimeout}
	for _, o := range opts {
		o(&cfg)
	}
	if strings.TrimSpace(baseURL) == "" {
		return nil, errors.New("httpstore: base URL is empty")
	}
	u, err := url.Parse(strings.TrimRight(strings.TrimSpace(baseURL), "/"))
	if err != nil {
		return nil, fmt.Errorf("httpstore: base URL %q: %w", baseURL, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("httpstore: base URL %q: scheme must be http or https", baseURL)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("httpstore: base URL %q: missing host", baseURL)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("httpstore: base URL %q: must not carry a query or fragment", baseURL)
	}
	if cfg.basePath != "" {
		cfg.profile.BasePath = cfg.basePath
	}
	bp := strings.Trim(cfg.profile.BasePath, "/")
	if bp == "" {
		bp = "messages"
	}
	client := cfg.client
	if client == nil {
		client = &http.Client{}
	}
	streamClient := *client
	streamClient.Timeout = 0
	return &Store{
		base:         u.String() + "/" + bp,
		profile:      cfg.profile,
		client:       client,
		stream:       &streamClient,
		identity:     cfg.identity,
		hooks:        cfg.hooks,
		timeout:      cfg.timeout,
		onFrameError: cfg.onFrameError,
	}, nil
}

// call describes one request.
type call struct {
	op     Op
	method string
	path   string // relative to base, already escaped, "" for the collection
	query  url.Values
	body   any
	accept string
}

func (s *Store) unsupported(op Op) error {
	if s.profile.Supports(op) {
		return nil
	}
	return fmt.Errorf("%w: %s (profile %q)", ErrUnsupported, op, s.profile.Name)
}

// send issues c on client and returns the response when its status is 2xx;
// the caller closes the body. Every other outcome is a mapped error.
func (s *Store) send(ctx context.Context, client *http.Client, c call) (*http.Response, error) {
	target := s.base
	if c.path != "" {
		target += "/" + c.path
	}
	if len(c.query) > 0 {
		target += "?" + c.query.Encode()
	}
	var body io.Reader
	if c.body != nil {
		b, err := json.Marshal(c.body)
		if err != nil {
			return nil, fmt.Errorf("httpstore: encode %s request: %w", c.op, err)
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, c.method, target, body)
	if err != nil {
		return nil, fmt.Errorf("httpstore: build %s request: %w", c.op, err)
	}
	if c.body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.accept != "" {
		req.Header.Set("Accept", c.accept)
	} else {
		req.Header.Set("Accept", "application/json")
	}
	for _, h := range s.hooks {
		if err := h(req, c.op); err != nil {
			return nil, fmt.Errorf("httpstore: request hook (%s): %w", c.op, err)
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", messaging.ErrStoreUnavailable, err)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp, nil
	}
	defer func() { _ = resp.Body.Close() }()
	return nil, mapStatus(c.op, resp)
}

// do runs a bounded, non-streaming call and decodes the response into out
// (when non-nil).
func (s *Store) do(ctx context.Context, c call, out any) error {
	if s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}
	return s.exchange(ctx, s.client, c, out)
}

func (s *Store) exchange(ctx context.Context, client *http.Client, c call, out any) error {
	resp, err := s.send(ctx, client, c)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if out == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(out); err != nil {
		return fmt.Errorf("httpstore: decode %s response: %w", c.op, err)
	}
	return nil
}

// filterQuery encodes f the way the profile's server parses it. Kind and
// Channel are comma-joined (one parameter) or repeated per
// Profile.KindsRepeated.
func (s *Store) filterQuery(q url.Values, f messaging.Filter, thread, limit bool) {
	var kinds, channels []string
	for _, k := range f.Kind {
		if k != "" {
			kinds = append(kinds, string(k))
		}
	}
	for _, ch := range f.Channel {
		if ch != "" {
			channels = append(channels, string(ch))
		}
	}
	put := func(name string, vals []string) {
		if len(vals) == 0 {
			return
		}
		if s.profile.KindsRepeated {
			for _, v := range vals {
				q.Add(name, v)
			}
			return
		}
		q.Set(name, strings.Join(vals, ","))
	}
	put("kind", kinds)
	put("channel", channels)
	if thread && f.ThreadID != "" {
		q.Set("thread_id", f.ThreadID)
	}
	if limit && f.Limit > 0 {
		q.Set("limit", strconv.Itoa(f.Limit))
	}
}

// claim returns the identity Get and Thread assert, or ErrIdentityRequired.
func (s *Store) claim() (string, error) {
	if s.identity.IsZero() {
		return "", ErrIdentityRequired
	}
	return s.identity.URN(), nil
}

func requireAddr(op Op, role string, a messaging.Address) error {
	if a.IsZero() {
		return fmt.Errorf("httpstore: %s needs a non-zero %s address", op, role)
	}
	return nil
}

// Send persists env on the server. ID and CreatedAt are cleared before the
// request (the server assigns them); a preset DeliveredAt or ConsumedAt is
// rejected with messaging.ErrPresetLifecycle without a round trip.
func (s *Store) Send(ctx context.Context, env messaging.Envelope) (messaging.Envelope, error) {
	if env.DeliveredAt != nil || env.ConsumedAt != nil {
		return messaging.Envelope{}, messaging.ErrPresetLifecycle
	}
	if err := s.unsupported(OpSend); err != nil {
		return messaging.Envelope{}, err
	}
	env.ID = ""
	env.CreatedAt = time.Time{}
	var out messaging.Envelope
	if err := s.do(ctx, call{op: OpSend, method: http.MethodPost, body: env}, &out); err != nil {
		return messaging.Envelope{}, err
	}
	return out, nil
}

// Get fetches one envelope. On a profile that asserts identity it needs
// WithIdentity (ErrIdentityRequired otherwise).
func (s *Store) Get(ctx context.Context, id string) (messaging.Envelope, error) {
	if err := s.unsupported(OpGet); err != nil {
		return messaging.Envelope{}, err
	}
	c := call{op: OpGet, method: http.MethodGet, path: url.PathEscape(id)}
	if s.profile.AssertAs {
		as, err := s.claim()
		if err != nil {
			return messaging.Envelope{}, err
		}
		c.query = url.Values{"as": {as}}
	}
	var out messaging.Envelope
	if err := s.do(ctx, c, &out); err != nil {
		return messaging.Envelope{}, err
	}
	return out, nil
}

type messagesBody struct {
	Messages []messaging.Envelope `json:"messages"`
}

// Inbox returns the recipient's undelivered envelopes and, server-side,
// marks them delivered. The result is exactly what the server returned: it
// is never truncated here, even when it is longer than f.Limit, because the
// server has already marked every returned envelope delivered and cutting
// the list would lose the rest.
func (s *Store) Inbox(ctx context.Context, to messaging.Address, f messaging.Filter) ([]messaging.Envelope, error) {
	if err := s.unsupported(OpInbox); err != nil {
		return nil, err
	}
	if err := requireAddr(OpInbox, "recipient", to); err != nil {
		return nil, err
	}
	q := url.Values{"to": {to.URN()}}
	if s.profile.AssertAs {
		q.Set("as", to.URN())
	}
	s.filterQuery(q, f, true, true)
	var out messagesBody
	if err := s.do(ctx, call{op: OpInbox, method: http.MethodGet, path: "inbox", query: q}, &out); err != nil {
		return nil, err
	}
	return out.Messages, nil
}

// Thread returns the envelopes of a thread. It is read-only. On a profile
// that asserts identity it needs WithIdentity, and a server that scopes a
// thread to its parties (Tether) returns only the turns involving that
// identity.
func (s *Store) Thread(ctx context.Context, threadID string, f messaging.Filter) ([]messaging.Envelope, error) {
	if err := s.unsupported(OpThread); err != nil {
		return nil, err
	}
	q := url.Values{}
	if s.profile.AssertAs {
		as, err := s.claim()
		if err != nil {
			return nil, err
		}
		q.Set("as", as)
	}
	s.filterQuery(q, f, true, true)
	var out messagesBody
	if err := s.do(ctx, call{op: OpThread, method: http.MethodGet, path: "thread/" + url.PathEscape(threadID), query: q}, &out); err != nil {
		return nil, err
	}
	return out.Messages, nil
}

// Consume records that recipient consumed the envelope. Idempotent. The
// recipient travels as ?as=<URN>, or in a JSON body under
// Profile.ConsumeBody.
func (s *Store) Consume(ctx context.Context, id string, recipient messaging.Address) error {
	if err := s.unsupported(OpConsume); err != nil {
		return err
	}
	if err := requireAddr(OpConsume, "recipient", recipient); err != nil {
		return err
	}
	c := call{op: OpConsume, method: http.MethodPost, path: url.PathEscape(id) + "/consume"}
	if s.profile.ConsumeBody {
		c.body = map[string]string{"recipient": recipient.URN()}
	} else {
		c.query = url.Values{"as": {recipient.URN()}}
	}
	return s.do(ctx, c, nil)
}

// Cancel marks an envelope dead. Idempotent.
func (s *Store) Cancel(ctx context.Context, id string) error {
	if err := s.unsupported(OpCancel); err != nil {
		return err
	}
	return s.do(ctx, call{op: OpCancel, method: http.MethodPost, path: url.PathEscape(id) + "/cancel"}, nil)
}

// Subscribe opens the server's SSE stream for recipient `to`. The connection
// is established before Subscribe returns, so a Send made right after it is
// observed; a failure to connect is returned here. The channel closes when
// ctx is canceled or the stream ends; there is no reconnect. Filter.Limit
// does not apply to a stream and is not sent.
//
// Subscribe is not bounded by WithTimeout or by a Timeout on the client
// passed to WithHTTPClient.
func (s *Store) Subscribe(ctx context.Context, to messaging.Address, f messaging.Filter) (<-chan messaging.Envelope, error) {
	if err := s.unsupported(OpSubscribe); err != nil {
		return nil, err
	}
	if err := requireAddr(OpSubscribe, "recipient", to); err != nil {
		return nil, err
	}
	q := url.Values{"to": {to.URN()}}
	if s.profile.AssertAs {
		q.Set("as", to.URN())
	}
	s.filterQuery(q, f, true, false)
	resp, err := s.send(ctx, s.stream, call{op: OpSubscribe, method: http.MethodGet, path: "subscribe", query: q, accept: "text/event-stream"})
	if err != nil {
		return nil, err
	}
	ch := make(chan messaging.Envelope, 16)
	go s.pump(ctx, resp.Body, ch)
	return ch, nil
}

// pump reads the SSE stream until it ends or ctx is canceled, delivering
// decoded envelopes, then closes the body and the channel.
func (s *Store) pump(ctx context.Context, body io.ReadCloser, ch chan<- messaging.Envelope) {
	defer close(ch)
	defer func() { _ = body.Close() }()
	report := func(err error) {
		if s.onFrameError != nil {
			s.onFrameError(err)
		}
	}
	err := readSSE(body, maxEventBytes, func(ev sseEvent) bool {
		var env messaging.Envelope
		if err := json.Unmarshal([]byte(ev.Data), &env); err != nil {
			report(fmt.Errorf("httpstore: decode SSE frame: %w", err))
			return true
		}
		select {
		case ch <- env:
			return true
		case <-ctx.Done():
			return false
		}
	}, report)
	// A clean end of stream, a consumer that stopped, and a canceled context
	// are normal ends; anything else is worth reporting.
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, errStopped) && ctx.Err() == nil {
		report(fmt.Errorf("httpstore: stream ended: %w", err))
	}
}
