package httpstore

import (
	"net/http"
	"time"

	messaging "github.com/hollis-labs/go-messaging"
)

// DefaultTimeout bounds each non-streaming call unless WithTimeout says
// otherwise.
const DefaultTimeout = 30 * time.Second

// Option configures New and NewDispatcher.
type Option func(*config)

type config struct {
	profile      Profile
	basePath     string
	client       *http.Client
	identity     messaging.Address
	hooks        []func(*http.Request, Op) error
	timeout      time.Duration
	onFrameError func(error)
}

// WithProfile selects the wire dialect. The default is TetherProfile().
func WithProfile(p Profile) Option {
	return func(c *config) { c.profile = p.clone() }
}

// WithBasePath overrides the profile's route prefix, for a server mounted
// somewhere else. It applies whatever the order of options. An empty path is
// ignored.
func WithBasePath(p string) Option {
	return func(c *config) {
		if p != "" {
			c.basePath = p
		}
	}
}

// WithHTTPClient sets the client used for every request. This is the
// transport seam: a mutual-TLS client, a pinned-certificate verifier or a
// unix-socket dialer plug in here, and this package neither knows nor cares
// which. A nil client is ignored.
//
// Timeouts: non-streaming calls are bounded by WithTimeout through a
// per-call context deadline; Subscribe and Dispatcher.Request use a copy of
// the client with Timeout set to zero, so a Timeout on the client you pass
// does not sever a long-lived stream or a blocking request. Bound those with
// the context.
func WithHTTPClient(c *http.Client) Option {
	return func(cfg *config) {
		if c != nil {
			cfg.client = c
		}
	}
}

// WithIdentity sets the identity the Store claims on Get and Thread when
// the profile asserts identity. Inbox, Subscribe and Consume claim the
// recipient they were called with and do not use it.
func WithIdentity(a messaging.Address) Option {
	return func(c *config) { c.identity = a }
}

// WithRequestHook registers a function called on every outgoing request
// just before it is sent, with the operation it belongs to. Use it to add
// authentication headers or to inject trace context (an OpenTelemetry
// propagator, say) without this package depending on either. A non-nil
// error aborts the call and is returned wrapped. Hooks run in registration
// order, and may be registered more than once.
func WithRequestHook(h func(*http.Request, Op) error) Option {
	return func(c *config) {
		if h != nil {
			c.hooks = append(c.hooks, h)
		}
	}
}

// WithTimeout bounds each non-streaming call (default DefaultTimeout). Zero
// disables the bound, leaving only the caller's context and the client's own
// Timeout. A negative value is ignored. Subscribe and blocking Request are
// never bounded by it.
func WithTimeout(d time.Duration) Option {
	return func(c *config) {
		if d >= 0 {
			c.timeout = d
		}
	}
}

// WithOnFrameError registers a callback for problems inside a Subscribe
// stream that would otherwise vanish: an event whose data is not a valid
// Envelope, an event over the size cap, or a stream that ends with a read
// error. It is called from the stream's goroutine and must not block.
func WithOnFrameError(f func(error)) Option {
	return func(c *config) { c.onFrameError = f }
}
