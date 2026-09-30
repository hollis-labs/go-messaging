package httpstore

// Op names one Store or Dispatcher operation, for Profile.Unsupported and
// for the WithRequestHook callback.
type Op string

// The operations a Store or Dispatcher performs on the wire.
const (
	OpSend      Op = "send"
	OpGet       Op = "get"
	OpInbox     Op = "inbox"
	OpThread    Op = "thread"
	OpConsume   Op = "consume"
	OpCancel    Op = "cancel"
	OpSubscribe Op = "subscribe"
	OpRequest   Op = "request"
)

// Profile selects a wire dialect. The two constructors describe dialects
// that exist today; they are descriptions of those servers, not a protocol
// this package defines. A zero-value Profile is usable: base path
// "/messages", no identity assertion, comma-joined filters, Consume carrying
// its recipient in ?as=, and every operation supported.
type Profile struct {
	// Name identifies the profile in error messages.
	Name string

	// BasePath is the route prefix on the server, for example "/messages"
	// or "/federation/v1/messages". Empty means "/messages".
	BasePath string

	// AssertAs makes the client claim an identity with ?as=<URN>: on Inbox
	// and Subscribe the claim is the recipient (?as equals ?to), and on Get
	// and Thread it is the configured WithIdentity, which is required
	// (ErrIdentityRequired otherwise, with no request sent).
	//
	// Consume always states its recipient: as ?as=<URN> by default, or in a
	// JSON body when ConsumeBody is set. That does not depend on AssertAs,
	// because Consume has nothing to assert other than its recipient.
	AssertAs bool

	// ConsumeBody sends Consume as a POST with a JSON body
	// {"recipient":"<URN>"} instead of ?as=<URN> (Torque).
	ConsumeBody bool

	// KindsRepeated encodes Filter.Kind and Filter.Channel as repeated
	// parameters (kind=a&kind=b, Torque). When false they are comma-joined
	// (kind=a,b, Tether).
	KindsRepeated bool

	// Unsupported lists operations the server does not carry. They fail
	// client-side with ErrUnsupported (which matches
	// messaging.ErrStoreUnavailable) without a request.
	Unsupported []Op

	// BlockingRequest makes Dispatcher.Request a single blocking
	// POST {base}/request?timeout=<Go duration> (Tether). When false,
	// Request is the generic Subscribe-then-Send helper over this Store.
	BlockingRequest bool
}

// TetherProfile describes the Tether daemon's go-messaging routes (as of
// Tether a441090): /messages, ?as= asserted on Inbox, Subscribe, Consume,
// Get and Thread, comma-joined kind filters, and a blocking request route.
// It is the default profile of New.
func TetherProfile() Profile {
	return Profile{
		Name:            "tether",
		BasePath:        "/messages",
		AssertAs:        true,
		BlockingRequest: true,
	}
}

// TorqueFederationProfile describes Torque's federation hop (as of Torque
// 06afdb9): /federation/v1/messages, repeated kind and channel parameters, a
// JSON Consume body, and no Inbox or Subscribe (the hop only carries Send,
// Get, Thread, Consume and Cancel). The peer is identified by its client
// certificate, not by a URL parameter; supply that transport with
// WithHTTPClient.
func TorqueFederationProfile() Profile {
	return Profile{
		Name:          "torque-federation",
		BasePath:      "/federation/v1/messages",
		ConsumeBody:   true,
		KindsRepeated: true,
		Unsupported:   []Op{OpInbox, OpSubscribe},
	}
}

// Supports reports whether the profile carries op.
func (p Profile) Supports(op Op) bool {
	for _, u := range p.Unsupported {
		if u == op {
			return false
		}
	}
	return true
}

func (p Profile) clone() Profile {
	p.Unsupported = append([]Op(nil), p.Unsupported...)
	return p
}
