package mailbox

// Kind constants name the wire type of a message — what shape of
// payload rides on it and how receivers should react. Distinct from
// the message Type constants (semantic role) and UI envelope shapes.
const (
	KindRequest      = "request"
	KindReply        = "reply"
	KindNotification = "notification"
	KindHandoff      = "handoff"
)

// Channel constants name the transport bucket a message travels on.
// Distinct from message kind and UI envelope content type:
//   - chat  : in-session conversational traffic (primary ↔ secondary)
//   - inbox : async polled; triggers notifications on arrival
//   - alert : agent-triggered one-off ("report ready") — toast + inbox
const (
	ChannelChat  = "chat"
	ChannelInbox = "inbox"
	ChannelAlert = "alert"
)

// InboxFilter bundles optional inbox filters. An empty-string value
// on any field means "no constraint on that dimension". Callers
// construct a filter with whichever fields they want to narrow by.
type InboxFilter struct {
	Status  string
	Channel string
	Kind    string
}

// Message is a single agent-to-agent (or agent-to-user) message row.
// Addressing is scoped to a (session_id, agent_id) tuple on both ends
// so that two instances of the same agent running in different
// sessions have distinct inboxes.
//
// ThreadID is TEXT-sized with no fixed-length assumptions.
type Message struct {
	ID            string  `json:"id"`
	FromSessionID string  `json:"from_session_id"`
	FromAgentID   string  `json:"from_agent_id"`
	ToSessionID   string  `json:"to_session_id"`
	ToAgentID     string  `json:"to_agent_id"`
	ThreadID      string  `json:"thread_id"`
	ReplyTo       string  `json:"reply_to"`
	Type          string  `json:"type"`
	Subject       string  `json:"subject"`
	Body          string  `json:"body"`
	Metadata      string  `json:"metadata"`
	Priority      int     `json:"priority"`
	Status        string  `json:"status"`
	Channel       string  `json:"channel"`
	Kind          string  `json:"kind"`
	PayloadJSON   string  `json:"payload_json"`
	CreatedAt     string  `json:"created_at"`
	ReadAt        *string `json:"read_at"`
	ResolvedAt    *string `json:"resolved_at"`
}

// SendInput is the caller-supplied portion of a new message. Defaults
// for missing fields are filled in by Store.Send — see the Store
// interface doc for the full default list.
type SendInput struct {
	FromSessionID string
	FromAgentID   string
	ToSessionID   string
	ToAgentID     string
	// Channel is optional; empty defaults to ChannelChat. The Store
	// validates against the CHECK constraint so unknown values reject
	// at insert time rather than silently rewriting themselves.
	Channel string
	// Kind is optional; empty defaults to KindNotification. Same CHECK
	// semantics as Channel — invalid values reject at insert.
	Kind string
	// PayloadJSON carries kind-specific structured payload. Empty defaults
	// to "{}".
	PayloadJSON string
	Type        string
	Subject     string
	Body        string
	ThreadID    string
	ReplyTo     string
	Metadata    string
	Priority    int
	// RegisterAs is an opaque, host-defined hint for optional
	// auto-registration. It is ignored when FromAgentID already resolves
	// or when Service has no registrar.
	RegisterAs string
}

// Message type constants (wire-level "type" column on each row). These
// name the message's semantic role; they are distinct from UI envelope
// shapes and from Kind (wire behavior).
const (
	TypeMessage      = "message"
	TypeHelpRequest  = "help_request"
	TypeDirective    = "directive"
	TypeStatusUpdate = "status_update"
	TypeHandoff      = "handoff"
)

// Status constants for the message row's lifecycle column.
const (
	StatusUnread       = "unread"
	StatusRead         = "read"
	StatusAcknowledged = "acknowledged"
	StatusResolved     = "resolved"
)

// MaxRecentLimit is the absolute upper bound the Store applies to
// Recent(limit). Larger limits are clamped to bound memory use.
const MaxRecentLimit = 100

// DefaultRecentLimit is the default row count for Recent(0). The Store
// and Service layers agree on the same default so that direct store
// calls and service-wrapped calls return the same row count.
const DefaultRecentLimit = 20
