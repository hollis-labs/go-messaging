package messaging

import (
	"encoding/json"
	"time"
)

// AddressKind categorizes what an Address points at.
type AddressKind string

const (
	KindAgent    AddressKind = "agent"
	KindUser     AddressKind = "user"
	KindService  AddressKind = "service"
	KindSession  AddressKind = "session"
	KindWorkflow AddressKind = "workflow"
	KindGroup    AddressKind = "group"
)

// Address is the typed in-memory form of a messaging URN.
//
// Canonical wire form: msg://<kind>/<authority>/<id>[/<subid>]
type Address struct {
	Kind      AddressKind
	Authority string
	ID        string
	SubID     string // optional
}

// IsZero reports whether the Address is uninitialized.
func (a Address) IsZero() bool {
	return a.Kind == "" && a.Authority == "" && a.ID == "" && a.SubID == ""
}

// MarshalJSON encodes Address as its canonical URN string. A zero
// Address encodes as "" rather than as the malformed "msg:////" that
// URN() composes from empty parts -- see UnmarshalJSON for why the
// asymmetry mattered.
func (a Address) MarshalJSON() ([]byte, error) {
	if a.IsZero() {
		return json.Marshal("")
	}
	return json.Marshal(a.URN())
}

// UnmarshalJSON decodes a canonical URN string into an Address. The
// empty string decodes to the zero Address.
//
// Accepting "" is what makes a zero Address round-trip. Before this,
// MarshalJSON emitted "msg:////" for a zero value and ParseURN then
// rejected it, so any struct with an optional Address field could be
// encoded and never decoded again. `json:"...,omitempty"` does not save
// such a field either -- omitempty has no effect on a struct, so the
// field is always written. delivery.RecipientDelivery and
// delivery.Attempt both hit this through BindingTarget and were
// undecodable whenever no binding was set, which is every response
// Tether's POST /messages/{id}/ack|nack returns.
func (a *Address) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s == "" {
		*a = Address{}
		return nil
	}
	parsed, err := ParseURN(s)
	if err != nil {
		return err
	}
	*a = parsed
	return nil
}

// Kind: what the envelope IS (routable, closed enum). Named MsgKind*
// constants to disambiguate from AddressKind (which uses Kind* prefix).
type Kind string

const (
	MsgKindRequest      Kind = "request"
	MsgKindResponse     Kind = "response"
	MsgKindNotice       Kind = "notice"
	MsgKindStatusUpdate Kind = "status_update"
	MsgKindHandoff      Kind = "handoff"
	MsgKindEscalation   Kind = "escalation"
)

// Channel is an opaque UX-layer pass-through. Apps define vocabulary;
// the shared package never interprets it.
type Channel string

// Envelope is the core message type.
//
// Wire format: canonical JSON. Payload serializes inline (json.RawMessage).
// Pointer time fields (DeliveredAt, ConsumedAt) serialize as null when nil.
type Envelope struct {
	ID          string            `json:"id"`
	Kind        Kind              `json:"kind"`
	Channel     Channel           `json:"channel,omitempty"`
	From        Address           `json:"from"`
	To          Address           `json:"to"`
	ThreadID    string            `json:"thread_id,omitempty"`
	InReplyTo   string            `json:"in_reply_to,omitempty"`
	Payload     json.RawMessage   `json:"payload,omitempty"`
	ContentType string            `json:"content_type,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	DeliveredAt *time.Time        `json:"delivered_at"`
	ConsumedAt  *time.Time        `json:"consumed_at"`
}

// Filter narrows Inbox/Thread/Subscribe result sets.
// Set fields are AND-combined; within a slice, values are OR-combined.
//
//	Filter{Kind: []Kind{MsgKindRequest, MsgKindHandoff}, ThreadID: "T1"}
//
// matches envelopes in thread T1 whose Kind is request OR handoff.
type Filter struct {
	Kind     []Kind    `json:"kind,omitempty"`
	Channel  []Channel `json:"channel,omitempty"`
	ThreadID string    `json:"thread_id,omitempty"`
	Limit    int       `json:"limit,omitempty"`
}

// Matches reports whether env satisfies the filter.
func (f Filter) Matches(env Envelope) bool {
	if len(f.Kind) > 0 {
		ok := false
		for _, k := range f.Kind {
			if env.Kind == k {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if len(f.Channel) > 0 {
		ok := false
		for _, c := range f.Channel {
			if env.Channel == c {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if f.ThreadID != "" && env.ThreadID != f.ThreadID {
		return false
	}
	return true
}
