package messaging

import (
	"errors"
	"strings"
)

// ErrInvalidAddress indicates a URN string is malformed or references an
// unknown AddressKind.
var ErrInvalidAddress = errors.New("invalid address")

const urnScheme = "msg://"

var validKinds = map[AddressKind]struct{}{
	KindAgent:    {},
	KindUser:     {},
	KindService:  {},
	KindSession:  {},
	KindWorkflow: {},
	KindGroup:    {},
}

// URN returns the canonical string form of the Address:
//
//	msg://<kind>/<authority>/<id>[/<subid>]
//
// Panics only on a genuinely malformed Address (empty Kind/Authority/ID).
// Callers should validate via ParseURN(a.URN()) == nil as a sanity check.
func (a Address) URN() string {
	var sb strings.Builder
	sb.WriteString(urnScheme)
	sb.WriteString(string(a.Kind))
	sb.WriteByte('/')
	sb.WriteString(a.Authority)
	sb.WriteByte('/')
	sb.WriteString(a.ID)
	if a.SubID != "" {
		sb.WriteByte('/')
		sb.WriteString(a.SubID)
	}
	return sb.String()
}

// ParseURN parses a canonical messaging URN into an Address.
// Returns ErrInvalidAddress for malformed strings or unknown AddressKinds.
func ParseURN(s string) (Address, error) {
	if !strings.HasPrefix(s, urnScheme) {
		return Address{}, ErrInvalidAddress
	}
	rest := s[len(urnScheme):]
	parts := strings.Split(rest, "/")
	// Expect 3 (kind/authority/id) or 4 (with subid) parts.
	if len(parts) < 3 || len(parts) > 4 {
		return Address{}, ErrInvalidAddress
	}
	for _, p := range parts {
		if p == "" {
			return Address{}, ErrInvalidAddress
		}
	}
	kind := AddressKind(parts[0])
	if _, ok := validKinds[kind]; !ok {
		return Address{}, ErrInvalidAddress
	}
	a := Address{
		Kind:      kind,
		Authority: parts[1],
		ID:        parts[2],
	}
	if len(parts) == 4 {
		a.SubID = parts[3]
	}
	return a, nil
}
