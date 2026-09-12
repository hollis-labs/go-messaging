package messaging

import (
	"encoding/json"
	"testing"
)

// A zero Address must survive a JSON round-trip.
//
// It did not before: MarshalJSON composed URN() from empty parts, giving
// "msg:////", and UnmarshalJSON handed that to ParseURN, which rejected
// it. Any struct carrying an optional Address could therefore be encoded
// and never decoded again — and `omitempty` does not rescue such a field,
// because it has no effect on a struct value.
func TestAddressZeroRoundTrips(t *testing.T) {
	var zero Address

	b, err := json.Marshal(zero)
	if err != nil {
		t.Fatalf("marshal zero Address: %v", err)
	}
	if string(b) != `""` {
		t.Errorf("zero Address marshaled to %s, want `\"\"` — %q was the malformed form that could not be parsed back", b, "msg:////")
	}

	var back Address
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal %s back into an Address: %v — this is the round-trip that was broken", b, err)
	}
	if !back.IsZero() {
		t.Errorf("round-tripped to %+v, want the zero Address", back)
	}
}

func TestAddressRoundTripsPopulated(t *testing.T) {
	for _, urn := range []string{
		"msg://agent/agent-mux/agt_aaaaaaaaaa",
		"msg://session/local/sess-1",
		"msg://user/local/me",
	} {
		orig, err := ParseURN(urn)
		if err != nil {
			t.Fatalf("ParseURN(%q): %v", urn, err)
		}
		b, err := json.Marshal(orig)
		if err != nil {
			t.Fatalf("marshal %q: %v", urn, err)
		}
		if string(b) != `"`+urn+`"` {
			t.Errorf("marshal(%q) = %s, want the canonical URN unchanged", urn, b)
		}
		var back Address
		if err := json.Unmarshal(b, &back); err != nil {
			t.Fatalf("unmarshal %s: %v", b, err)
		}
		if back != orig {
			t.Errorf("round-trip of %q gave %+v, want %+v", urn, back, orig)
		}
	}
}

// Accepting "" must not widen into accepting anything else malformed —
// "" is the zero form and nothing more.
func TestAddressStillRejectsMalformed(t *testing.T) {
	for _, bad := range []string{
		`"msg:////"`,    // the old zero encoding: still not a valid address
		`"msg://"`,      // scheme only
		`"agent/x/y"`,   // no scheme
		`"msg://agent"`, // too few segments
		`"   "`,         // whitespace is not empty
	} {
		var a Address
		if err := json.Unmarshal([]byte(bad), &a); err == nil {
			t.Errorf("unmarshal(%s) succeeded as %+v; only \"\" may decode to the zero Address", bad, a)
		}
	}
}
