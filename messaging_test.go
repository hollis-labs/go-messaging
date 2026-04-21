package messaging

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestEnvelope_JSONRoundTrip(t *testing.T) {
	from := Address{Kind: KindAgent, Authority: "nanite", ID: "chat-1"}
	to := Address{Kind: KindAgent, Authority: "agent-mux", ID: "sess-abc", SubID: "primary"}
	created := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	delivered := created.Add(time.Second)

	orig := Envelope{
		ID:          "01925a3c-0000-7000-8000-000000000001",
		Kind:        MsgKindRequest,
		Channel:     "chat",
		From:        from,
		To:          to,
		ThreadID:    "thread-42",
		InReplyTo:   "",
		Payload:     json.RawMessage(`{"action":"review","task":"T-1"}`),
		ContentType: "application/json",
		Metadata:    map[string]string{"trace_id": "abc"},
		CreatedAt:   created,
		DeliveredAt: &delivered,
		ConsumedAt:  nil,
	}

	b, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// Payload should serialize inline (not base64) because json.RawMessage.
	if !containsAll(string(b), `"payload":{"action":"review","task":"T-1"}`) {
		t.Errorf("Payload not inline: %s", string(b))
	}
	// DeliveredAt non-null, ConsumedAt null.
	if !containsAll(string(b), `"consumed_at":null`) {
		t.Errorf("ConsumedAt should be null: %s", string(b))
	}

	var got Envelope
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got.ID != orig.ID || got.Kind != orig.Kind || got.Channel != orig.Channel {
		t.Errorf("core fields mismatch: %+v", got)
	}
	if got.From != orig.From || got.To != orig.To {
		t.Errorf("address round-trip failed: from=%v to=%v", got.From, got.To)
	}
	if !got.CreatedAt.Equal(orig.CreatedAt) {
		t.Errorf("CreatedAt: got %v, want %v", got.CreatedAt, orig.CreatedAt)
	}
	if got.DeliveredAt == nil || !got.DeliveredAt.Equal(delivered) {
		t.Errorf("DeliveredAt: got %v, want %v", got.DeliveredAt, delivered)
	}
	if got.ConsumedAt != nil {
		t.Errorf("ConsumedAt should be nil, got %v", got.ConsumedAt)
	}
	if string(got.Payload) != string(orig.Payload) {
		t.Errorf("Payload: got %s, want %s", got.Payload, orig.Payload)
	}
	if got.Metadata["trace_id"] != "abc" {
		t.Errorf("Metadata not round-tripped: %v", got.Metadata)
	}
}

func TestEnvelope_JSONOmitEmpty(t *testing.T) {
	minimal := Envelope{
		ID:        "test-1",
		Kind:      MsgKindNotice,
		From:      Address{Kind: KindAgent, Authority: "a", ID: "1"},
		To:        Address{Kind: KindAgent, Authority: "b", ID: "2"},
		CreatedAt: time.Date(2026, 4, 20, 0, 0, 0, 0, time.UTC),
	}
	b, err := json.Marshal(minimal)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	s := string(b)
	// Optional fields should be absent when empty.
	for _, forbidden := range []string{`"channel":`, `"thread_id":`, `"in_reply_to":`, `"metadata":`} {
		if containsAll(s, forbidden) {
			t.Errorf("unexpected field in minimal envelope JSON: %q in %s", forbidden, s)
		}
	}
}

func containsAll(haystack string, needles ...string) bool {
	for _, n := range needles {
		if !strings.Contains(haystack, n) {
			return false
		}
	}
	return true
}

func TestFilter_Matches(t *testing.T) {
	env := Envelope{
		Kind:     MsgKindRequest,
		Channel:  "chat",
		ThreadID: "T1",
	}

	cases := []struct {
		name   string
		filter Filter
		want   bool
	}{
		{"empty matches all", Filter{}, true},
		{"kind match", Filter{Kind: []Kind{MsgKindRequest}}, true},
		{"kind no-match", Filter{Kind: []Kind{MsgKindResponse}}, false},
		{"kind OR match", Filter{Kind: []Kind{MsgKindResponse, MsgKindRequest}}, true},
		{"channel match", Filter{Channel: []Channel{"chat"}}, true},
		{"channel no-match", Filter{Channel: []Channel{"inbox"}}, false},
		{"thread match", Filter{ThreadID: "T1"}, true},
		{"thread no-match", Filter{ThreadID: "T2"}, false},
		{"AND: all match", Filter{Kind: []Kind{MsgKindRequest}, Channel: []Channel{"chat"}, ThreadID: "T1"}, true},
		{"AND: kind mismatch", Filter{Kind: []Kind{MsgKindResponse}, Channel: []Channel{"chat"}, ThreadID: "T1"}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.filter.Matches(env); got != tc.want {
				t.Errorf("Matches = %v, want %v", got, tc.want)
			}
		})
	}
}
