package messaging

import (
	"testing"
)

func TestAddress_URN_RoundTrip(t *testing.T) {
	cases := []struct {
		name string
		addr Address
		urn  string
	}{
		{
			name: "agent with subid",
			addr: Address{Kind: KindAgent, Authority: "agent-mux", ID: "sess-abc", SubID: "primary"},
			urn:  "msg://agent/agent-mux/sess-abc/primary",
		},
		{
			name: "agent without subid",
			addr: Address{Kind: KindAgent, Authority: "nanite", ID: "chat-a"},
			urn:  "msg://agent/nanite/chat-a",
		},
		{
			name: "user",
			addr: Address{Kind: KindUser, Authority: "nanite", ID: "chrispian"},
			urn:  "msg://user/nanite/chrispian",
		},
		{
			name: "service",
			addr: Address{Kind: KindService, Authority: "clockwork", ID: "scheduler"},
			urn:  "msg://service/clockwork/scheduler",
		},
		{
			name: "session",
			addr: Address{Kind: KindSession, Authority: "agent-mux", ID: "sess-xyz"},
			urn:  "msg://session/agent-mux/sess-xyz",
		},
		{
			name: "workflow",
			addr: Address{Kind: KindWorkflow, Authority: "agent-mux", ID: "wf-20260420-01"},
			urn:  "msg://workflow/agent-mux/wf-20260420-01",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.addr.URN()
			if got != tc.urn {
				t.Errorf("URN() = %q, want %q", got, tc.urn)
			}
			parsed, err := ParseURN(tc.urn)
			if err != nil {
				t.Fatalf("ParseURN(%q) error: %v", tc.urn, err)
			}
			if parsed != tc.addr {
				t.Errorf("ParseURN(%q) = %+v, want %+v", tc.urn, parsed, tc.addr)
			}
		})
	}
}

func TestParseURN_Errors(t *testing.T) {
	bad := []string{
		"",
		"not-a-urn",
		"http://agent/foo/bar",
		"msg://unknown/foo/bar",
		"msg://agent",
		"msg://agent/",
		"msg://agent/foo",
		"msg://agent/foo/",
		"msg://agent/foo/bar/baz/extra",
	}
	for _, s := range bad {
		t.Run(s, func(t *testing.T) {
			if _, err := ParseURN(s); err == nil {
				t.Errorf("ParseURN(%q) expected error, got nil", s)
			}
		})
	}
}
