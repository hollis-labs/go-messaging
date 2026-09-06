package messaging_test

import (
	"os"
	"strings"
	"testing"
)

func TestMessagingVNextContractDocumentPinsG01Acceptance(t *testing.T) {
	data, err := os.ReadFile("CONTRACTS.md")
	if err != nil {
		t.Fatalf("read CONTRACTS.md: %v", err)
	}
	doc := string(data)
	for _, want := range []string{
		"go-messaging` | Must not import `agentkit`",
		"Root Store compatibility",
		"Mailbox compatibility",
		"Reliable delivery contract",
		"sender-scoped idempotency key",
		"Lease, fencing, retry, and dead-letter rules",
		"Rooms and group fanout",
		"no fake provider ID",
		"root `Store.Inbox` remains destructive",
		"`mailbox.Service.Ack` remains read/attention state",
		"Nanite",
		"go-tether-client",
		"Torque",
		"at-least-once delivery with idempotent effects",
	} {
		if !strings.Contains(doc, want) {
			t.Fatalf("CONTRACTS.md missing %q", want)
		}
	}
}
