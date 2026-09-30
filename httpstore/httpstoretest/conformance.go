package httpstoretest

import (
	"testing"

	messaging "github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/httpstore"
	"github.com/hollis-labs/go-messaging/memstore"
	"github.com/hollis-labs/go-messaging/messagingtest"
)

// ConformanceIdentity is the identity RunConformance configures on the
// client, for profiles whose Get and Thread assert one.
var ConformanceIdentity = messaging.Address{Kind: messaging.KindAgent, Authority: "test", ID: "contract-caller"}

// RunConformance runs the go-messaging contract suites against an
// httpstore.Store talking to a lenient reference server (memstore behind
// Handler) for profile p, with opts applied after the profile and
// ConformanceIdentity.
//
// RunContract always runs; operations the profile lists as unsupported are
// excluded with messagingtest.Without (OpInbox as "Inbox", OpSubscribe as
// "Subscribe"). RunRouterContract runs only when the profile supports both,
// because the Router suite reads through Inbox.
func RunConformance(t *testing.T, p httpstore.Profile, opts ...httpstore.Option) {
	t.Helper()
	factory := func(t *testing.T) messaging.Store {
		srv := NewServer(t, memstore.New(), nil, p)
		all := append([]httpstore.Option{httpstore.WithProfile(p), httpstore.WithIdentity(ConformanceIdentity)}, opts...)
		s, err := httpstore.New(srv.URL, all...)
		if err != nil {
			t.Fatalf("httpstoretest: httpstore.New: %v", err)
		}
		return s
	}
	var without []string
	if !p.Supports(httpstore.OpInbox) {
		without = append(without, "Inbox")
	}
	if !p.Supports(httpstore.OpSubscribe) {
		without = append(without, "Subscribe")
	}
	t.Run("RunContract", func(t *testing.T) {
		messagingtest.RunContract(t, factory, messagingtest.Without(without...))
	})
	if len(without) == 0 {
		t.Run("RunRouterContract", func(t *testing.T) {
			messagingtest.RunRouterContract(t, factory)
		})
	}
}
