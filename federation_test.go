package messaging_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/memstore"
)

func agent(authority, id string) messaging.Address {
	return messaging.Address{Kind: messaging.KindAgent, Authority: authority, ID: id}
}

// TestRouter_Dispatcher exercises a federated request/reply: a Dispatcher
// wrapping a Router issues a request to a foreign authority and receives the
// reply back on the local authority — the same Dispatcher API as the
// non-federated case, with routing handled entirely by the Router.
func TestRouter_Dispatcher(t *testing.T) {
	local, foreign := memstore.New(), memstore.New()
	router := messaging.NewRouter(local, "local")
	if err := router.Register("remote", foreign); err != nil {
		t.Fatal(err)
	}
	disp := messaging.NewDispatcher(router)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	caller := agent("local", "caller")
	callee := agent("remote", "callee")

	// The responder lives on the foreign authority and replies through the
	// same Router, so its response routes back to the local authority.
	go func() {
		sub, err := router.Subscribe(ctx, callee, messaging.Filter{
			Kind: []messaging.Kind{messaging.MsgKindRequest},
		})
		if err != nil {
			return
		}
		for req := range sub {
			_, _ = disp.Reply(ctx, req, json.RawMessage(`"pong"`))
			return
		}
	}()
	time.Sleep(10 * time.Millisecond)

	resp, err := disp.Request(ctx, messaging.Envelope{
		From:    caller,
		To:      callee,
		Payload: json.RawMessage(`"ping"`),
	})
	if err != nil {
		t.Fatalf("federated Request: %v", err)
	}
	if string(resp.Payload) != `"pong"` {
		t.Errorf("resp payload = %s, want \"pong\"", resp.Payload)
	}

	// The request lives in the foreign Store; the reply in the local Store.
	if _, err := foreign.Get(ctx, resp.InReplyTo); err != nil {
		t.Errorf("request envelope not in foreign Store: %v", err)
	}
	if _, err := local.Get(ctx, resp.ID); err != nil {
		t.Errorf("response envelope not in local Store: %v", err)
	}
}

// TestRouter_IDKeyedOpsAreLocal documents that Get and Cancel — which take
// an envelope ID with no authority — are served from the local Store even
// when a foreign route exists.
func TestRouter_IDKeyedOpsAreLocal(t *testing.T) {
	local, foreign := memstore.New(), memstore.New()
	router := messaging.NewRouter(local, "local")
	if err := router.Register("remote", foreign); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// An envelope created directly in the foreign Store is not reachable via
	// the Router's ID-keyed Get; only the local Store is consulted.
	sent, err := foreign.Send(ctx, messaging.Envelope{
		Kind: messaging.MsgKindNotice,
		From: agent("local", "a"),
		To:   agent("remote", "b"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Get(ctx, sent.ID); !errors.Is(err, messaging.ErrNotFound) {
		t.Errorf("router.Get of foreign-only ID: got %v, want ErrNotFound", err)
	}
}

// ExampleRouter shows the standalone-to-federated progression: the same
// Router code path serves a purely local install and, once a foreign route
// is registered, a federated one.
func ExampleRouter() {
	// A standalone install: one local Store, no foreign routes registered.
	router := messaging.NewRouter(memstore.New(), "hq")
	ctx := context.Background()

	// Everything resolves locally — messaging works with zero extra config.
	fmt.Println("hq is local:", router.IsLocal("hq"))
	fmt.Println("branch is local:", router.IsLocal("branch"))

	// Federate by registering a foreign authority. In production the second
	// argument is typically an HTTP-backed Store reaching the other host.
	_ = router.Register("branch", memstore.New())
	fmt.Println("branch is local:", router.IsLocal("branch"))
	fmt.Println("routes:", router.Authorities())

	// A notice addressed to the foreign authority is now dispatched there.
	_, _ = router.Send(ctx, messaging.Envelope{
		Kind: messaging.MsgKindNotice,
		From: messaging.Address{Kind: messaging.KindUser, Authority: "hq", ID: "alice"},
		To:   messaging.Address{Kind: messaging.KindAgent, Authority: "branch", ID: "bob"},
	})

	// Output:
	// hq is local: true
	// branch is local: true
	// branch is local: false
	// routes: [branch]
}
