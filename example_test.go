package messaging_test

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/memstore"
)

// Example demonstrates the canonical usage pattern: construct a Dispatcher,
// set up a responder, issue a Request, receive the Response.
func Example() {
	store := memstore.New()
	dispatcher := messaging.NewDispatcher(store)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	A := messaging.Address{Kind: messaging.KindAgent, Authority: "app", ID: "alice"}
	B := messaging.Address{Kind: messaging.KindAgent, Authority: "app", ID: "bob"}

	// Bob subscribes to requests and auto-responds.
	go func() {
		sub, err := store.Subscribe(ctx, messaging.Filter{
			Kind: []messaging.Kind{messaging.MsgKindRequest},
		})
		if err != nil {
			return
		}
		for req := range sub {
			if req.To != B {
				continue
			}
			_, _ = dispatcher.Reply(ctx, req, json.RawMessage(`{"status":"ok"}`))
			return
		}
	}()
	time.Sleep(10 * time.Millisecond) // let subscribe establish

	// Alice sends a Request; blocks until Bob replies or ctx expires.
	resp, err := dispatcher.Request(ctx, messaging.Envelope{
		From:    A,
		To:      B,
		Payload: json.RawMessage(`{"ask":"health"}`),
	})
	if err != nil {
		fmt.Println("request failed:", err)
		return
	}
	fmt.Printf("got response: %s\n", string(resp.Payload))
	// Output: got response: {"status":"ok"}
}
