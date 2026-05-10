// Package main demonstrates the canonical Request/Reply flow from
// go-messaging: construct an in-memory Store, wrap it with a
// Dispatcher, run a responder goroutine that subscribes to incoming
// requests, and issue a Request that blocks until the correlated
// Response arrives or the context expires.
//
// Run with:
//
//	go run ./examples/request-reply
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	messaging "github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/memstore"
)

func main() {
	store := memstore.New()
	dispatcher := messaging.NewDispatcher(store)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	alice := messaging.Address{Kind: messaging.KindAgent, Authority: "app", ID: "alice"}
	bob := messaging.Address{Kind: messaging.KindAgent, Authority: "app", ID: "bob"}

	// Bob subscribes to incoming requests addressed to him and replies
	// to the first one with a static payload.
	ready := make(chan struct{})
	go func() {
		sub, err := store.Subscribe(ctx, bob, messaging.Filter{
			Kind: []messaging.Kind{messaging.MsgKindRequest},
		})
		if err != nil {
			log.Printf("subscribe: %v", err)
			close(ready)
			return
		}
		close(ready)
		for req := range sub {
			if req.To != bob {
				continue
			}
			if _, err := dispatcher.Reply(ctx, req, json.RawMessage(`{"status":"ok"}`)); err != nil {
				log.Printf("reply: %v", err)
			}
			return
		}
	}()
	<-ready
	// Small delay so Subscribe's internal registration is visible to
	// the next Send. memstore's Subscribe is live-only by design.
	time.Sleep(10 * time.Millisecond)

	// Alice sends a Request and blocks until Bob replies or ctx times out.
	resp, err := dispatcher.Request(ctx, messaging.Envelope{
		From:    alice,
		To:      bob,
		Payload: json.RawMessage(`{"ask":"health"}`),
	})
	if err != nil {
		log.Fatalf("request failed: %v", err)
	}

	fmt.Printf("response from %s: %s\n", resp.From.URN(), string(resp.Payload))
}
