package httpstore_test

import (
	"context"
	"fmt"
	"net/http/httptest"

	messaging "github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/httpstore"
	"github.com/hollis-labs/go-messaging/httpstore/httpstoretest"
	"github.com/hollis-labs/go-messaging/memstore"
)

// A Store client for a Tether-style daemon. Here the daemon is the reference
// server from httpstoretest; in an application the URL is the real one.
func Example() {
	srv := httptest.NewServer(httpstoretest.Handler(memstore.New(), nil, httpstore.TetherProfile()))
	defer srv.Close()

	me := messaging.Address{Kind: messaging.KindAgent, Authority: "app", ID: "alice"}
	peer := messaging.Address{Kind: messaging.KindAgent, Authority: "app", ID: "bob"}

	store, err := httpstore.New(srv.URL, httpstore.WithIdentity(me))
	if err != nil {
		panic(err)
	}
	ctx := context.Background()

	sent, err := store.Send(ctx, messaging.Envelope{Kind: messaging.MsgKindNotice, From: me, To: peer})
	if err != nil {
		panic(err)
	}
	got, err := store.Get(ctx, sent.ID)
	if err != nil {
		panic(err)
	}
	inbox, err := store.Inbox(ctx, peer, messaging.Filter{})
	if err != nil {
		panic(err)
	}
	fmt.Println(got.Kind, got.To.ID, len(inbox))
	// Output: notice bob 1
}
