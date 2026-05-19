package memstore_test

import (
	"testing"

	"github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/memstore"
	"github.com/hollis-labs/go-messaging/messagingtest"
)

func TestMemstore_Contract(t *testing.T) {
	messagingtest.RunContract(t, func(t *testing.T) messaging.Store {
		return memstore.New()
	})
}

// TestMemstore_Router runs the authority-routing contract suite: a Router
// over memstore must satisfy the federation guarantees and remain a
// contract-conformant Store.
func TestMemstore_Router(t *testing.T) {
	messagingtest.RunRouterContract(t, func(t *testing.T) messaging.Store {
		return memstore.New()
	})
}
