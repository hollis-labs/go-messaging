package messagingtest_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/memstore"
	"github.com/hollis-labs/go-messaging/messagingtest"
)

// noInboxStore is a memstore whose Inbox and Subscribe are unavailable, the
// shape of a federation hop that never exposes them.
type noInboxStore struct{ *memstore.Store }

func (noInboxStore) Inbox(context.Context, messaging.Address, messaging.Filter) ([]messaging.Envelope, error) {
	return nil, fmt.Errorf("%w: no inbox", messaging.ErrStoreUnavailable)
}

func (noInboxStore) Subscribe(context.Context, messaging.Address, messaging.Filter) (<-chan messaging.Envelope, error) {
	return nil, fmt.Errorf("%w: no subscribe", messaging.ErrStoreUnavailable)
}

// ranNames runs RunContract with opts and returns the sub-test names whose
// factory was called, i.e. the ones that were not skipped.
func ranNames(t *testing.T, mk func() messaging.Store, opts ...messagingtest.ContractOption) map[string]bool {
	t.Helper()
	var mu sync.Mutex
	ran := map[string]bool{}
	t.Run("contract", func(t *testing.T) {
		messagingtest.RunContract(t, func(t *testing.T) messaging.Store {
			mu.Lock()
			ran[t.Name()] = true
			mu.Unlock()
			return mk()
		}, opts...)
	})
	return ran
}

func TestWithout_SkipsDependentSubtests(t *testing.T) {
	inbox := []string{
		"Inbox atomic delivery",
		"Inbox chronological + tie-break",
		"Thread chronological, no side effects",
	}
	subscribe := []string{
		"Subscribe live-only",
		"Subscribe filters to recipient only",
		"Subscribe ctx cancel closes channel",
		"Dispatcher.Request round-trip",
		"Dispatcher.Request times out",
	}
	always := []string{
		"Send assigns ID + CreatedAt",
		"Send rejects preset lifecycle",
		"Get returns ErrNotFound for missing",
		"Consume sets ConsumedAt, idempotent",
		"Cancel marks dead, idempotent",
		"Cancel NotFound",
	}
	mk := func() messaging.Store { return noInboxStore{memstore.New()} }

	t.Run("Inbox and Subscribe", func(t *testing.T) {
		ran := ranNames(t, mk, messagingtest.Without("Inbox", "Subscribe"))
		for _, n := range always {
			if !hasSuffix(ran, n) {
				t.Errorf("sub-test %q should still run", n)
			}
		}
		for _, n := range append(append([]string{}, inbox...), subscribe...) {
			if hasSuffix(ran, n) {
				t.Errorf("sub-test %q should be skipped", n)
			}
		}
	})

	t.Run("Inbox only", func(t *testing.T) {
		ran := ranNames(t, func() messaging.Store { return memstore.New() }, messagingtest.Without("Inbox"))
		for _, n := range inbox {
			if hasSuffix(ran, n) {
				t.Errorf("sub-test %q should be skipped", n)
			}
		}
		for _, n := range subscribe {
			if !hasSuffix(ran, n) {
				t.Errorf("sub-test %q should still run with only Inbox excluded", n)
			}
		}
	})

	t.Run("Subscribe only", func(t *testing.T) {
		ran := ranNames(t, func() messaging.Store { return memstore.New() }, messagingtest.Without("Subscribe"))
		for _, n := range subscribe {
			if hasSuffix(ran, n) {
				t.Errorf("sub-test %q should be skipped", n)
			}
		}
		for _, n := range inbox {
			if !hasSuffix(ran, n) {
				t.Errorf("sub-test %q should still run with only Subscribe excluded", n)
			}
		}
	})

	t.Run("no options runs everything", func(t *testing.T) {
		ran := ranNames(t, func() messaging.Store { return memstore.New() })
		for _, n := range append(append(append([]string{}, always...), inbox...), subscribe...) {
			if !hasSuffix(ran, n) {
				t.Errorf("sub-test %q should run without options", n)
			}
		}
	})
}

// hasSuffix reports whether a recorded sub-test path ends in the sub-test
// name (t.Name() rewrites spaces to underscores).
func hasSuffix(ran map[string]bool, name string) bool {
	want := "/" + strings.NewReplacer(" ", "_").Replace(name)
	for k := range ran {
		if strings.HasSuffix(k, want) {
			return true
		}
	}
	return false
}
