package delivery_test

import (
	"testing"
	"time"

	"github.com/hollis-labs/go-messaging/delivery"
	"github.com/hollis-labs/go-messaging/deliverytest"
)

func TestMemoryStoreContract(t *testing.T) {
	deliverytest.RunStoreContract(t, func(t *testing.T) deliverytest.Harness {
		clock := deliverytest.NewFakeClock(time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC))
		return deliverytest.Harness{
			Store: delivery.NewMemoryStore(delivery.WithClock(clock)),
			Clock: clock,
		}
	})
}
