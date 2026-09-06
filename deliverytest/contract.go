// Package deliverytest provides a reusable conformance suite for
// delivery.Store implementations.
package deliverytest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	messaging "github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/delivery"
)

// FakeClock is a deterministic, goroutine-safe test clock.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock starts a deterministic clock at start.UTC().
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start.UTC()}
}

// Now returns the current fake time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the fake clock forward.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Set assigns the fake clock.
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t.UTC()
}

// Harness provides a fresh Store and its deterministic clock for each subtest.
type Harness struct {
	Store delivery.Store
	Clock *FakeClock
}

// Factory constructs a fresh reliable-delivery store for one subtest.
type Factory func(t *testing.T) Harness

// RunStoreContract exercises the Messaging vNext reliable-delivery contract.
func RunStoreContract(t *testing.T, factory Factory) {
	t.Helper()

	t.Run("immutable message identity and frozen recipient obligations", func(t *testing.T) {
		h := factory(t)
		res := mustEnqueue(t, h.Store, basicRequest("idem-immutable", agent("alice"), agent("bob"), agent("carol")))
		if res.Message.ID == "" || res.Message.CreatedAt.IsZero() || res.Message.Digest == "" {
			t.Fatalf("message identity not assigned: %+v", res.Message)
		}
		if len(res.Deliveries) != 2 {
			t.Fatalf("deliveries = %d, want 2", len(res.Deliveries))
		}
		origPayload := string(res.Message.Payload)
		res.Message.Payload[0] = 'X'
		got, err := h.Store.GetMessage(context.Background(), res.Message.ID)
		must(t, err)
		if string(got.Payload) != origPayload {
			t.Fatalf("message payload was mutable through result copy: %s", got.Payload)
		}
		listed, err := h.Store.ListDeliveries(context.Background(), delivery.Filter{MessageID: got.ID})
		must(t, err)
		if len(listed) != 2 || listed[0].MessageID != got.ID || listed[1].MessageID != got.ID {
			t.Fatalf("frozen obligations not listable by message: %+v", listed)
		}
		for _, d := range listed {
			receipts, err := h.Store.Receipts(context.Background(), d.ID)
			must(t, err)
			if len(receipts) != 1 || receipts[0].Stage != delivery.StagePersisted || receipts[0].AttemptID != "" {
				t.Fatalf("persisted receipt missing or collapsed: %+v", receipts)
			}
		}
	})

	t.Run("sender scoped idempotency and digest conflicts", func(t *testing.T) {
		h := factory(t)
		req := basicRequest("same", agent("alice"), agent("bob"))
		first := mustEnqueue(t, h.Store, req)
		dupe := mustEnqueue(t, h.Store, req)
		if !dupe.Duplicate || dupe.Message.ID != first.Message.ID || dupe.Message.Digest != first.Message.Digest {
			t.Fatalf("idempotent replay mismatch: first=%+v dupe=%+v", first.Message, dupe.Message)
		}

		sameKeyOtherSender := req
		sameKeyOtherSender.From = agent("mallory")
		other := mustEnqueue(t, h.Store, sameKeyOtherSender)
		if other.Message.ID == first.Message.ID {
			t.Fatal("idempotency key was not scoped by sender")
		}

		conflict := req
		conflict.Payload = []byte(`{"body":"different"}`)
		_, err := h.Store.Enqueue(context.Background(), conflict)
		var digestErr *delivery.DigestConflictError
		if !errors.As(err, &digestErr) || !errors.Is(err, delivery.ErrDigestConflict) {
			t.Fatalf("got %T %[1]v, want DigestConflictError", err)
		}
		if digestErr.ExistingMessageID != first.Message.ID || digestErr.ExistingDigest == digestErr.NewDigest {
			t.Fatalf("bad conflict metadata: %+v", digestErr)
		}
	})

	t.Run("read and list do not claim or acknowledge", func(t *testing.T) {
		h := factory(t)
		res := mustEnqueue(t, h.Store, basicRequest("list", agent("alice"), agent("bob")))
		before := res.Deliveries[0]
		for i := 0; i < 3; i++ {
			_, err := h.Store.GetMessage(context.Background(), res.Message.ID)
			must(t, err)
			listed, err := h.Store.ListDeliveries(context.Background(), delivery.Filter{Recipient: agent("bob")})
			must(t, err)
			if len(listed) != 1 || listed[0].Status != delivery.DeliveryPending || listed[0].AttemptCount != 0 || listed[0].ActiveLeaseToken != "" {
				t.Fatalf("read/list changed delivery state: %+v", listed)
			}
		}
		claimed, err := h.Store.Claim(context.Background(), claimFor(before.ID, agent("bob"), "worker-1", time.Minute, 7))
		must(t, err)
		if claimed.Delivery.AttemptCount != 1 || claimed.Attempt.Stage != delivery.StageLeaseAcquired {
			t.Fatalf("claim did not acquire first lease: %+v", claimed)
		}
	})

	t.Run("leased claims are exclusive across concurrent consumers", func(t *testing.T) {
		h := factory(t)
		res := mustEnqueue(t, h.Store, basicRequest("concurrent", agent("alice"), agent("bob")))
		deliveryID := res.Deliveries[0].ID
		const workers = 16
		var wg sync.WaitGroup
		var mu sync.Mutex
		successes := 0
		staleOrBusy := 0
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, err := h.Store.Claim(context.Background(), claimFor(deliveryID, agent("bob"), "worker", time.Minute, 1))
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					successes++
				} else if errors.Is(err, delivery.ErrAlreadyClaimed) {
					staleOrBusy++
				} else {
					t.Errorf("unexpected claim error: %v", err)
				}
			}(i)
		}
		wg.Wait()
		if successes != 1 || staleOrBusy != workers-1 {
			t.Fatalf("successes=%d busy=%d", successes, staleOrBusy)
		}
	})

	t.Run("lease expiry enables reclaim and rejects stale token", func(t *testing.T) {
		h := factory(t)
		res := mustEnqueue(t, h.Store, basicRequest("expiry", agent("alice"), agent("bob")))
		first, err := h.Store.Claim(context.Background(), claimFor(res.Deliveries[0].ID, agent("bob"), "host-a", time.Minute, 1))
		must(t, err)
		h.Clock.Advance(time.Minute + time.Nanosecond)
		second, err := h.Store.Claim(context.Background(), claimFor(res.Deliveries[0].ID, agent("bob"), "host-b", time.Minute, 2))
		must(t, err)
		if second.Attempt.ID == first.Attempt.ID || second.Attempt.LeaseToken == first.Attempt.LeaseToken || second.Delivery.AttemptCount != 2 {
			t.Fatalf("reclaim did not create a new attempt: first=%+v second=%+v", first.Attempt, second.Attempt)
		}
		_, _, err = h.Store.Ack(context.Background(), delivery.AckRequest{Lease: leaseRef(first.Attempt), Stage: delivery.StageHostAccepted})
		if !errors.Is(err, delivery.ErrStaleLease) {
			t.Fatalf("stale token ack got %v, want ErrStaleLease", err)
		}
	})

	t.Run("fenced and idempotent ack keeps receipt stages distinct", func(t *testing.T) {
		h := factory(t)
		res := mustEnqueue(t, h.Store, basicRequest("ack", agent("alice"), agent("bob")))
		claim, err := h.Store.Claim(context.Background(), claimFor(res.Deliveries[0].ID, agent("bob"), "host", time.Minute, 3))
		must(t, err)
		ref := leaseRef(claim.Attempt)

		d1, a1, err := h.Store.Ack(context.Background(), delivery.AckRequest{Lease: ref, Stage: delivery.StageHostAccepted})
		must(t, err)
		if d1.Status != delivery.DeliveryLeased || a1.Stage != delivery.StageHostAccepted || a1.HostAcceptedAt.IsZero() || !a1.TurnSubmittedAt.IsZero() {
			t.Fatalf("host accepted stage collapsed: delivery=%+v attempt=%+v", d1, a1)
		}
		receipts, err := h.Store.Receipts(context.Background(), d1.ID)
		must(t, err)
		if !hasReceipt(receipts, delivery.StagePersisted) || !hasReceipt(receipts, delivery.StageLeaseAcquired) || !hasReceipt(receipts, delivery.StageHostAccepted) || hasReceipt(receipts, delivery.StageTurnSubmitted) || hasReceipt(receipts, delivery.StageConsumed) {
			t.Fatalf("host accepted receipts collapsed stages: %+v", receipts)
		}
		d2, a2, err := h.Store.Ack(context.Background(), delivery.AckRequest{Lease: ref, Stage: delivery.StageTurnSubmitted})
		must(t, err)
		if d2.Status != delivery.DeliveryLeased || a2.Stage != delivery.StageTurnSubmitted || a2.TurnSubmittedAt.IsZero() || !a2.ConsumedAt.IsZero() {
			t.Fatalf("turn submitted stage collapsed: delivery=%+v attempt=%+v", d2, a2)
		}
		receipts, err = h.Store.Receipts(context.Background(), d2.ID)
		must(t, err)
		if !hasReceipt(receipts, delivery.StageTurnSubmitted) || hasReceipt(receipts, delivery.StageConsumed) {
			t.Fatalf("turn submitted receipts collapsed consumed: %+v", receipts)
		}
		d3, a3, err := h.Store.Ack(context.Background(), delivery.AckRequest{Lease: ref, Stage: delivery.StageConsumed})
		must(t, err)
		if d3.Status != delivery.DeliveryDelivered || a3.Stage != delivery.StageConsumed || a3.ConsumedAt.IsZero() {
			t.Fatalf("consumed did not complete delivery: delivery=%+v attempt=%+v", d3, a3)
		}
		dupeDelivery, dupeAttempt, err := h.Store.Ack(context.Background(), delivery.AckRequest{Lease: ref, Stage: delivery.StageConsumed})
		must(t, err)
		if dupeDelivery.ID != d3.ID || dupeAttempt.ID != a3.ID || dupeDelivery.AttemptCount != d3.AttemptCount {
			t.Fatalf("duplicate ack was not idempotent: delivery=%+v attempt=%+v", dupeDelivery, dupeAttempt)
		}
		stale := ref
		stale.BindingGeneration++
		_, _, err = h.Store.Ack(context.Background(), delivery.AckRequest{Lease: stale, Stage: delivery.StageConsumed})
		if !errors.Is(err, delivery.ErrStaleLease) {
			t.Fatalf("stale generation ack got %v, want ErrStaleLease", err)
		}
	})

	t.Run("nack schedules retry and deadline dead letters", func(t *testing.T) {
		h := factory(t)
		deadline := h.Clock.Now().Add(5 * time.Minute)
		req := basicRequest("retry", agent("alice"), agent("bob"))
		req.DeadlineAt = deadline
		res := mustEnqueue(t, h.Store, req)
		claim, err := h.Store.Claim(context.Background(), claimFor(res.Deliveries[0].ID, agent("bob"), "host", time.Minute, 4))
		must(t, err)
		nextAt := h.Clock.Now().Add(2 * time.Minute)
		d, a, err := h.Store.Nack(context.Background(), delivery.NackRequest{Lease: leaseRef(claim.Attempt), Retryable: true, Error: "host busy", NextAttemptAt: nextAt})
		must(t, err)
		dupeD, dupeA, err := h.Store.Nack(context.Background(), delivery.NackRequest{Lease: leaseRef(claim.Attempt), Retryable: true, Error: "host busy", NextAttemptAt: nextAt})
		must(t, err)
		if dupeD.ID != d.ID || dupeA.ID != a.ID || dupeD.AttemptCount != d.AttemptCount {
			t.Fatalf("duplicate nack was not idempotent: delivery=%+v attempt=%+v", dupeD, dupeA)
		}
		if d.Status != delivery.DeliveryRetryScheduled || !d.NextAttemptAt.Equal(nextAt) || a.Stage != delivery.StageFailed {
			t.Fatalf("retry not scheduled: delivery=%+v attempt=%+v", d, a)
		}
		_, err = h.Store.Claim(context.Background(), claimFor(d.ID, agent("bob"), "host", time.Minute, 5))
		if !errors.Is(err, delivery.ErrNoDeliveryReady) {
			t.Fatalf("early retry claim got %v, want ErrNoDeliveryReady", err)
		}
		h.Clock.Set(nextAt)
		retryClaim, err := h.Store.Claim(context.Background(), claimFor(d.ID, agent("bob"), "host", 10*time.Minute, 5))
		must(t, err)
		h.Clock.Set(deadline.Add(time.Nanosecond))
		dead, _, err := h.Store.Nack(context.Background(), delivery.NackRequest{Lease: leaseRef(retryClaim.Attempt), Retryable: true, Error: "past deadline", NextAttemptAt: h.Clock.Now().Add(time.Minute)})
		if !errors.Is(err, delivery.ErrDeadLettered) || dead.Status != delivery.DeliveryDeadLettered {
			t.Fatalf("deadline nack got delivery=%+v err=%v", dead, err)
		}
	})

	t.Run("authorized redrive reopens dead letter", func(t *testing.T) {
		h := factory(t)
		res := mustEnqueue(t, h.Store, basicRequest("redrive", agent("alice"), agent("bob")))
		claim, err := h.Store.Claim(context.Background(), claimFor(res.Deliveries[0].ID, agent("bob"), "host", time.Minute, 6))
		must(t, err)
		dead, _, err := h.Store.Nack(context.Background(), delivery.NackRequest{Lease: leaseRef(claim.Attempt), Retryable: false, Error: "terminal"})
		if !errors.Is(err, delivery.ErrDeadLettered) {
			t.Fatalf("terminal nack got %v", err)
		}
		_, err = h.Store.Redrive(context.Background(), delivery.RedriveRequest{DeliveryID: dead.ID})
		if !errors.Is(err, delivery.ErrUnauthorized) {
			t.Fatalf("unauthorized redrive got %v", err)
		}
		reopened, err := h.Store.Redrive(context.Background(), delivery.RedriveRequest{DeliveryID: dead.ID, AuthorizedBy: "operator", NewDeadlineAt: h.Clock.Now().Add(time.Hour)})
		must(t, err)
		if reopened.Status != delivery.DeliveryPending || reopened.DeadLetterReason != "" {
			t.Fatalf("redrive did not reopen: %+v", reopened)
		}
	})

	t.Run("group fanout freezes recipient snapshot and per-recipient outcomes", func(t *testing.T) {
		h := factory(t)
		req := basicRequest("fanout", agent("alice"), agent("bob"), session("sess-1"), session("sess-2"))
		req.Group = messaging.Address{Kind: messaging.KindGroup, Authority: "test", ID: "room"}
		res := mustEnqueue(t, h.Store, req)
		if len(res.Deliveries) != 3 {
			t.Fatalf("fanout deliveries = %d, want 3", len(res.Deliveries))
		}
		first, err := h.Store.Claim(context.Background(), claimFor(res.Deliveries[0].ID, res.Deliveries[0].Recipient, "host", time.Minute, 1))
		must(t, err)
		_, _, err = h.Store.Ack(context.Background(), delivery.AckRequest{Lease: leaseRef(first.Attempt), Stage: delivery.StageConsumed})
		must(t, err)

		second, err := h.Store.Claim(context.Background(), claimFor(res.Deliveries[1].ID, res.Deliveries[1].Recipient, "host", time.Minute, 1))
		must(t, err)
		dead, _, err := h.Store.Nack(context.Background(), delivery.NackRequest{Lease: leaseRef(second.Attempt), Retryable: false, Error: "rejected"})
		if !errors.Is(err, delivery.ErrDeadLettered) || dead.Status != delivery.DeliveryDeadLettered {
			t.Fatalf("second outcome not dead-lettered: delivery=%+v err=%v", dead, err)
		}

		listed, err := h.Store.ListDeliveries(context.Background(), delivery.Filter{MessageID: res.Message.ID})
		must(t, err)
		if len(listed) != 3 {
			t.Fatalf("recipient set changed: %+v", listed)
		}
		statuses := map[delivery.DeliveryStatus]int{}
		for _, d := range listed {
			statuses[d.Status]++
			if d.MessageID != res.Message.ID || d.Recipient.IsZero() {
				t.Fatalf("bad delivery obligation: %+v", d)
			}
		}
		if statuses[delivery.DeliveryDelivered] != 1 || statuses[delivery.DeliveryDeadLettered] != 1 || statuses[delivery.DeliveryPending] != 1 {
			t.Fatalf("per-recipient outcomes collapsed: %v", statuses)
		}
	})

	t.Run("offline presence alone does not exhaust retries", func(t *testing.T) {
		h := factory(t)
		res := mustEnqueue(t, h.Store, basicRequest("offline", agent("alice"), agent("offline")))
		h.Clock.Advance(24 * time.Hour)
		listed, err := h.Store.ListDeliveries(context.Background(), delivery.Filter{Recipient: agent("offline")})
		must(t, err)
		if len(listed) != 1 || listed[0].Status != delivery.DeliveryPending || listed[0].AttemptCount != 0 {
			t.Fatalf("offline/listing burned attempts: %+v", listed)
		}
		claim, err := h.Store.Claim(context.Background(), claimFor(res.Deliveries[0].ID, agent("offline"), "host", time.Minute, 1))
		must(t, err)
		if claim.Delivery.AttemptCount != 1 {
			t.Fatalf("first online claim attempt count = %d", claim.Delivery.AttemptCount)
		}
	})
}

func basicRequest(key string, from messaging.Address, recipients ...messaging.Address) delivery.EnqueueRequest {
	targets := make([]delivery.RecipientTarget, 0, len(recipients))
	for i, r := range recipients {
		binding := delivery.BindingTarget{Address: r, BindingGeneration: int64(i + 1), RouteGeneration: 1}
		switch r.Kind {
		case messaging.KindSession:
			binding.SessionID = r.ID
		case messaging.KindAgent:
			binding.ActorID = r.ID
		}
		targets = append(targets, delivery.RecipientTarget{Address: r, Binding: binding})
	}
	return delivery.EnqueueRequest{
		IdempotencyKey: key,
		From:           from,
		Recipients:     targets,
		Kind:           messaging.MsgKindNotice,
		Channel:        messaging.Channel("control"),
		Payload:        []byte(`{"body":"hello"}`),
		ContentType:    "application/json",
		Metadata:       map[string]string{"trace": "contract"},
	}
}

func claimFor(id delivery.DeliveryID, recipient messaging.Address, holder string, lease time.Duration, generation int64) delivery.ClaimRequest {
	return delivery.ClaimRequest{
		DeliveryID:        id,
		Recipient:         recipient,
		Holder:            holder,
		BindingGeneration: generation,
		LeaseDuration:     lease,
	}
}

func leaseRef(a delivery.Attempt) delivery.LeaseRef {
	return delivery.LeaseRef{
		DeliveryID:        a.DeliveryID,
		AttemptID:         a.ID,
		LeaseToken:        a.LeaseToken,
		BindingGeneration: a.BindingGeneration,
	}
}

func hasReceipt(receipts []delivery.Receipt, stage delivery.ReceiptStage) bool {
	for _, r := range receipts {
		if r.Stage == stage {
			return true
		}
	}
	return false
}

func mustEnqueue(t *testing.T, s delivery.Store, req delivery.EnqueueRequest) delivery.EnqueueResult {
	t.Helper()
	res, err := s.Enqueue(context.Background(), req)
	must(t, err)
	return res
}

func agent(id string) messaging.Address {
	return messaging.Address{Kind: messaging.KindAgent, Authority: "test", ID: id}
}

func session(id string) messaging.Address {
	return messaging.Address{Kind: messaging.KindSession, Authority: "test", ID: id}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
