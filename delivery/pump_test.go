package delivery_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	messaging "github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/go-messaging/delivery"
	"github.com/hollis-labs/go-messaging/deliverytest"
)

func TestPumpReconcilesAfterLostHintsAndReconnect(t *testing.T) {
	clock := deliverytest.NewFakeClock(time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC))
	store := delivery.NewMemoryStore(delivery.WithClock(clock))
	host := newFakeHandoff(clock)
	pump := delivery.NewPump(store, host, delivery.WithPumpClock(clock), delivery.WithPumpRetryBackoff(0), delivery.WithPumpMaxWorkers(2))
	res := mustEnqueue(t, store, pumpRequest("lost-hints", agent("alice"), agent("bob")))
	// No hint is required: durable ready listing is the source of truth.
	claimed, err := pump.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if claimed != 1 {
		t.Fatalf("claimed=%d, want 1", claimed)
	}
	del, err := store.GetDelivery(context.Background(), res.Deliveries[0].ID)
	if err != nil {
		t.Fatalf("get delivery: %v", err)
	}
	if del.Status != delivery.DeliveryDelivered {
		t.Fatalf("delivery not delivered after reconciliation: %+v", del)
	}

	// A restarted pump with only durable store state must see no lost obligation.
	restarted := delivery.NewPump(store, host, delivery.WithPumpClock(clock))
	claimed, err = restarted.Reconcile(context.Background())
	if err != nil || claimed != 0 {
		t.Fatalf("restart reconcile claimed=%d err=%v, want 0 nil", claimed, err)
	}
}

func TestPumpCrashBeforeAndAfterHostAcceptanceConverges(t *testing.T) {
	clock := deliverytest.NewFakeClock(time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC))
	t.Run("before host acceptance", func(t *testing.T) {
		store := delivery.NewMemoryStore(delivery.WithClock(clock))
		host := newFakeHandoff(clock)
		host.recordErrs = []error{errors.New("record crash")}
		pump := delivery.NewPump(store, host, delivery.WithPumpClock(clock), delivery.WithPumpRetryBackoff(0))
		res := mustEnqueue(t, store, pumpRequest("before-acceptance", agent("alice"), agent("bob")))
		if _, err := pump.Reconcile(context.Background()); err == nil {
			t.Fatal("expected record crash")
		}
		del, err := store.GetDelivery(context.Background(), res.Deliveries[0].ID)
		if err != nil {
			t.Fatalf("get delivery: %v", err)
		}
		if del.Status != delivery.DeliveryRetryScheduled || del.AttemptCount != 1 {
			t.Fatalf("record crash did not leave retryable obligation: %+v", del)
		}
		if _, err := pump.Reconcile(context.Background()); err != nil {
			t.Fatalf("second reconcile: %v", err)
		}
		del, _ = store.GetDelivery(context.Background(), res.Deliveries[0].ID)
		if del.Status != delivery.DeliveryDelivered || del.AttemptCount != 2 {
			t.Fatalf("retry did not converge: %+v", del)
		}
	})

	t.Run("after host acceptance before submitted", func(t *testing.T) {
		store := delivery.NewMemoryStore(delivery.WithClock(clock))
		host := newFakeHandoff(clock)
		host.submitErrs = []error{errors.New("submit crash")}
		pump := delivery.NewPump(store, host, delivery.WithPumpClock(clock), delivery.WithPumpRetryBackoff(0))
		res := mustEnqueue(t, store, pumpRequest("after-acceptance", agent("alice"), agent("bob")))
		if _, err := pump.Reconcile(context.Background()); err == nil {
			t.Fatal("expected submit crash")
		}
		receipts, err := store.Receipts(context.Background(), res.Deliveries[0].ID)
		if err != nil {
			t.Fatalf("receipts: %v", err)
		}
		if !hasReceipt(receipts, delivery.StageHostAccepted) || hasReceipt(receipts, delivery.StageTurnSubmitted) || hasReceipt(receipts, delivery.StageConsumed) {
			t.Fatalf("crash receipts not fenced correctly: %+v", receipts)
		}
		if _, err := pump.Reconcile(context.Background()); err != nil {
			t.Fatalf("second reconcile: %v", err)
		}
		del, _ := store.GetDelivery(context.Background(), res.Deliveries[0].ID)
		if del.Status != delivery.DeliveryDelivered || del.AttemptCount != 2 {
			t.Fatalf("retry did not converge: %+v", del)
		}
	})
}

func TestPumpDuplicateHintsAndReceiptsAreIdempotent(t *testing.T) {
	clock := deliverytest.NewFakeClock(time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC))
	store := delivery.NewMemoryStore(delivery.WithClock(clock))
	host := newFakeHandoff(clock)
	pump := delivery.NewPump(store, host, delivery.WithPumpClock(clock))
	res := mustEnqueue(t, store, pumpRequest("duplicates", agent("alice"), agent("bob")))
	for i := 0; i < 10; i++ {
		pump.Hint()
	}
	if _, err := pump.Reconcile(context.Background()); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if _, err := pump.Reconcile(context.Background()); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	receipts, err := store.Receipts(context.Background(), res.Deliveries[0].ID)
	if err != nil {
		t.Fatalf("receipts: %v", err)
	}
	counts := map[delivery.ReceiptStage]int{}
	for _, r := range receipts {
		counts[r.Stage]++
	}
	for _, stage := range []delivery.ReceiptStage{delivery.StagePersisted, delivery.StageLeaseAcquired, delivery.StageHostAccepted, delivery.StageTurnSubmitted, delivery.StageConsumed} {
		if counts[stage] != 1 {
			t.Fatalf("stage %s count=%d receipts=%+v", stage, counts[stage], receipts)
		}
	}
}

func TestPumpBackpressureFairnessShutdownAndOffline(t *testing.T) {
	clock := deliverytest.NewFakeClock(time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC))
	t.Run("bounded workers and fairness", func(t *testing.T) {
		store := delivery.NewMemoryStore(delivery.WithClock(clock))
		host := newFakeHandoff(clock)
		host.submitDelay = 10 * time.Millisecond
		for i := 0; i < 8; i++ {
			mustEnqueue(t, store, pumpRequest(fmt.Sprintf("fair-%d", i), agent("alice"), agent(fmt.Sprintf("worker-%d", i))))
		}
		pump := delivery.NewPump(store, host, delivery.WithPumpClock(clock), delivery.WithPumpMaxWorkers(2), delivery.WithPumpBatchSize(8))
		claimed, err := pump.Reconcile(context.Background())
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if claimed != 8 || host.maxConcurrent > 2 || len(host.submitted) != 8 {
			t.Fatalf("claimed=%d submitted=%d maxConcurrent=%d", claimed, len(host.submitted), host.maxConcurrent)
		}
	})

	t.Run("offline owner remains queued", func(t *testing.T) {
		store := delivery.NewMemoryStore(delivery.WithClock(clock))
		host := newFakeHandoff(clock)
		host.offline[agent("offline").URN()] = true
		res := mustEnqueue(t, store, pumpRequest("offline", agent("alice"), agent("offline")))
		pump := delivery.NewPump(store, host, delivery.WithPumpClock(clock), delivery.WithPumpRetryBackoff(0))
		claimed, err := pump.Reconcile(context.Background())
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if claimed != 0 {
			t.Fatalf("claimed offline delivery=%d", claimed)
		}
		del, err := store.GetDelivery(context.Background(), res.Deliveries[0].ID)
		if err != nil {
			t.Fatalf("get delivery: %v", err)
		}
		if del.Status != delivery.DeliveryPending || del.AttemptCount != 0 {
			t.Fatalf("offline owner burned attempts: %+v", del)
		}
	})

	t.Run("run exits on shutdown and owns workers", func(t *testing.T) {
		store := delivery.NewMemoryStore(delivery.WithClock(clock))
		host := newFakeHandoff(clock)
		host.blockUntilCanceled = true
		mustEnqueue(t, store, pumpRequest("shutdown", agent("alice"), agent("bob")))
		pump := delivery.NewPump(store, host, delivery.WithPumpClock(clock), delivery.WithPumpPollInterval(time.Hour))
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- pump.Run(ctx) }()
		for host.inFlight() == 0 {
			time.Sleep(time.Millisecond)
		}
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("run err=%v, want context canceled", err)
			}
		case <-time.After(time.Second):
			t.Fatal("pump did not shut down")
		}
	})
}

func TestPumpReferenceHandoffRecordsBeforeAckAndDoesNotConsumeOnSubmit(t *testing.T) {
	clock := deliverytest.NewFakeClock(time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC))
	base := delivery.NewMemoryStore(delivery.WithClock(clock))
	store := &recordingStore{Store: base}
	host := newFakeHandoff(clock)
	host.events = &store.events
	host.outcome = delivery.HandoffOutcome{SubmittedAt: clock.Now().Add(time.Minute)} // HTTP 200 / SendTurn equivalent only.
	pump := delivery.NewPump(store, host, delivery.WithPumpClock(clock))
	res := mustEnqueue(t, store, pumpRequest("submit-only", agent("alice"), agent("bob")))
	if _, err := pump.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	want := []string{"record:" + string(res.Deliveries[0].ID), "ack:host_accepted", "submit:" + string(res.Deliveries[0].ID), "ack:turn_submitted"}
	if fmt.Sprint(store.events) != fmt.Sprint(want) {
		t.Fatalf("events=%v want=%v", store.events, want)
	}
	del, err := store.GetDelivery(context.Background(), res.Deliveries[0].ID)
	if err != nil {
		t.Fatalf("get delivery: %v", err)
	}
	if del.Status == delivery.DeliveryDelivered {
		t.Fatalf("turn submission alone marked consumed: %+v", del)
	}
	receipts, err := store.Receipts(context.Background(), res.Deliveries[0].ID)
	if err != nil {
		t.Fatalf("receipts: %v", err)
	}
	if hasReceipt(receipts, delivery.StageConsumed) {
		t.Fatalf("submit-only handoff created consumed receipt: %+v", receipts)
	}
}

func pumpRequest(key string, from messaging.Address, recipients ...messaging.Address) delivery.EnqueueRequest {
	targets := make([]delivery.RecipientTarget, 0, len(recipients))
	for i, r := range recipients {
		targets = append(targets, delivery.RecipientTarget{Address: r, Binding: delivery.BindingTarget{Address: r, ActorID: r.ID, BindingGeneration: int64(i + 1)}})
	}
	return delivery.EnqueueRequest{IdempotencyKey: key, From: from, Recipients: targets, Kind: messaging.MsgKindNotice, Payload: []byte(`{}`)}
}

type fakeHandoff struct {
	clock *deliverytest.FakeClock

	mu                 sync.Mutex
	offline            map[string]bool
	recorded           []delivery.DeliveryID
	submitted          []delivery.DeliveryID
	recordErrs         []error
	submitErrs         []error
	outcome            delivery.HandoffOutcome
	submitDelay        time.Duration
	currentConcurrent  int
	maxConcurrent      int
	blockUntilCanceled bool
	events             *[]string
}

func newFakeHandoff(clock *deliverytest.FakeClock) *fakeHandoff {
	return &fakeHandoff{clock: clock, offline: map[string]bool{}, outcome: delivery.HandoffOutcome{SubmittedAt: clock.Now().Add(time.Minute), ConsumedAt: clock.Now().Add(2 * time.Minute)}}
}

func (h *fakeHandoff) Available(_ context.Context, d delivery.RecipientDelivery) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.offline[d.Recipient.URN()], nil
}

func (h *fakeHandoff) RecordDelivery(_ context.Context, claim delivery.ClaimResult) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recorded = append(h.recorded, claim.Delivery.ID)
	if h.events != nil {
		*h.events = append(*h.events, "record:"+string(claim.Delivery.ID))
	}
	if len(h.recordErrs) > 0 {
		err := h.recordErrs[0]
		h.recordErrs = h.recordErrs[1:]
		return err
	}
	return nil
}

func (h *fakeHandoff) Submit(ctx context.Context, claim delivery.ClaimResult) (delivery.HandoffOutcome, error) {
	h.mu.Lock()
	h.submitted = append(h.submitted, claim.Delivery.ID)
	if h.events != nil {
		*h.events = append(*h.events, "submit:"+string(claim.Delivery.ID))
	}
	h.currentConcurrent++
	if h.currentConcurrent > h.maxConcurrent {
		h.maxConcurrent = h.currentConcurrent
	}
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		h.currentConcurrent--
		h.mu.Unlock()
	}()
	if h.blockUntilCanceled {
		<-ctx.Done()
		return delivery.HandoffOutcome{}, ctx.Err()
	}
	if h.submitDelay > 0 {
		time.Sleep(h.submitDelay)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.submitErrs) > 0 {
		err := h.submitErrs[0]
		h.submitErrs = h.submitErrs[1:]
		return delivery.HandoffOutcome{}, err
	}
	return h.outcome, nil
}

func (h *fakeHandoff) inFlight() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.currentConcurrent
}

type recordingStore struct {
	delivery.Store
	events []string
}

func (s *recordingStore) Ack(ctx context.Context, req delivery.AckRequest) (delivery.RecipientDelivery, delivery.Attempt, error) {
	s.events = append(s.events, "ack:"+string(req.Stage))
	return s.Store.Ack(ctx, req)
}
