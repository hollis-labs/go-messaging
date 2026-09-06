package delivery

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrHandoffOffline = errors.New("delivery handoff owner offline")

// HandoffOutcome reports what the host durably observed after accepting a
// delivery. SubmittedAt records queue/turn submission only; it is not model
// consumption. ConsumedAt must be set by a separate observation.
type HandoffOutcome struct {
	SubmittedAt   time.Time
	ConsumedAt    time.Time
	Retryable     bool
	NextAttemptAt time.Time
}

// Handoff records host-owned delivery policy. Implementations decide whether a
// recipient is currently claimable, durably record the delivery/attempt before
// acceptance, and submit work to their local runtime/queue.
type Handoff interface {
	Available(context.Context, RecipientDelivery) (bool, error)
	RecordDelivery(context.Context, ClaimResult) error
	Submit(context.Context, ClaimResult) (HandoffOutcome, error)
}

type PumpOption func(*Pump)

func WithPumpClock(clock Clock) PumpOption {
	return func(p *Pump) {
		if clock != nil {
			p.clock = clock
		}
	}
}

func WithPumpHolder(holder string) PumpOption {
	return func(p *Pump) {
		if holder != "" {
			p.holder = holder
		}
	}
}

func WithPumpLeaseDuration(d time.Duration) PumpOption {
	return func(p *Pump) {
		if d > 0 {
			p.leaseDuration = d
		}
	}
}

func WithPumpRetryBackoff(d time.Duration) PumpOption {
	return func(p *Pump) {
		if d >= 0 {
			p.retryBackoff = d
		}
	}
}

func WithPumpMaxWorkers(n int) PumpOption {
	return func(p *Pump) {
		if n > 0 {
			p.maxWorkers = n
		}
	}
}

func WithPumpBatchSize(n int) PumpOption {
	return func(p *Pump) {
		if n > 0 {
			p.batchSize = n
		}
	}
}

func WithPumpPollInterval(d time.Duration) PumpOption {
	return func(p *Pump) {
		if d > 0 {
			p.pollInterval = d
		}
	}
}

// Pump reconciles durable delivery obligations with a host handoff adapter.
// Hints only wake reconciliation; correctness comes from ListDeliveries and
// Claim, so missed subscribe events converge on the next poll/reconnect.
type Pump struct {
	store Store
	host  Handoff

	clock         Clock
	holder        string
	leaseDuration time.Duration
	retryBackoff  time.Duration
	maxWorkers    int
	batchSize     int
	pollInterval  time.Duration

	hints chan struct{}
}

func NewPump(store Store, host Handoff, opts ...PumpOption) *Pump {
	p := &Pump{
		store:         store,
		host:          host,
		clock:         realClock{},
		holder:        "delivery-pump",
		leaseDuration: time.Minute,
		retryBackoff:  time.Second,
		maxWorkers:    1,
		batchSize:     32,
		pollInterval:  time.Second,
		hints:         make(chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Hint coalesces a live notification. It is safe to drop because Run and
// Reconcile always read durable ready deliveries.
func (p *Pump) Hint() {
	select {
	case p.hints <- struct{}{}:
	default:
	}
}

// Run reconciles until ctx is canceled. It owns no goroutines after return.
func (p *Pump) Run(ctx context.Context) error {
	if _, err := p.Reconcile(ctx); err != nil && ctx.Err() == nil {
		return err
	}
	timer := time.NewTimer(p.pollInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.hints:
			if _, err := p.Reconcile(ctx); err != nil && ctx.Err() == nil {
				return err
			}
		case <-timer.C:
			if _, err := p.Reconcile(ctx); err != nil && ctx.Err() == nil {
				return err
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(p.pollInterval)
	}
}

// Reconcile claims and hands off currently ready deliveries. It returns the
// number of successful delivery claims attempted during this pass.
func (p *Pump) Reconcile(ctx context.Context) (int, error) {
	ready, err := p.store.ListDeliveries(ctx, Filter{ReadyOnly: true, Limit: p.batchSize})
	if err != nil {
		return 0, err
	}
	sem := make(chan struct{}, p.maxWorkers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	claimed := 0
	var firstErr error
	for _, d := range ready {
		if ctx.Err() != nil {
			break
		}
		available, err := p.host.Available(ctx, d)
		if err != nil {
			return claimed, err
		}
		if !available {
			continue
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(d RecipientDelivery) {
			defer wg.Done()
			defer func() { <-sem }()
			err := p.process(ctx, d)
			if err != nil {
				if errors.Is(err, ErrAlreadyClaimed) || errors.Is(err, ErrNoDeliveryReady) || ctx.Err() != nil {
					return
				}
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			if ctx.Err() == nil {
				mu.Lock()
				claimed++
				mu.Unlock()
			}
		}(d)
	}
	wg.Wait()
	return claimed, firstErr
}

func (p *Pump) process(ctx context.Context, d RecipientDelivery) error {
	claim, err := p.store.Claim(ctx, ClaimRequest{DeliveryID: d.ID, Recipient: d.Recipient, Holder: p.holder, BindingGeneration: d.Binding.BindingGeneration, LeaseDuration: p.leaseDuration, Nowait: true})
	if err != nil {
		return err
	}
	if err := p.host.RecordDelivery(ctx, claim); err != nil {
		_, _, _ = p.store.Nack(ctx, NackRequest{Lease: leaseFromAttempt(claim.Attempt), Retryable: true, Error: err.Error(), NextAttemptAt: p.nextRetry(), At: p.now()})
		return err
	}
	if _, _, err := p.store.Ack(ctx, AckRequest{Lease: leaseFromAttempt(claim.Attempt), Stage: StageHostAccepted, At: p.now()}); err != nil {
		return err
	}
	outcome, err := p.host.Submit(ctx, claim)
	if err != nil {
		_, _, _ = p.store.Nack(ctx, NackRequest{Lease: leaseFromAttempt(claim.Attempt), Retryable: true, Error: err.Error(), NextAttemptAt: p.nextRetry(), At: p.now()})
		return err
	}
	if !outcome.SubmittedAt.IsZero() {
		if _, _, err := p.store.Ack(ctx, AckRequest{Lease: leaseFromAttempt(claim.Attempt), Stage: StageTurnSubmitted, At: outcome.SubmittedAt}); err != nil {
			return err
		}
	}
	if !outcome.ConsumedAt.IsZero() {
		if _, _, err := p.store.Ack(ctx, AckRequest{Lease: leaseFromAttempt(claim.Attempt), Stage: StageConsumed, At: outcome.ConsumedAt}); err != nil {
			return err
		}
	}
	return nil
}

func (p *Pump) nextRetry() time.Time { return p.now().Add(p.retryBackoff).UTC() }

func (p *Pump) now() time.Time { return p.clock.Now().UTC() }

func leaseFromAttempt(a Attempt) LeaseRef {
	return LeaseRef{DeliveryID: a.DeliveryID, AttemptID: a.ID, LeaseToken: a.LeaseToken, BindingGeneration: a.BindingGeneration}
}
