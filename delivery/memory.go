package delivery

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	messaging "github.com/hollis-labs/go-messaging"
)

// Clock supplies deterministic time for stores and tests.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

// Option configures MemoryStore.
type Option func(*MemoryStore)

// WithClock injects deterministic time. Nil clocks are ignored.
func WithClock(clock Clock) Option {
	return func(s *MemoryStore) {
		if clock != nil {
			s.clock = clock
		}
	}
}

// MemoryStore is an in-memory reference implementation of Store. It is safe
// for concurrent consumers but not durable across process restarts.
type MemoryStore struct {
	mu sync.Mutex

	clock Clock

	messages   map[MessageID]Message
	deliveries map[DeliveryID]RecipientDelivery
	attempts   map[AttemptID]Attempt
	byMessage  map[MessageID][]DeliveryID
	byDelivery map[DeliveryID][]AttemptID
	receipts   map[DeliveryID][]Receipt

	idempotency map[string]idempotencyRecord
}

type idempotencyRecord struct {
	messageID MessageID
	digest    Digest
}

// NewMemoryStore constructs an empty reliable delivery store.
func NewMemoryStore(opts ...Option) *MemoryStore {
	s := &MemoryStore{
		clock:       realClock{},
		messages:    map[MessageID]Message{},
		deliveries:  map[DeliveryID]RecipientDelivery{},
		attempts:    map[AttemptID]Attempt{},
		byMessage:   map[MessageID][]DeliveryID{},
		byDelivery:  map[DeliveryID][]AttemptID{},
		receipts:    map[DeliveryID][]Receipt{},
		idempotency: map[string]idempotencyRecord{},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

var _ Store = (*MemoryStore)(nil)

func (s *MemoryStore) Enqueue(_ context.Context, req EnqueueRequest) (EnqueueResult, error) {
	if req.From.IsZero() || len(req.Recipients) == 0 || req.Kind == "" {
		return EnqueueResult{}, ErrInvalidArgument
	}
	for _, r := range req.Recipients {
		if r.Address.IsZero() {
			return EnqueueResult{}, ErrInvalidArgument
		}
	}

	digest, err := canonicalDigest(req)
	if err != nil {
		return EnqueueResult{}, err
	}
	idemKey := idempotencyScope(req.From, req.IdempotencyKey)

	s.mu.Lock()
	defer s.mu.Unlock()

	if req.IdempotencyKey != "" {
		if existing, ok := s.idempotency[idemKey]; ok {
			if existing.digest != digest {
				return EnqueueResult{}, &DigestConflictError{
					Sender:            req.From,
					IdempotencyKey:    req.IdempotencyKey,
					ExistingMessageID: existing.messageID,
					ExistingDigest:    existing.digest,
					NewDigest:         digest,
				}
			}
			msg := cloneMessage(s.messages[existing.messageID])
			dels := s.deliveriesForMessageLocked(existing.messageID)
			return EnqueueResult{Message: msg, Deliveries: dels, Duplicate: true}, nil
		}
	}

	now := s.now()
	msgID := MessageID(newID())
	msg := Message{
		ID:             msgID,
		Digest:         digest,
		IdempotencyKey: req.IdempotencyKey,
		From:           req.From,
		Group:          req.Group,
		Kind:           req.Kind,
		Channel:        req.Channel,
		ThreadID:       req.ThreadID,
		InReplyTo:      req.InReplyTo,
		Payload:        cloneBytes(req.Payload),
		ContentType:    req.ContentType,
		Metadata:       cloneMap(req.Metadata),
		CreatedAt:      now,
	}
	s.messages[msgID] = msg
	if req.IdempotencyKey != "" {
		s.idempotency[idemKey] = idempotencyRecord{messageID: msgID, digest: digest}
	}

	deliveries := make([]RecipientDelivery, 0, len(req.Recipients))
	for _, target := range req.Recipients {
		deliveryID := DeliveryID(newID())
		binding := target.Binding
		if binding.Address.IsZero() {
			binding.Address = target.Address
		}
		d := RecipientDelivery{
			ID:         deliveryID,
			MessageID:  msgID,
			Recipient:  target.Address,
			Binding:    binding,
			Status:     DeliveryPending,
			DeadlineAt: req.DeadlineAt,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		s.deliveries[deliveryID] = d
		s.byMessage[msgID] = append(s.byMessage[msgID], deliveryID)
		s.appendReceiptLocked(Receipt{MessageID: msgID, DeliveryID: deliveryID, Stage: StagePersisted, At: now})
		deliveries = append(deliveries, cloneDelivery(d))
	}
	return EnqueueResult{Message: cloneMessage(msg), Deliveries: deliveries}, nil
}

func (s *MemoryStore) GetMessage(_ context.Context, id MessageID) (Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg, ok := s.messages[id]
	if !ok {
		return Message{}, ErrNotFound
	}
	return cloneMessage(msg), nil
}

func (s *MemoryStore) GetDelivery(_ context.Context, id DeliveryID) (RecipientDelivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseExpiredLocked()
	d, ok := s.deliveries[id]
	if !ok {
		return RecipientDelivery{}, ErrNotFound
	}
	return cloneDelivery(d), nil
}

func (s *MemoryStore) ListDeliveries(_ context.Context, f Filter) ([]RecipientDelivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseExpiredLocked()

	out := make([]RecipientDelivery, 0)
	ids := make([]string, 0, len(s.deliveries))
	for id := range s.deliveries {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	for _, rawID := range ids {
		d := s.deliveries[DeliveryID(rawID)]
		if !matchesFilter(s.now(), d, f) {
			continue
		}
		out = append(out, cloneDelivery(d))
		if f.Limit > 0 && len(out) >= f.Limit {
			break
		}
	}
	return out, nil
}

func (s *MemoryStore) Claim(_ context.Context, req ClaimRequest) (ClaimResult, error) {
	if req.Holder == "" || req.LeaseDuration <= 0 {
		return ClaimResult{}, ErrInvalidArgument
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseExpiredLocked()

	now := s.now()
	id, err := s.selectDeliveryLocked(now, req)
	if err != nil {
		return ClaimResult{}, err
	}
	d := s.deliveries[id]
	if terminal(d.Status) {
		return ClaimResult{}, ErrTerminalDelivery
	}
	if deadlineExpired(now, d) {
		d = s.deadLetterLocked(d, "deadline exceeded")
		return ClaimResult{}, ErrDeadlineExceeded
	}
	if d.Status == DeliveryLeased && d.ActiveLeaseToken != "" && now.Before(d.LeaseExpiresAt) {
		return ClaimResult{}, ErrAlreadyClaimed
	}
	if !d.NextAttemptAt.IsZero() && now.Before(d.NextAttemptAt) {
		return ClaimResult{}, ErrNoDeliveryReady
	}

	attemptID := AttemptID(newID())
	token := LeaseToken(newID())
	binding := d.Binding
	if binding.BindingGeneration == 0 {
		binding.BindingGeneration = req.BindingGeneration
	}
	expires := now.Add(req.LeaseDuration)
	attempt := Attempt{
		ID:                attemptID,
		MessageID:         d.MessageID,
		DeliveryID:        d.ID,
		LeaseToken:        token,
		Holder:            req.Holder,
		Binding:           binding,
		BindingGeneration: req.BindingGeneration,
		AcquiredAt:        now,
		ExpiresAt:         expires,
		Stage:             StageLeaseAcquired,
	}
	d.AttemptCount++
	d.ActiveAttemptID = attemptID
	d.ActiveLeaseToken = token
	d.LeaseHolder = req.Holder
	d.LeaseExpiresAt = expires
	d.Status = DeliveryLeased
	d.UpdatedAt = now
	s.deliveries[d.ID] = d
	s.attempts[attemptID] = attempt
	s.byDelivery[d.ID] = append(s.byDelivery[d.ID], attemptID)
	s.appendReceiptLocked(Receipt{MessageID: d.MessageID, DeliveryID: d.ID, AttemptID: attemptID, Stage: StageLeaseAcquired, At: now})

	msg := s.messages[d.MessageID]
	return ClaimResult{Message: cloneMessage(msg), Delivery: cloneDelivery(d), Attempt: cloneAttempt(attempt)}, nil
}

func (s *MemoryStore) ExtendLease(_ context.Context, ref LeaseRef, until time.Time) (Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, a, err := s.currentLeaseLocked(ref, false)
	if err != nil {
		return Attempt{}, err
	}
	now := s.now()
	if !until.After(now) || until.Before(a.ExpiresAt) {
		return Attempt{}, ErrInvalidArgument
	}
	a.ExpiresAt = until.UTC()
	d.LeaseExpiresAt = until.UTC()
	d.UpdatedAt = now
	s.attempts[a.ID] = a
	s.deliveries[d.ID] = d
	return cloneAttempt(a), nil
}

func (s *MemoryStore) Ack(_ context.Context, req AckRequest) (RecipientDelivery, Attempt, error) {
	if req.Stage == "" {
		return RecipientDelivery{}, Attempt{}, ErrInvalidArgument
	}
	if req.Stage != StageHostAccepted && req.Stage != StageTurnSubmitted && req.Stage != StageConsumed {
		return RecipientDelivery{}, Attempt{}, ErrUnsupportedStage
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	d, a, err := s.currentLeaseLocked(req.Lease, true)
	if err != nil {
		return RecipientDelivery{}, Attempt{}, err
	}
	if a.Stage == StageFailed || a.Stage == StageDeadLettered {
		return cloneDelivery(d), cloneAttempt(a), nil
	}
	at := req.At.UTC()
	if at.IsZero() {
		at = s.now()
	}
	if deadlineExpired(at, d) && d.Status != DeliveryDelivered {
		d = s.deadLetterLocked(d, "deadline exceeded")
		a.Stage = StageDeadLettered
		s.attempts[a.ID] = a
		return cloneDelivery(d), cloneAttempt(a), ErrDeadlineExceeded
	}

	if stageRank(a.Stage) > stageRank(req.Stage) {
		return cloneDelivery(d), cloneAttempt(a), nil
	}
	switch req.Stage {
	case StageHostAccepted:
		if a.HostAcceptedAt.IsZero() {
			a.HostAcceptedAt = at
		}
	case StageTurnSubmitted:
		if a.HostAcceptedAt.IsZero() {
			a.HostAcceptedAt = at
		}
		if a.TurnSubmittedAt.IsZero() {
			a.TurnSubmittedAt = at
		}
	case StageConsumed:
		if a.HostAcceptedAt.IsZero() {
			a.HostAcceptedAt = at
		}
		if a.TurnSubmittedAt.IsZero() {
			a.TurnSubmittedAt = at
		}
		if a.ConsumedAt.IsZero() {
			a.ConsumedAt = at
		}
		d.Status = DeliveryDelivered
		d.ActiveAttemptID = ""
		d.ActiveLeaseToken = ""
		d.LeaseHolder = ""
		d.LeaseExpiresAt = time.Time{}
	}
	a.Stage = req.Stage
	d.UpdatedAt = at
	s.appendReceiptLocked(Receipt{MessageID: d.MessageID, DeliveryID: d.ID, AttemptID: a.ID, Stage: req.Stage, At: at})
	s.attempts[a.ID] = a
	s.deliveries[d.ID] = d
	return cloneDelivery(d), cloneAttempt(a), nil
}

func (s *MemoryStore) Nack(_ context.Context, req NackRequest) (RecipientDelivery, Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, a, err := s.currentLeaseLocked(req.Lease, true)
	if err != nil {
		return RecipientDelivery{}, Attempt{}, err
	}
	if a.Stage == StageFailed || a.Stage == StageDeadLettered {
		return cloneDelivery(d), cloneAttempt(a), nil
	}
	at := req.At.UTC()
	if at.IsZero() {
		at = s.now()
	}
	a.Stage = StageFailed
	a.FailedAt = at
	a.Error = req.Error
	a.Retryable = req.Retryable
	a.NextAttemptAt = req.NextAttemptAt.UTC()

	d.ActiveAttemptID = ""
	d.ActiveLeaseToken = ""
	d.LeaseHolder = ""
	d.LeaseExpiresAt = time.Time{}
	d.UpdatedAt = at

	if !req.Retryable || deadlineExpired(at, d) || (!req.NextAttemptAt.IsZero() && deadlineExpired(req.NextAttemptAt, d)) {
		d.Status = DeliveryDeadLettered
		d.DeadLetterReason = req.Error
		a.Stage = StageDeadLettered
	} else {
		d.Status = DeliveryRetryScheduled
		if req.NextAttemptAt.IsZero() {
			d.NextAttemptAt = at
		} else {
			d.NextAttemptAt = req.NextAttemptAt.UTC()
		}
	}

	s.appendReceiptLocked(Receipt{MessageID: d.MessageID, DeliveryID: d.ID, AttemptID: a.ID, Stage: StageFailed, At: at, Detail: req.Error})
	if a.Stage == StageDeadLettered {
		s.appendReceiptLocked(Receipt{MessageID: d.MessageID, DeliveryID: d.ID, AttemptID: a.ID, Stage: StageDeadLettered, At: at, Detail: req.Error})
	}
	s.attempts[a.ID] = a
	s.deliveries[d.ID] = d
	if d.Status == DeliveryDeadLettered {
		return cloneDelivery(d), cloneAttempt(a), ErrDeadLettered
	}
	return cloneDelivery(d), cloneAttempt(a), nil
}

func (s *MemoryStore) Redrive(_ context.Context, req RedriveRequest) (RecipientDelivery, error) {
	if req.AuthorizedBy == "" {
		return RecipientDelivery{}, ErrUnauthorized
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.deliveries[req.DeliveryID]
	if !ok {
		return RecipientDelivery{}, ErrNotFound
	}
	if d.Status != DeliveryDeadLettered {
		return RecipientDelivery{}, ErrInvalidArgument
	}
	at := req.At.UTC()
	if at.IsZero() {
		at = s.now()
	}
	d.Status = DeliveryPending
	d.NextAttemptAt = time.Time{}
	d.DeadlineAt = req.NewDeadlineAt.UTC()
	d.DeadLetterReason = ""
	d.UpdatedAt = at
	s.deliveries[d.ID] = d
	return cloneDelivery(d), nil
}

func (s *MemoryStore) Attempts(_ context.Context, deliveryID DeliveryID) ([]Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.deliveries[deliveryID]; !ok {
		return nil, ErrNotFound
	}
	ids := s.byDelivery[deliveryID]
	out := make([]Attempt, 0, len(ids))
	for _, id := range ids {
		out = append(out, cloneAttempt(s.attempts[id]))
	}
	return out, nil
}

func (s *MemoryStore) Receipts(_ context.Context, deliveryID DeliveryID) ([]Receipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.deliveries[deliveryID]; !ok {
		return nil, ErrNotFound
	}
	receipts := s.receipts[deliveryID]
	out := make([]Receipt, len(receipts))
	copy(out, receipts)
	return out, nil
}

func (s *MemoryStore) appendReceiptLocked(r Receipt) {
	if r.At.IsZero() {
		r.At = s.now()
	}
	for _, existing := range s.receipts[r.DeliveryID] {
		if existing.AttemptID == r.AttemptID && existing.Stage == r.Stage {
			return
		}
	}
	s.receipts[r.DeliveryID] = append(s.receipts[r.DeliveryID], r)
}

func (s *MemoryStore) currentLeaseLocked(ref LeaseRef, allowCompletedIDempotent bool) (RecipientDelivery, Attempt, error) {
	d, ok := s.deliveries[ref.DeliveryID]
	if !ok {
		return RecipientDelivery{}, Attempt{}, ErrNotFound
	}
	a, ok := s.attempts[ref.AttemptID]
	if !ok || a.DeliveryID != ref.DeliveryID || a.LeaseToken != ref.LeaseToken {
		return RecipientDelivery{}, Attempt{}, ErrStaleLease
	}
	if ref.BindingGeneration != a.BindingGeneration {
		return RecipientDelivery{}, Attempt{}, ErrStaleLease
	}
	if allowCompletedIDempotent && (a.Stage == StageFailed || a.Stage == StageDeadLettered || a.Stage == StageConsumed) {
		if d.ActiveAttemptID == "" || d.ActiveAttemptID == ref.AttemptID {
			return d, a, nil
		}
	}
	if terminal(d.Status) {
		return RecipientDelivery{}, Attempt{}, ErrTerminalDelivery
	}
	if d.ActiveAttemptID != ref.AttemptID || d.ActiveLeaseToken != ref.LeaseToken {
		return RecipientDelivery{}, Attempt{}, ErrStaleLease
	}
	if !s.now().Before(a.ExpiresAt) {
		s.expireLeaseLocked(d, a)
		return RecipientDelivery{}, Attempt{}, ErrStaleLease
	}
	return d, a, nil
}

func (s *MemoryStore) selectDeliveryLocked(now time.Time, req ClaimRequest) (DeliveryID, error) {
	if req.DeliveryID != "" {
		d, ok := s.deliveries[req.DeliveryID]
		if !ok {
			return "", ErrNotFound
		}
		if !req.Recipient.IsZero() && d.Recipient.URN() != req.Recipient.URN() {
			return "", ErrNoDeliveryReady
		}
		return req.DeliveryID, nil
	}
	ids := make([]string, 0, len(s.deliveries))
	for id := range s.deliveries {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	for _, raw := range ids {
		d := s.deliveries[DeliveryID(raw)]
		if !req.Recipient.IsZero() && d.Recipient.URN() != req.Recipient.URN() {
			continue
		}
		if terminal(d.Status) || d.Status == DeliveryDelivered {
			continue
		}
		if d.Status == DeliveryLeased && d.ActiveLeaseToken != "" && now.Before(d.LeaseExpiresAt) {
			continue
		}
		if !d.NextAttemptAt.IsZero() && now.Before(d.NextAttemptAt) {
			continue
		}
		return d.ID, nil
	}
	return "", ErrNoDeliveryReady
}

func (s *MemoryStore) releaseExpiredLocked() {
	now := s.now()
	for _, d := range s.deliveries {
		if d.Status == DeliveryLeased && d.ActiveAttemptID != "" && !d.LeaseExpiresAt.IsZero() && !now.Before(d.LeaseExpiresAt) {
			a := s.attempts[d.ActiveAttemptID]
			s.expireLeaseLocked(d, a)
		}
	}
}

func (s *MemoryStore) expireLeaseLocked(d RecipientDelivery, a Attempt) {
	now := s.now()
	a.Stage = StageFailed
	a.FailedAt = now
	a.Error = "lease expired"
	a.Retryable = true
	a.NextAttemptAt = now
	d.Status = DeliveryRetryScheduled
	d.ActiveAttemptID = ""
	d.ActiveLeaseToken = ""
	d.LeaseHolder = ""
	d.LeaseExpiresAt = time.Time{}
	d.NextAttemptAt = now
	d.UpdatedAt = now
	s.attempts[a.ID] = a
	s.deliveries[d.ID] = d
}

func (s *MemoryStore) deadLetterLocked(d RecipientDelivery, reason string) RecipientDelivery {
	d.Status = DeliveryDeadLettered
	d.ActiveAttemptID = ""
	d.ActiveLeaseToken = ""
	d.LeaseHolder = ""
	d.LeaseExpiresAt = time.Time{}
	d.DeadLetterReason = reason
	d.UpdatedAt = s.now()
	s.deliveries[d.ID] = d
	return d
}

func (s *MemoryStore) deliveriesForMessageLocked(id MessageID) []RecipientDelivery {
	ids := s.byMessage[id]
	out := make([]RecipientDelivery, 0, len(ids))
	for _, id := range ids {
		out = append(out, cloneDelivery(s.deliveries[id]))
	}
	return out
}

func (s *MemoryStore) now() time.Time { return s.clock.Now().UTC() }

func matchesFilter(now time.Time, d RecipientDelivery, f Filter) bool {
	if f.MessageID != "" && d.MessageID != f.MessageID {
		return false
	}
	if !f.Recipient.IsZero() && d.Recipient.URN() != f.Recipient.URN() {
		return false
	}
	if len(f.Status) > 0 {
		ok := false
		for _, st := range f.Status {
			if d.Status == st {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if f.ReadyOnly {
		if terminal(d.Status) || d.Status == DeliveryLeased {
			return false
		}
		if !d.NextAttemptAt.IsZero() && now.Before(d.NextAttemptAt) {
			return false
		}
		if deadlineExpired(now, d) {
			return false
		}
	}
	return true
}

func terminal(status DeliveryStatus) bool {
	return status == DeliveryDelivered || status == DeliveryDeadLettered || status == DeliveryCanceled
}

func deadlineExpired(at time.Time, d RecipientDelivery) bool {
	return !d.DeadlineAt.IsZero() && !at.Before(d.DeadlineAt)
}

func stageRank(stage ReceiptStage) int {
	switch stage {
	case StageLeaseAcquired:
		return 1
	case StageHostAccepted:
		return 2
	case StageTurnSubmitted:
		return 3
	case StageConsumed:
		return 4
	case StageFailed:
		return 5
	case StageDeadLettered, StageCanceled:
		return 6
	default:
		return 0
	}
}

func idempotencyScope(sender messaging.Address, key string) string {
	return sender.URN() + "\x00" + key
}

func newID() string {
	id, err := uuid.NewV7()
	if err != nil {
		panic(fmt.Errorf("uuid v7: %w", err))
	}
	return id.String()
}

func canonicalDigest(req EnqueueRequest) (Digest, error) {
	type canonicalRecipient struct {
		Address messaging.Address `json:"address"`
		Binding BindingTarget     `json:"binding"`
	}
	type canonical struct {
		From        messaging.Address    `json:"from"`
		Group       messaging.Address    `json:"group,omitempty"`
		Kind        messaging.Kind       `json:"kind"`
		Channel     messaging.Channel    `json:"channel,omitempty"`
		ThreadID    string               `json:"thread_id,omitempty"`
		InReplyTo   string               `json:"in_reply_to,omitempty"`
		Payload     []byte               `json:"payload,omitempty"`
		ContentType string               `json:"content_type,omitempty"`
		Metadata    map[string]string    `json:"metadata,omitempty"`
		Recipients  []canonicalRecipient `json:"recipients"`
	}
	recipients := make([]canonicalRecipient, len(req.Recipients))
	for i, r := range req.Recipients {
		binding := r.Binding
		if binding.Address.IsZero() {
			binding.Address = r.Address
		}
		recipients[i] = canonicalRecipient{Address: r.Address, Binding: binding}
	}
	// Digest is based on the resolved set, not argument ordering.
	sort.Slice(recipients, func(i, j int) bool {
		if recipients[i].Address.URN() == recipients[j].Address.URN() {
			return fmt.Sprintf("%+v", recipients[i].Binding) < fmt.Sprintf("%+v", recipients[j].Binding)
		}
		return recipients[i].Address.URN() < recipients[j].Address.URN()
	})
	body := canonical{
		From:        req.From,
		Group:       req.Group,
		Kind:        req.Kind,
		Channel:     req.Channel,
		ThreadID:    req.ThreadID,
		InReplyTo:   req.InReplyTo,
		Payload:     cloneBytes(req.Payload),
		ContentType: req.ContentType,
		Metadata:    cloneMap(req.Metadata),
		Recipients:  recipients,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	return digestHex(sha256.Sum256(data)), nil
}

func cloneMessage(m Message) Message {
	m.Payload = cloneBytes(m.Payload)
	m.Metadata = cloneMap(m.Metadata)
	return m
}

func cloneDelivery(d RecipientDelivery) RecipientDelivery { return d }

func cloneAttempt(a Attempt) Attempt { return a }

func cloneBytes(in []byte) []byte {
	if len(in) == 0 {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func cloneMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
