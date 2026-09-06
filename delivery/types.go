// Package delivery defines Messaging vNext reliable delivery primitives.
//
// The package is deliberately neutral: it depends only on go-messaging's
// Address/Kind/Channel value types and does not import agentkit, provider
// wrappers, Tether, Nanite, or Torque. It separates immutable message content
// from per-recipient obligations, leased attempts, and receipt stages.
package delivery

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	messaging "github.com/hollis-labs/go-messaging"
)

var (
	ErrNotFound         = errors.New("delivery record not found")
	ErrInvalidArgument  = errors.New("invalid delivery argument")
	ErrDigestConflict   = errors.New("sender idempotency digest conflict")
	ErrAlreadyClaimed   = errors.New("delivery already claimed")
	ErrNoDeliveryReady  = errors.New("no delivery ready")
	ErrStaleLease       = errors.New("stale delivery lease")
	ErrDeadlineExceeded = errors.New("delivery deadline exceeded")
	ErrDeadLettered     = errors.New("delivery dead-lettered")
	ErrUnauthorized     = errors.New("unauthorized delivery operation")
	ErrUnsupportedStage = errors.New("unsupported delivery receipt stage")
	ErrTerminalDelivery = errors.New("delivery is terminal")
)

// ID aliases make API intent explicit while preserving simple JSON/string use.
type MessageID string
type DeliveryID string
type AttemptID string
type LeaseToken string

// Digest is the hex SHA-256 of canonical immutable message content plus the
// frozen recipient set. Attempts, leases, observations, and attention state are
// excluded.
type Digest string

// DeliveryStatus is the per-recipient obligation lifecycle.
type DeliveryStatus string

const (
	DeliveryPending        DeliveryStatus = "pending"
	DeliveryLeased         DeliveryStatus = "leased"
	DeliveryRetryScheduled DeliveryStatus = "retry_scheduled"
	DeliveryDelivered      DeliveryStatus = "delivered"
	DeliveryDeadLettered   DeliveryStatus = "dead_lettered"
	DeliveryCanceled       DeliveryStatus = "canceled"
)

// ReceiptStage names observable durable handoff stages. These stages are
// receipts about transport/host state only; they do not prove model
// understanding or task success.
type ReceiptStage string

const (
	StagePersisted     ReceiptStage = "persisted"
	StageLeaseAcquired ReceiptStage = "lease_acquired"
	StageHostAccepted  ReceiptStage = "host_accepted"
	StageTurnSubmitted ReceiptStage = "turn_submitted"
	StageConsumed      ReceiptStage = "consumed"
	StageFailed        ReceiptStage = "failed"
	StageDeadLettered  ReceiptStage = "dead_lettered"
	StageCanceled      ReceiptStage = "canceled"
)

// BindingTarget records the exact actor/session/binding a host is claiming.
// A concrete session target must not be redirected to a replacement session;
// actor targets may be routed by a higher layer before enqueue and then frozen
// here with the resulting generation.
type BindingTarget struct {
	Address           messaging.Address `json:"address"`
	ActorID           string            `json:"actor_id,omitempty"`
	SessionID         string            `json:"session_id,omitempty"`
	HostID            string            `json:"host_id,omitempty"`
	BindingGeneration int64             `json:"binding_generation,omitempty"`
	RouteGeneration   int64             `json:"route_generation,omitempty"`
}

// RecipientTarget is one frozen resolved recipient obligation. Group fanout is
// represented by passing the resolved recipient snapshot; later membership
// changes create new sends rather than moving existing retries.
type RecipientTarget struct {
	Address messaging.Address `json:"address"`
	Binding BindingTarget     `json:"binding,omitempty"`
}

// Message is immutable after enqueue. Cancellation, attention state, attempts,
// and receipts are represented on separate records.
type Message struct {
	ID             MessageID         `json:"id"`
	Digest         Digest            `json:"digest"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	From           messaging.Address `json:"from"`
	Group          messaging.Address `json:"group,omitempty"`
	Kind           messaging.Kind    `json:"kind"`
	Channel        messaging.Channel `json:"channel,omitempty"`
	ThreadID       string            `json:"thread_id,omitempty"`
	InReplyTo      string            `json:"in_reply_to,omitempty"`
	Payload        []byte            `json:"payload,omitempty"`
	ContentType    string            `json:"content_type,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
}

// RecipientDelivery is one obligation for one frozen recipient.
type RecipientDelivery struct {
	ID               DeliveryID        `json:"id"`
	MessageID        MessageID         `json:"message_id"`
	Recipient        messaging.Address `json:"recipient"`
	Binding          BindingTarget     `json:"binding,omitempty"`
	Status           DeliveryStatus    `json:"status"`
	AttemptCount     int               `json:"attempt_count"`
	ActiveAttemptID  AttemptID         `json:"active_attempt_id,omitempty"`
	ActiveLeaseToken LeaseToken        `json:"active_lease_token,omitempty"`
	LeaseHolder      string            `json:"lease_holder,omitempty"`
	LeaseExpiresAt   time.Time         `json:"lease_expires_at,omitempty"`
	NextAttemptAt    time.Time         `json:"next_attempt_at,omitempty"`
	DeadlineAt       time.Time         `json:"deadline_at,omitempty"`
	DeadLetterReason string            `json:"dead_letter_reason,omitempty"`
	CreatedAt        time.Time         `json:"created_at"`
	UpdatedAt        time.Time         `json:"updated_at"`
}

// Receipt records one observable delivery stage. Receipt stages are evidence
// of persistence and handoff progress only; they do not assert model
// understanding or task success.
type Receipt struct {
	MessageID  MessageID    `json:"message_id"`
	DeliveryID DeliveryID   `json:"delivery_id"`
	AttemptID  AttemptID    `json:"attempt_id,omitempty"`
	Stage      ReceiptStage `json:"stage"`
	At         time.Time    `json:"at"`
	Detail     string       `json:"detail,omitempty"`
}

// Attempt records one host/runtime handoff attempt.
type Attempt struct {
	ID                AttemptID     `json:"id"`
	MessageID         MessageID     `json:"message_id"`
	DeliveryID        DeliveryID    `json:"delivery_id"`
	LeaseToken        LeaseToken    `json:"lease_token"`
	Holder            string        `json:"holder"`
	Binding           BindingTarget `json:"binding,omitempty"`
	BindingGeneration int64         `json:"binding_generation,omitempty"`
	AcquiredAt        time.Time     `json:"acquired_at"`
	ExpiresAt         time.Time     `json:"expires_at"`
	Stage             ReceiptStage  `json:"stage"`
	HostAcceptedAt    time.Time     `json:"host_accepted_at,omitempty"`
	TurnSubmittedAt   time.Time     `json:"turn_submitted_at,omitempty"`
	ConsumedAt        time.Time     `json:"consumed_at,omitempty"`
	FailedAt          time.Time     `json:"failed_at,omitempty"`
	Error             string        `json:"error,omitempty"`
	Retryable         bool          `json:"retryable,omitempty"`
	NextAttemptAt     time.Time     `json:"next_attempt_at,omitempty"`
}

// LeaseRef fences all attempt mutations.
type LeaseRef struct {
	DeliveryID        DeliveryID `json:"delivery_id"`
	AttemptID         AttemptID  `json:"attempt_id"`
	LeaseToken        LeaseToken `json:"lease_token"`
	BindingGeneration int64      `json:"binding_generation,omitempty"`
}

// EnqueueRequest atomically persists one immutable message and its frozen
// recipient obligations. IdempotencyKey is scoped by From.
type EnqueueRequest struct {
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	From           messaging.Address `json:"from"`
	Group          messaging.Address `json:"group,omitempty"`
	Recipients     []RecipientTarget `json:"recipients"`
	Kind           messaging.Kind    `json:"kind"`
	Channel        messaging.Channel `json:"channel,omitempty"`
	ThreadID       string            `json:"thread_id,omitempty"`
	InReplyTo      string            `json:"in_reply_to,omitempty"`
	Payload        []byte            `json:"payload,omitempty"`
	ContentType    string            `json:"content_type,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	DeadlineAt     time.Time         `json:"deadline_at,omitempty"`
}

// EnqueueResult returns the accepted immutable body and all frozen recipient
// obligations. Duplicate is true for same-sender/same-key/same-digest replay.
type EnqueueResult struct {
	Message    Message             `json:"message"`
	Deliveries []RecipientDelivery `json:"deliveries"`
	Duplicate  bool                `json:"duplicate"`
}

// DigestConflictError reports a sender-scoped idempotency conflict without
// exposing payload content.
type DigestConflictError struct {
	Sender            messaging.Address `json:"sender"`
	IdempotencyKey    string            `json:"idempotency_key"`
	ExistingMessageID MessageID         `json:"existing_message_id"`
	ExistingDigest    Digest            `json:"existing_digest"`
	NewDigest         Digest            `json:"new_digest"`
}

func (e *DigestConflictError) Error() string {
	return fmt.Sprintf("%v: sender=%s key=%q existing_message=%s", ErrDigestConflict, e.Sender.URN(), e.IdempotencyKey, e.ExistingMessageID)
}

func (e *DigestConflictError) Unwrap() error { return ErrDigestConflict }

// ClaimRequest leases one ready delivery for an exact holder/binding.
type ClaimRequest struct {
	Recipient         messaging.Address `json:"recipient,omitempty"`
	DeliveryID        DeliveryID        `json:"delivery_id,omitempty"`
	Holder            string            `json:"holder"`
	BindingGeneration int64             `json:"binding_generation,omitempty"`
	LeaseDuration     time.Duration     `json:"lease_duration"`
	Nowait            bool              `json:"nowait,omitempty"`
}

// ClaimResult returns the delivery, immutable body, and leased attempt.
type ClaimResult struct {
	Message  Message           `json:"message"`
	Delivery RecipientDelivery `json:"delivery"`
	Attempt  Attempt           `json:"attempt"`
}

// AckRequest records a positive receipt stage for the current lease. Host
// acceptance, turn submission, and consumption remain distinct observations.
type AckRequest struct {
	Lease LeaseRef     `json:"lease"`
	Stage ReceiptStage `json:"stage"`
	At    time.Time    `json:"at,omitempty"`
}

// NackRequest records a failed attempt. Retryable failures schedule another
// claim; terminal failures or exhausted deadlines dead-letter the obligation.
type NackRequest struct {
	Lease         LeaseRef  `json:"lease"`
	Retryable     bool      `json:"retryable"`
	Error         string    `json:"error,omitempty"`
	NextAttemptAt time.Time `json:"next_attempt_at,omitempty"`
	At            time.Time `json:"at,omitempty"`
}

// RedriveRequest reopens a dead-lettered delivery only when the caller has
// already been authorized by the host/service layer.
type RedriveRequest struct {
	DeliveryID    DeliveryID `json:"delivery_id"`
	AuthorizedBy  string     `json:"authorized_by"`
	NewDeadlineAt time.Time  `json:"new_deadline_at,omitempty"`
	At            time.Time  `json:"at,omitempty"`
}

// Filter narrows delivery listing. Listing is read-only and never claims,
// acknowledges, or advances attempts.
type Filter struct {
	MessageID MessageID         `json:"message_id,omitempty"`
	Recipient messaging.Address `json:"recipient,omitempty"`
	Status    []DeliveryStatus  `json:"status,omitempty"`
	ReadyOnly bool              `json:"ready_only,omitempty"`
	Limit     int               `json:"limit,omitempty"`
}

// Store is the reliable delivery persistence contract.
type Store interface {
	Enqueue(context.Context, EnqueueRequest) (EnqueueResult, error)
	GetMessage(context.Context, MessageID) (Message, error)
	GetDelivery(context.Context, DeliveryID) (RecipientDelivery, error)
	ListDeliveries(context.Context, Filter) ([]RecipientDelivery, error)
	Claim(context.Context, ClaimRequest) (ClaimResult, error)
	ExtendLease(context.Context, LeaseRef, time.Time) (Attempt, error)
	Ack(context.Context, AckRequest) (RecipientDelivery, Attempt, error)
	Nack(context.Context, NackRequest) (RecipientDelivery, Attempt, error)
	Redrive(context.Context, RedriveRequest) (RecipientDelivery, error)
	Attempts(context.Context, DeliveryID) ([]Attempt, error)
	Receipts(context.Context, DeliveryID) ([]Receipt, error)
}

func digestHex(sum [32]byte) Digest { return Digest(hex.EncodeToString(sum[:])) }
