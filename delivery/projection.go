package delivery

import (
	messaging "github.com/hollis-labs/go-messaging"
)

// EnvelopeEnqueueRequest projects a legacy root Envelope send into the reliable
// delivery core. It is an explicit adapter boundary: callers choose this path
// instead of shadow-writing a second authoritative inbox.
func EnvelopeEnqueueRequest(env messaging.Envelope) (EnqueueRequest, error) {
	if env.From.IsZero() || env.To.IsZero() || env.Kind == "" {
		return EnqueueRequest{}, ErrInvalidArgument
	}
	if env.DeliveredAt != nil || env.ConsumedAt != nil {
		return EnqueueRequest{}, messaging.ErrPresetLifecycle
	}
	return EnqueueRequest{
		From:        env.From,
		Recipients:  []RecipientTarget{{Address: env.To, Binding: BindingTarget{Address: env.To}}},
		Kind:        env.Kind,
		Channel:     env.Channel,
		ThreadID:    env.ThreadID,
		InReplyTo:   env.InReplyTo,
		Payload:     cloneBytes(env.Payload),
		ContentType: env.ContentType,
		Metadata:    cloneMap(env.Metadata),
	}, nil
}

// EnvelopeFromDelivery projects one immutable message plus one recipient
// obligation back to the root Envelope shape. Receipt stages are used only to
// fill the root compatibility lifecycle fields; they remain transport facts.
func EnvelopeFromDelivery(msg Message, d RecipientDelivery, receipts []Receipt) messaging.Envelope {
	env := messaging.Envelope{
		ID:          string(msg.ID),
		Kind:        msg.Kind,
		Channel:     msg.Channel,
		From:        msg.From,
		To:          d.Recipient,
		ThreadID:    msg.ThreadID,
		InReplyTo:   msg.InReplyTo,
		Payload:     cloneBytes(msg.Payload),
		ContentType: msg.ContentType,
		Metadata:    cloneMap(msg.Metadata),
		CreatedAt:   msg.CreatedAt,
	}
	for _, r := range receipts {
		switch r.Stage {
		case StageHostAccepted, StageTurnSubmitted:
			if env.DeliveredAt == nil {
				at := r.At
				env.DeliveredAt = &at
			}
		case StageConsumed:
			if env.DeliveredAt == nil {
				at := r.At
				env.DeliveredAt = &at
			}
			if env.ConsumedAt == nil {
				at := r.At
				env.ConsumedAt = &at
			}
		}
	}
	if d.Status == DeliveryDelivered && env.DeliveredAt == nil {
		at := d.UpdatedAt
		env.DeliveredAt = &at
	}
	return env
}

// StableActorTarget addresses a durable actor without fabricating a session.
func StableActorTarget(address messaging.Address, actorID string, generation int64) RecipientTarget {
	return RecipientTarget{Address: address, Binding: BindingTarget{Address: address, ActorID: actorID, BindingGeneration: generation}}
}

// ExactSessionTarget addresses one concrete session. Higher layers must not
// redirect this delivery to a replacement session.
func ExactSessionTarget(address messaging.Address, sessionID string, generation int64) RecipientTarget {
	return RecipientTarget{Address: address, Binding: BindingTarget{Address: address, SessionID: sessionID, BindingGeneration: generation}}
}
