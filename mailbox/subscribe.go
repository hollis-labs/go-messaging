package mailbox

// In-process pubsub used by Service.SendMessage to fan out newly-
// inserted messages to live streaming subscribers. There is no
// replay buffer: a subscriber only sees messages published after its
// subscribe call takes effect, and subscribers that cannot keep up
// with their channel buffer are dropped silently (the publish is
// non-blocking).
//
// Concurrency model:
//
//   - subscribe appends a buffered channel to the per-key slice under
//     a write lock, then spawns a goroutine that waits on ctx.Done()
//     to remove and close the channel under a write lock.
//   - publish holds the RLock across the non-blocking sends. Non-
//     blocking sends are O(1) and cannot wedge the lock, and holding
//     the RLock prevents a close/send race with the unsubscribe
//     goroutine, which takes the write lock before closing any
//     channel. A previous version snapshotted the subscriber slice
//     and released the lock before sending, which raced with close on
//     shutdown and could panic with "send on closed channel" —
//     default in a select does not rescue that, because default only
//     fires when the send would block.
import (
	"context"
	"fmt"
	"sync"
)

// subscriberBufferSize is the capacity of each subscriber's channel.
// 16 is enough to absorb small bursts without dropping, but small
// enough that a wedged consumer is noticed quickly.
const subscriberBufferSize = 16

// pubsub is a minimal in-process fan-out hub keyed by
// sessionID + ":" + agentID. It is safe for concurrent use.
type pubsub struct {
	mu   sync.RWMutex
	subs map[string][]chan *Message
}

// newPubsub constructs an empty pubsub.
func newPubsub() *pubsub {
	return &pubsub{subs: make(map[string][]chan *Message)}
}

// subscribe registers a new subscriber for the given (sessionID,
// agentID) pair and returns the receive side of a buffered channel.
// The caller is expected to cancel ctx when the subscription should
// end; a goroutine owned by the pubsub then removes the channel from
// the subscriber table and closes it so that pending receivers
// unblock.
func (p *pubsub) subscribe(ctx context.Context, sessionID, agentID string) <-chan *Message {
	key := sessionID + ":" + agentID
	ch := make(chan *Message, subscriberBufferSize)

	p.mu.Lock()
	p.subs[key] = append(p.subs[key], ch)
	p.mu.Unlock()

	goSafe(ctx, "mailbox.pubsub.unsubscribe", func(context.Context) {
		<-ctx.Done()
		p.mu.Lock()
		defer p.mu.Unlock()
		list := p.subs[key]
		found := false
		for i, c := range list {
			if c == ch {
				p.subs[key] = append(list[:i], list[i+1:]...)
				found = true
				break
			}
		}
		if len(p.subs[key]) == 0 {
			delete(p.subs, key)
		}
		// Only close if we removed it from the map; otherwise
		// closeAll (service shutdown) already closed the channel and
		// doing so again would panic.
		if found {
			close(ch)
		}
	})

	return ch
}

// closeAll drains every subscriber channel and empties the subscriber
// map. Called from Service.Close on graceful shutdown so receivers
// blocked on <-ch unblock with a closed-channel signal. After
// closeAll, publish is a no-op (map is empty) and any later per-
// subscriber unsubscribe-on-ctx-done goroutine skips its close(ch)
// call because its channel is no longer in the map (see subscribe's
// found guard).
func (p *pubsub) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, list := range p.subs {
		for _, ch := range list {
			close(ch)
		}
	}
	p.subs = make(map[string][]chan *Message)
}

// publish fans the message out to every subscriber whose key matches
// the message's destination (ToSessionID, ToAgentID). The send is
// non-blocking: if a subscriber's channel is full, the message is
// dropped for that subscriber only.
//
// The RLock is held across the non-blocking sends. This is safe
// because non-blocking sends are O(1) and cannot wedge the lock, and
// it is necessary because it blocks the unsubscribe goroutine from
// taking the write lock and closing a subscriber channel while
// publish is still sending to it. Without this invariant, publish
// could race with close and panic with "send on closed channel" —
// default in the select does not rescue that, because default only
// fires when the send would block, not when it would panic.
func (p *pubsub) publish(msg *Message) {
	if msg == nil {
		return
	}
	key := msg.ToSessionID + ":" + msg.ToAgentID

	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, ch := range p.subs[key] {
		select {
		case ch <- msg:
		default:
			// Subscriber buffer full: drop for this subscriber only.
		}
	}
}

// SubscribeSessionAgent validates the requested agent ID and returns
// a receive-only channel on which live messages addressed to
// (sessionID, agentID) will arrive. The caller must cancel ctx to
// release the subscription; doing so unblocks any pending receive
// with a closed-channel signal. A context that is never canceled
// leaks one goroutine and one buffered channel per call for the
// lifetime of the Service — always pass a cancelable context and
// cancel it when the subscription ends.
func (svc *Service) SubscribeSessionAgent(ctx context.Context, sessionID, agentID string) (<-chan *Message, error) {
	if err := ValidateAgentID(ctx, svc.resolver, agentID); err != nil {
		return nil, fmt.Errorf("%w: agent_id: %w", ErrValidation, err)
	}
	return svc.pub.subscribe(ctx, sessionID, agentID), nil
}
