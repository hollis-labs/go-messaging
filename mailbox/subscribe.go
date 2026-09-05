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
//     a write lock, then spawns a goroutine that waits on either ctx.Done()
//     or service shutdown to remove and close the channel under a write lock.
//   - publish holds the RLock across the non-blocking sends. Non-
//     blocking sends are O(1) and cannot wedge the lock, and holding
//     the RLock prevents a close/send race with the unsubscribe
//     goroutine, which takes the write lock before closing any
//     channel. A previous version snapshotted the subscriber slice
//     and released the lock before sending, which raced with close on
//     shutdown and could panic with "send on closed channel" —
//     default in a select does not rescue that, because default only
//     fires when the send would block.
//   - shutdown closes done while holding the write lock, which permanently
//     closes admission. Janitor accounting is incremented under the same lock,
//     so closeAll can safely wait until every admitted janitor exits.
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
	mu       sync.RWMutex
	subs     map[subscriptionKey][]chan *Message
	done     chan struct{}
	janitors sync.WaitGroup
	closed   bool
}

type subscriptionKey struct {
	sessionID string
	agentID   string
}

// newPubsub constructs an empty pubsub.
func newPubsub() *pubsub {
	return &pubsub{
		subs: make(map[subscriptionKey][]chan *Message),
		done: make(chan struct{}),
	}
}

// subscribe registers a new subscriber for the given (sessionID,
// agentID) pair and returns the receive side of a buffered channel.
// The caller should cancel ctx when the subscription should end. A goroutine
// owned by the pubsub removes and closes the channel on cancellation, or exits
// during closeAll even if ctx is not cancelable.
func (p *pubsub) subscribe(ctx context.Context, sessionID, agentID string) (<-chan *Message, error) {
	key := subscriptionKey{sessionID: sessionID, agentID: agentID}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrClosed
	}
	ch := make(chan *Message, subscriberBufferSize)
	p.subs[key] = append(p.subs[key], ch)
	// Add while admission is serialized by mu. Once closeAll acquires mu and
	// marks closed, no future Add is possible, so waiting is safe.
	p.janitors.Add(1)
	p.mu.Unlock()

	goSafe(ctx, "mailbox.pubsub.unsubscribe", func(context.Context) {
		defer p.janitors.Done()
		select {
		case <-ctx.Done():
		case <-p.done:
		}
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

	return ch, nil
}

// closeAll closes every subscriber channel, rejects later admission, and waits
// for all subscription janitors to terminate. Shutdown and context cancellation
// both remove under mu and use the found guard, so each channel is closed once.
func (p *pubsub) closeAll() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.janitors.Wait()
		return
	}
	p.closed = true
	close(p.done)
	for _, list := range p.subs {
		for _, ch := range list {
			close(ch)
		}
	}
	p.subs = make(map[subscriptionKey][]chan *Message)
	p.mu.Unlock()

	p.janitors.Wait()
}

// publish fans the message out to every subscriber whose key matches
// the message's destination (ToSessionID, ToAgentID). The send is
// non-blocking: if a subscriber's channel is full, the message is
// dropped for that subscriber only. Each successful delivery receives a deep
// copy, so subscribers and the SendMessage caller never share mutable storage.
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
	key := subscriptionKey{sessionID: msg.ToSessionID, agentID: msg.ToAgentID}

	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, ch := range p.subs[key] {
		select {
		case ch <- cloneMessage(msg):
		default:
			// Subscriber buffer full: drop for this subscriber only.
		}
	}
}

// SubscribeSessionAgent validates the requested agent ID and returns
// a receive-only channel on which live messages addressed to
// (sessionID, agentID) will arrive. The caller must cancel ctx to
// release the subscription; doing so unblocks any pending receive
// with a closed-channel signal. Service.Close also closes the channel and
// terminates the subscription janitor, including when ctx is Background.
//
// Every delivered *Message is independently owned by that subscriber. It does
// not alias the value returned by SendMessage or another subscriber's value.
func (svc *Service) SubscribeSessionAgent(ctx context.Context, sessionID, agentID string) (<-chan *Message, error) {
	if err := ValidateAgentID(ctx, svc.resolver, agentID); err != nil {
		return nil, fmt.Errorf("%w: agent_id: %w", ErrValidation, err)
	}
	ch, err := svc.pub.subscribe(ctx, sessionID, agentID)
	if err != nil {
		return nil, fmt.Errorf("subscribe: %w", err)
	}
	return ch, nil
}
