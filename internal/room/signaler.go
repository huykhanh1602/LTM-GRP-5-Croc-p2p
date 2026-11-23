package room

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/huykhanh2141/croc-p2p/pkg/protocol"
)

// ErrRoomFull reports that a signalling room already holds two peers. The croc
// relay only pairs two peers per room, so a newcomer must wait and retry.
var ErrRoomFull = errors.New("room: signalling room is full")

// Waiting-room retry policy: five attempts with a random delay between 50ms and
// 250ms, so two newcomers that arrived together spread out instead of colliding
// again immediately.
const (
	DefaultJoinAttempts = 5
	DefaultRetryBase    = 50 * time.Millisecond
	DefaultRetryMax     = 250 * time.Millisecond
)

// Signaler is the transport Manager uses for the control plane. Manager only
// produces and consumes ControlMessage values; whether they travel over the
// croc relay or an in-process bus is the implementation's business.
//
// The "waiting room" (lobby) is where a newcomer meets the coordinator: the
// coordinator keeps one free relay room open, the newcomer joins it, receives
// PEER_LIST and leaves again so the slot is free for the next newcomer.
type Signaler interface {
	// Join enters the roomID waiting room as self. It returns an error wrapping
	// ErrRoomFull when another peer already occupies the second slot.
	Join(ctx context.Context, roomID string, self protocol.PeerInfo) error
	// Leave frees this peer's waiting-room slot again.
	Leave(ctx context.Context, roomID string) error
	// SendRoom delivers msg to every other member of roomID, which must have
	// been joined with Join. Signalling rooms hold at most two peers, so this
	// is effectively unicast; it is how a newcomer reaches the coordinator
	// whose peer ID it does not know yet.
	SendRoom(ctx context.Context, roomID string, msg protocol.ControlMessage) error
	// Send delivers msg to exactly one peer.
	Send(ctx context.Context, to string, msg protocol.ControlMessage) error
	// Recv is the stream of inbound control messages.
	Recv() <-chan protocol.ControlMessage
	// Close releases every connection. It must be safe to call twice.
	Close() error
}

// joinWithRetry joins the waiting room, retrying with a short random backoff
// while it is still occupied. It stops early on any other error, when the
// context is cancelled, or when the attempts are exhausted; the final error
// still wraps ErrRoomFull so callers can use errors.Is.
func joinWithRetry(ctx context.Context, sig Signaler, roomID string, self protocol.PeerInfo, attempts int, base, max time.Duration) error {
	if attempts < 1 {
		attempts = 1
	}

	var err error
	for attempt := 0; attempt < attempts; attempt++ {
		if err = sig.Join(ctx, roomID, self); err == nil {
			return nil
		}
		if !errors.Is(err, ErrRoomFull) {
			return err
		}
		if attempt == attempts-1 {
			break
		}
		if waitErr := sleepCtx(ctx, backoffDelay(attempt, base, max)); waitErr != nil {
			return waitErr
		}
	}
	return fmt.Errorf("room: waiting room %q still full after %d attempts: %w", roomID, attempts, err)
}

// backoffDelay doubles the base delay once per attempt, caps it at max and
// returns a random duration in [0, cap) so contending newcomers spread out.
func backoffDelay(attempt int, base, max time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	ceiling := base
	for i := 0; i < attempt; i++ {
		if max > 0 && ceiling >= max {
			break
		}
		ceiling *= 2
	}
	if max > 0 && ceiling > max {
		ceiling = max
	}
	return time.Duration(rand.Int64N(int64(ceiling)))
}

// sleepCtx waits for d unless the context ends first.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
