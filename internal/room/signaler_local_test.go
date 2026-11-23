package room

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/huykhanh2141/croc-p2p/pkg/protocol"
)

// testPeer builds a minimal online peer for signalling tests.
func testPeer(id string) protocol.PeerInfo {
	now := time.Now().UTC()
	return protocol.PeerInfo{
		ID:       id,
		Name:     id,
		Status:   protocol.PeerOnline,
		JoinedAt: now,
		LastSeen: now,
	}
}

// TestLocalSignalerLobbyHoldsTwoPeers covers the relay limit that makes the
// waiting-room retry necessary: two peers fit, the third is rejected until a
// slot is freed.
func TestLocalSignalerLobbyHoldsTwoPeers(t *testing.T) {
	ctx := context.Background()
	net := NewLocalNetwork()
	a, b, c := net.Signaler(), net.Signaler(), net.Signaler()

	if err := a.Join(ctx, "room-1", testPeer("a")); err != nil {
		t.Fatalf("first join: %v", err)
	}
	if err := b.Join(ctx, "room-1", testPeer("b")); err != nil {
		t.Fatalf("second join: %v", err)
	}

	if err := c.Join(ctx, "room-1", testPeer("c")); !errors.Is(err, ErrRoomFull) {
		t.Fatalf("third join error = %v; want ErrRoomFull", err)
	}

	// Re-joining the same room with the same identity is idempotent.
	if err := b.Join(ctx, "room-1", testPeer("b")); err != nil {
		t.Fatalf("idempotent re-join: %v", err)
	}

	// Freeing a slot lets the newcomer in.
	if err := a.Leave(ctx, "room-1"); err != nil {
		t.Fatalf("leave: %v", err)
	}
	if err := c.Join(ctx, "room-1", testPeer("c")); err != nil {
		t.Fatalf("join after a slot was freed: %v", err)
	}

	// And the lobby is full again.
	late := net.Signaler()
	if err := late.Join(ctx, "room-1", testPeer("late")); !errors.Is(err, ErrRoomFull) {
		t.Fatalf("join when full again = %v; want ErrRoomFull", err)
	}
}

// TestLocalSignalerSendDeliversToSinglePeer checks unicast delivery, that the
// sender does not receive its own message, and the error for unknown peers.
func TestLocalSignalerSendDeliversToSinglePeer(t *testing.T) {
	ctx := context.Background()
	net := NewLocalNetwork()
	a, b := net.Signaler(), net.Signaler()

	if err := a.Join(ctx, "room-1", testPeer("a")); err != nil {
		t.Fatalf("a join: %v", err)
	}
	if err := b.Join(ctx, "room-1", testPeer("b")); err != nil {
		t.Fatalf("b join: %v", err)
	}

	msg := *protocol.NewControlMessage(protocol.MsgPeerList, "room-1", "a")
	msg.Peers = []protocol.PeerInfo{testPeer("a")}
	if err := a.Send(ctx, "b", msg); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case got := <-b.Recv():
		if got.Type != protocol.MsgPeerList || got.SenderID != "a" || got.RoomID != "room-1" {
			t.Fatalf("received %+v; want PEER_LIST from a in room-1", got)
		}
	case <-time.After(time.Second):
		t.Fatal("no message delivered to the target peer")
	}

	select {
	case got := <-a.Recv():
		t.Fatalf("sender received its own message: %+v", got)
	default:
	}

	if err := a.Send(ctx, "ghost", msg); !errors.Is(err, protocol.ErrPeerOffline) {
		t.Fatalf("send to unknown peer = %v; want protocol.ErrPeerOffline", err)
	}
}

// stubSignaler scripts Join outcomes so the retry loop can be tested without
// any timing.
type stubSignaler struct {
	joinCalls atomic.Int32
	joinErr   func(call int32) error
}

func (s *stubSignaler) Join(context.Context, string, protocol.PeerInfo) error {
	call := s.joinCalls.Add(1)
	if s.joinErr == nil {
		return nil
	}
	return s.joinErr(call)
}

func (s *stubSignaler) Leave(context.Context, string) error { return nil }

func (s *stubSignaler) SendRoom(context.Context, string, protocol.ControlMessage) error { return nil }

func (s *stubSignaler) Send(context.Context, string, protocol.ControlMessage) error { return nil }

func (s *stubSignaler) Recv() <-chan protocol.ControlMessage {
	return make(chan protocol.ControlMessage)
}

func (s *stubSignaler) Close() error { return nil }

// TestJoinWithRetrySucceedsAfterBusyAttempts checks the retry loop stops as
// soon as the room has a free slot.
func TestJoinWithRetrySucceedsAfterBusyAttempts(t *testing.T) {
	stub := &stubSignaler{joinErr: func(call int32) error {
		if call < 3 {
			return fmt.Errorf("busy: %w", ErrRoomFull)
		}
		return nil
	}}

	err := joinWithRetry(context.Background(), stub, "room-1", testPeer("a"), 5, time.Millisecond, 2*time.Millisecond)
	if err != nil {
		t.Fatalf("joinWithRetry: %v", err)
	}
	if got := stub.joinCalls.Load(); got != 3 {
		t.Fatalf("join attempts = %d; want 3", got)
	}
}

// TestJoinWithRetryGivesUpWhenRoomStaysFull checks the five-attempt budget and
// that the final error is still recognisable with errors.Is.
func TestJoinWithRetryGivesUpWhenRoomStaysFull(t *testing.T) {
	stub := &stubSignaler{joinErr: func(int32) error {
		return fmt.Errorf("busy: %w", ErrRoomFull)
	}}

	err := joinWithRetry(context.Background(), stub, "room-1", testPeer("a"), 5, time.Millisecond, 2*time.Millisecond)
	if !errors.Is(err, ErrRoomFull) {
		t.Fatalf("error = %v; want it to wrap ErrRoomFull", err)
	}
	if got := stub.joinCalls.Load(); got != 5 {
		t.Fatalf("join attempts = %d; want 5", got)
	}
}

// TestJoinWithRetryStopsOnOtherErrors makes sure only ErrRoomFull is retried.
func TestJoinWithRetryStopsOnOtherErrors(t *testing.T) {
	boom := errors.New("boom")
	stub := &stubSignaler{joinErr: func(int32) error { return boom }}

	err := joinWithRetry(context.Background(), stub, "room-1", testPeer("a"), 5, time.Millisecond, 2*time.Millisecond)
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v; want the original error", err)
	}
	if got := stub.joinCalls.Load(); got != 1 {
		t.Fatalf("join attempts = %d; want 1", got)
	}
}

// TestJoinWithRetryHonoursCancelledContext checks the loop stops when the
// caller gives up.
func TestJoinWithRetryHonoursCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stub := &stubSignaler{joinErr: func(int32) error {
		return fmt.Errorf("busy: %w", ErrRoomFull)
	}}

	err := joinWithRetry(ctx, stub, "room-1", testPeer("a"), 5, time.Millisecond, 2*time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v; want context.Canceled", err)
	}
	if got := stub.joinCalls.Load(); got != 1 {
		t.Fatalf("join attempts = %d; want 1", got)
	}
}

// releaseOnFull frees a reserved lobby slot the first time the real signaler
// reports ErrRoomFull, so the retry loop is guaranteed to observe the slot
// becoming free (no sleep-based race in the test).
type releaseOnFull struct {
	Signaler
	release func()
	once    sync.Once
}

func (r *releaseOnFull) Join(ctx context.Context, roomID string, self protocol.PeerInfo) error {
	err := r.Signaler.Join(ctx, roomID, self)
	if errors.Is(err, ErrRoomFull) {
		r.once.Do(func() { time.AfterFunc(10*time.Millisecond, r.release) })
	}
	return err
}

// TestJoinWithRetryThroughLocalNetwork is the integration version: a real
// in-memory lobby is full, the occupied slot is freed shortly after the first
// rejection, and the retry loop gets in.
func TestJoinWithRetryThroughLocalNetwork(t *testing.T) {
	ctx := context.Background()
	net := NewLocalNetwork()

	coord := net.Signaler()
	if err := coord.Join(ctx, "room-1", testPeer("coord")); err != nil {
		t.Fatalf("coordinator join: %v", err)
	}
	release := net.Reserve("room-1")
	t.Cleanup(release) // idempotent: a second call finds nothing to remove

	joiner := &releaseOnFull{Signaler: net.Signaler(), release: release}
	if err := joinWithRetry(ctx, joiner, "room-1", testPeer("joiner"), 10, 5*time.Millisecond, 25*time.Millisecond); err != nil {
		t.Fatalf("retry should have joined after the slot was freed: %v", err)
	}

	// The lobby is full again: a third newcomer has to retry later.
	third := net.Signaler()
	if err := third.Join(ctx, "room-1", testPeer("third")); !errors.Is(err, ErrRoomFull) {
		t.Fatalf("third join error = %v; want ErrRoomFull", err)
	}

	// Leaving the waiting room is what frees the slot for the next newcomer.
	if err := joiner.Leave(ctx, "room-1"); err != nil {
		t.Fatalf("joiner leave: %v", err)
	}
	if err := third.Join(ctx, "room-1", testPeer("third")); err != nil {
		t.Fatalf("join after the newcomer left: %v", err)
	}
}

// TestBackoffDelayStaysWithinBounds checks the jitter window, including the cap.
func TestBackoffDelayStaysWithinBounds(t *testing.T) {
	base, max := 50*time.Millisecond, 250*time.Millisecond
	for attempt := 0; attempt < 8; attempt++ {
		for i := 0; i < 50; i++ {
			got := backoffDelay(attempt, base, max)
			if got < 0 || got >= max {
				t.Fatalf("backoffDelay(%d) = %v; want [0, %v)", attempt, got, max)
			}
		}
	}
	if got := backoffDelay(3, 0, max); got != 0 {
		t.Fatalf("backoffDelay with zero base = %v; want 0", got)
	}
}

// TestManagerRetryDefaults pins the agreed policy: five attempts, 50-250ms.
func TestManagerRetryDefaults(t *testing.T) {
	if DefaultJoinAttempts != 5 {
		t.Fatalf("DefaultJoinAttempts = %d; want 5", DefaultJoinAttempts)
	}
	if DefaultRetryBase != 50*time.Millisecond || DefaultRetryMax != 250*time.Millisecond {
		t.Fatalf("retry window = %v..%v; want 50ms..250ms", DefaultRetryBase, DefaultRetryMax)
	}

	m := New("")
	t.Cleanup(func() { _ = m.Close() })

	if m.signaler == nil {
		t.Fatal("New returned a manager without a signaler")
	}
	if m.retryAttempts != DefaultJoinAttempts || m.retryBase != DefaultRetryBase || m.retryMax != DefaultRetryMax {
		t.Fatalf("manager retry policy = %d/%v/%v; want %d/%v/%v",
			m.retryAttempts, m.retryBase, m.retryMax, DefaultJoinAttempts, DefaultRetryBase, DefaultRetryMax)
	}
	if m.relayAddr != protocol.DefaultRelay {
		t.Fatalf("relayAddr = %q; want %q", m.relayAddr, protocol.DefaultRelay)
	}
}
