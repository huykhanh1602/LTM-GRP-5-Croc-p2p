package room

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/huykhanh2141/croc-p2p/pkg/protocol"
)

// slowSignaler delays every PONG by d, standing in for a slow link so the RTT
// assertion is about the measurement and not about clock noise.
type slowSignaler struct {
	Signaler
	delay time.Duration
}

func (s *slowSignaler) Send(ctx context.Context, to string, msg protocol.ControlMessage) error {
	if msg.Type == protocol.MsgPong {
		time.Sleep(s.delay)
	}
	return s.Signaler.Send(ctx, to, msg)
}

// TestHeartbeatMeasuresRoundTrip checks that PING/PONG produces a real RTT and
// refreshes liveness on both sides.
func TestHeartbeatMeasuresRoundTrip(t *testing.T) {
	net, coord, coordSelf := newTestRoom(t, "room-1")

	slow := &slowSignaler{Signaler: net.Signaler(), delay: 5 * time.Millisecond}
	peer := NewWithSignaler(slow)
	peer.heartbeatInterval = time.Hour
	t.Cleanup(func() { _ = peer.Close() })

	peerSelf, _, err := peer.JoinRoom(context.Background(), "room-1", "secret")
	if err != nil {
		t.Fatalf("JoinRoom: %v", err)
	}

	before := rosterPeer(t, coord, peerSelf.ID).LastSeen

	rtt, err := coord.Heartbeat(context.Background(), peerSelf.ID)
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if rtt < 2*time.Millisecond {
		t.Fatalf("round-trip time = %v; want it to include the injected 5ms PONG delay", rtt)
	}
	if got := rosterPeer(t, coord, peerSelf.ID).LastSeen; !got.After(before) {
		t.Fatalf("LastSeen was not refreshed: %v -> %v", before, got)
	}
	if got := rosterPeer(t, coord, coordSelf.ID); got.Status != protocol.PeerOnline {
		t.Fatalf("coordinator status = %q; want online", got.Status)
	}
	if c, ok := peer.Coordinator(); !ok || c.ID != coordSelf.ID {
		t.Fatalf("peer coordinator = %+v, %v; want %q", c, ok, coordSelf.ID)
	}

	if _, err := coord.Heartbeat(context.Background(), "ghost"); !errors.Is(err, protocol.ErrPeerOffline) {
		t.Fatalf("Heartbeat for unknown peer = %v; want protocol.ErrPeerOffline", err)
	}
}

// TestHeartbeatTimesOutWhenPeerSilent uses a signaler that swallows messages:
// the PING is "sent" but no PONG ever arrives.
func TestHeartbeatTimesOutWhenPeerSilent(t *testing.T) {
	m := NewWithSignaler(&stubSignaler{})
	m.heartbeatInterval = time.Hour
	m.peerTimeout = 40 * time.Millisecond
	t.Cleanup(func() { _ = m.Close() })

	if _, err := m.CreateRoom(context.Background(), "room-1", "secret"); err != nil {
		t.Fatalf("CreateRoom: %v", err)
	}
	now := time.Now().UTC()
	addRosterPeer(t, m, "silent-peer", now, now)

	start := time.Now()
	if _, err := m.Heartbeat(context.Background(), "silent-peer"); !errors.Is(err, protocol.ErrPeerOffline) {
		t.Fatalf("Heartbeat error = %v; want protocol.ErrPeerOffline", err)
	}
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Fatalf("Heartbeat returned after %v; want it to wait for the peer timeout", elapsed)
	}

	m.mu.RLock()
	pending := len(m.pendingPings)
	m.mu.RUnlock()
	if pending != 0 {
		t.Fatalf("%d pending pings left behind; want 0", pending)
	}
}

// TestCoordinatorPingsEveryMember checks the coordinator side of the split
// liveness rule: it probes every member each tick.
func TestCoordinatorPingsEveryMember(t *testing.T) {
	net, coord, coordSelf := newTestRoom(t, "room-1")
	memberA, aSelf, _ := joinTestRoom(t, net, "room-1")
	_, bSelf, _ := joinTestRoom(t, net, "room-1")
	waitForRosterSize(t, coord, 3)

	beforeA := rosterPeer(t, coord, aSelf.ID).LastSeen
	beforeB := rosterPeer(t, coord, bSelf.ID).LastSeen
	beforeCoordFromA := rosterPeer(t, memberA, coordSelf.ID).LastSeen
	time.Sleep(5 * time.Millisecond)

	coord.tick(context.Background())

	if got := rosterPeer(t, coord, aSelf.ID).LastSeen; !got.After(beforeA) {
		t.Fatalf("coordinator did not ping %q", aSelf.ID)
	}
	if got := rosterPeer(t, coord, bSelf.ID).LastSeen; !got.After(beforeB) {
		t.Fatalf("coordinator did not ping %q", bSelf.ID)
	}
	// The members answered, so they refreshed the coordinator's liveness.
	if got := rosterPeer(t, memberA, coordSelf.ID).LastSeen; !got.After(beforeCoordFromA) {
		t.Fatalf("member %q did not hear from the coordinator", aSelf.ID)
	}
}

// TestOnlySuccessorPingsCoordinator checks the other half of the rule: exactly
// one peer (the successor) probes the coordinator, which keeps the traffic
// linear in the room size.
func TestOnlySuccessorPingsCoordinator(t *testing.T) {
	net, coord, coordSelf := newTestRoom(t, "room-1")
	successor, successorSelf, _ := joinTestRoom(t, net, "room-1")
	other, otherSelf, _ := joinTestRoom(t, net, "room-1")
	waitForRosterSize(t, coord, 3)
	waitForRosterSize(t, successor, 3)
	waitForRosterSize(t, other, 3)

	if next, ok := successor.successor(); !ok || next.ID != successorSelf.ID {
		t.Fatalf("successor() = %+v, %v; want %q", next, ok, successorSelf.ID)
	}
	if next, ok := other.successor(); !ok || next.ID != successorSelf.ID {
		t.Fatalf("other peer successor() = %+v, %v; want %q", next, ok, successorSelf.ID)
	}

	beforeSuccessor := rosterPeer(t, coord, successorSelf.ID).LastSeen
	beforeOther := rosterPeer(t, coord, otherSelf.ID).LastSeen
	time.Sleep(5 * time.Millisecond)

	// A peer that is not the successor stays quiet.
	other.tick(context.Background())
	if got := rosterPeer(t, coord, otherSelf.ID).LastSeen; !got.Equal(beforeOther) {
		t.Fatalf("non-successor %q pinged the coordinator (%v -> %v)", otherSelf.ID, beforeOther, got)
	}

	// The successor probes the coordinator.
	successor.tick(context.Background())
	if got := rosterPeer(t, coord, successorSelf.ID).LastSeen; !got.After(beforeSuccessor) {
		t.Fatalf("successor %q did not ping the coordinator", successorSelf.ID)
	}
	if got := rosterPeer(t, successor, coordSelf.ID).LastSeen; got.IsZero() {
		t.Fatal("successor has no liveness for the coordinator")
	}
}
