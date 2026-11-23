package room

import (
	"context"
	"testing"
	"time"

	"github.com/huykhanh2141/croc-p2p/pkg/protocol"
)

// waitForCoordinator polls until m reports want as coordinator. Election and
// roster announcements arrive on the receive-loop goroutine, so tests must wait
// for them.
func waitForCoordinator(t *testing.T, m *Manager, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if coordinatorID(m) == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("coordinator = %q; want %q (announcement was not applied in time)", coordinatorID(m), want)
}

// waitForTerm polls until m has adopted want as its term.
func waitForTerm(t *testing.T, m *Manager, want uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if currentTerm(m) == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("term = %d; want %d (announcement was not applied in time)", currentTerm(m), want)
}

// TestSuccessorTakesOverAfterCoordinatorTimeout covers the liveness rule: when
// the coordinator stays silent past peerTimeout, the successor in the priority
// order takes over, bumps the term and announces it.
func TestSuccessorTakesOverAfterCoordinatorTimeout(t *testing.T) {
	base := time.Now().UTC().Add(-time.Hour)
	sweepAt := base.Add(50 * time.Minute)

	net, _, coordSelf := newTestRoom(t, "room-1")
	m, self, _ := joinTestRoom(t, net, "room-1")

	// Pretend the real coordinator is gone so this peer is the successor.
	markOffline(t, m, coordSelf.ID)
	addRosterPeer(t, m, "peer-early", base, base) // coordinator, silent at sweepAt
	setCoordinator(t, m, "peer-early")

	elections := collectElections(t, m)
	baseTerm := currentTerm(m)

	m.sweep(sweepAt)

	if got := coordinatorID(m); got != self.ID {
		t.Fatalf("coordinator after timeout = %q; want the successor %q", got, self.ID)
	}
	if got := currentTerm(m); got != baseTerm+1 {
		t.Fatalf("term after takeover = %d; want %d", got, baseTerm+1)
	}
	if !rosterPeer(t, m, self.ID).Coordinator {
		t.Fatal("successor is not flagged as coordinator")
	}
	if rosterPeer(t, m, "peer-early").Coordinator {
		t.Fatal("silent coordinator is still flagged as coordinator")
	}
	if got := rosterPeer(t, m, "peer-early").Status; got != protocol.PeerOffline {
		t.Fatalf("silent coordinator status = %q; want %q", got, protocol.PeerOffline)
	}

	info := nextElection(t, elections)
	if info.CoordinatorID != self.ID || info.Term != baseTerm+1 {
		t.Fatalf("ELECT_COORDINATOR = %+v; want coordinator %q term %d", info, self.ID, baseTerm+1)
	}
	if info.Reason != "coordinator timeout" {
		t.Fatalf("reason = %q; want %q", info.Reason, "coordinator timeout")
	}
}

// TestNonSuccessorLeavesTheTakeoverAlone checks that only the successor takes
// over: a peer further down the list waits for the announcement instead of
// electing itself (which would split the room).
func TestNonSuccessorLeavesTheTakeoverAlone(t *testing.T) {
	base := time.Now().UTC().Add(-time.Hour)
	sweepAt := base.Add(50 * time.Minute)

	net, _, coordSelf := newTestRoom(t, "room-1")
	m, self, _ := joinTestRoom(t, net, "room-1")
	markOffline(t, m, coordSelf.ID)
	addRosterPeer(t, m, "peer-early", base, base)                   // coordinator, times out
	addRosterPeer(t, m, "peer-mid", base.Add(time.Minute), sweepAt) // next in line
	setCoordinator(t, m, "peer-early")

	baseTerm := currentTerm(m)
	m.sweep(sweepAt)

	if got := coordinatorID(m); got == self.ID {
		t.Fatal("a peer that is not the successor took the coordinator role")
	}
	if next, ok := m.successor(); !ok || next.ID != "peer-mid" {
		t.Fatalf("successor() = %+v, %v; want peer-mid", next, ok)
	}
	if got := currentTerm(m); got != baseTerm {
		t.Fatalf("term = %d; want %d (no takeover happened)", got, baseTerm)
	}
}

// TestSuccessorTieBreaksOnPeerID checks that peers with the same arrival stamp
// are ordered by peer ID, so every machine picks the same successor.
func TestSuccessorTieBreaksOnPeerID(t *testing.T) {
	base := time.Now().UTC().Add(-time.Hour)
	sameJoin := base.Add(time.Minute)

	net, _, coordSelf := newTestRoom(t, "room-1")
	m, _, _ := joinTestRoom(t, net, "room-1")
	markOffline(t, m, coordSelf.ID)
	addRosterPeer(t, m, "peer-left", base, base) // coordinator
	addRosterPeer(t, m, "peer-b", sameJoin, base.Add(time.Hour))
	addRosterPeer(t, m, "peer-a", sameJoin, base.Add(time.Hour))
	setCoordinator(t, m, "peer-left")

	next, ok := m.successor()
	if !ok || next.ID != "peer-a" {
		t.Fatalf("successor() = %+v, %v; want peer-a (lower peer ID)", next, ok)
	}
}

// TestElectionChainAcrossManagers is the multi-peer chain: the coordinator
// dies, the successor takes over and the room converges; when that one dies
// too, the next peer in line takes over.
func TestElectionChainAcrossManagers(t *testing.T) {
	net, coordA, _ := newTestRoom(t, "room-1")
	peerB, bSelf, _ := joinTestRoom(t, net, "room-1")
	peerC, cSelf, _ := joinTestRoom(t, net, "room-1")
	waitForRosterSize(t, coordA, 3)
	waitForRosterSize(t, peerB, 3)
	waitForRosterSize(t, peerC, 3)
	termBefore := currentTerm(coordA)

	// The coordinator stops answering.
	if err := coordA.Close(); err != nil {
		t.Fatalf("close coordinator: %v", err)
	}

	// The successor notices on its next tick and announces the new term.
	peerB.tick(context.Background())
	if got := coordinatorID(peerB); got != bSelf.ID {
		t.Fatalf("coordinator after the first death = %q; want %q", got, bSelf.ID)
	}
	if got := currentTerm(peerB); got != termBefore+1 {
		t.Fatalf("term after the first takeover = %d; want %d", got, termBefore+1)
	}
	// The third peer follows the announcement instead of electing itself.
	waitForCoordinator(t, peerC, bSelf.ID)
	waitForTerm(t, peerC, termBefore+1)

	// If the new coordinator dies as well, the next peer takes over.
	if err := peerB.Close(); err != nil {
		t.Fatalf("close successor: %v", err)
	}
	peerC.tick(context.Background())
	if got := coordinatorID(peerC); got != cSelf.ID {
		t.Fatalf("coordinator after the second death = %q; want %q", got, cSelf.ID)
	}
	if got := currentTerm(peerC); got != termBefore+2 {
		t.Fatalf("term after the second takeover = %d; want %d", got, termBefore+2)
	}
}

// TestCoordinatorLeaveHandsOff covers the graceful path: the leaving
// coordinator names its successor with a new term, so nobody waits for a
// timeout.
func TestCoordinatorLeaveHandsOff(t *testing.T) {
	net, coordA, _ := newTestRoom(t, "room-1")
	peerB, bSelf, _ := joinTestRoom(t, net, "room-1")
	peerC, _, _ := joinTestRoom(t, net, "room-1")
	waitForRosterSize(t, coordA, 3)
	waitForRosterSize(t, peerB, 3)
	waitForRosterSize(t, peerC, 3)
	termBefore := currentTerm(coordA)

	if err := coordA.LeaveRoom(context.Background()); err != nil {
		t.Fatalf("LeaveRoom: %v", err)
	}

	// The leaver forgets the room entirely.
	if got := roomIDOf(coordA); got != "" {
		t.Fatalf("roomID after leave = %q; want empty", got)
	}
	if got := coordinatorID(coordA); got != "" {
		t.Fatalf("coordinator after leave = %q; want empty", got)
	}
	if coord, ok := coordA.Coordinator(); ok {
		t.Fatalf("leaver still reports coordinator %q", coord.ID)
	}
	if got := len(coordA.Peers()); got != 0 {
		t.Fatalf("leaver roster size = %d; want 0", got)
	}

	// The successor takes the hand-off and the third peer follows.
	waitForCoordinator(t, peerB, bSelf.ID)
	waitForTerm(t, peerB, termBefore+1)
	waitForCoordinator(t, peerC, bSelf.ID)
	waitForTerm(t, peerC, termBefore+1)
	waitForRosterSize(t, peerB, 2)
	waitForRosterSize(t, peerC, 2)
}

// TestSplitBrainConvergesOnHigherTerm covers a coordinator that is only slow:
// the successor already took over with a higher term, so the old coordinator
// steps down instead of fighting for the role.
func TestSplitBrainConvergesOnHigherTerm(t *testing.T) {
	net, coordA, _ := newTestRoom(t, "room-1")
	peerB, bSelf, _ := joinTestRoom(t, net, "room-1")
	peerC, _, _ := joinTestRoom(t, net, "room-1")
	waitForRosterSize(t, coordA, 3)
	waitForRosterSize(t, peerB, 3)
	waitForRosterSize(t, peerC, 3)
	termBefore := currentTerm(coordA)

	// The successor timed the coordinator out while it was merely slow.
	peerB.coordinatorSilent("split brain: coordinator slow")
	if got := coordinatorID(peerB); got != bSelf.ID {
		t.Fatalf("successor coordinator = %q; want %q", got, bSelf.ID)
	}
	if got := currentTerm(peerB); got != termBefore+1 {
		t.Fatalf("successor term = %d; want %d", got, termBefore+1)
	}

	// The old coordinator sees the higher term and steps down.
	waitForCoordinator(t, coordA, bSelf.ID)
	waitForTerm(t, coordA, termBefore+1)
	waitForCoordinator(t, peerC, bSelf.ID)
	waitForTerm(t, peerC, termBefore+1)

	for name, m := range map[string]*Manager{"old-coordinator": coordA, "successor": peerB, "third": peerC} {
		flagged := 0
		for _, peer := range m.Peers() {
			if peer.Coordinator {
				flagged++
			}
		}
		if flagged != 1 {
			t.Fatalf("%s has %d peers flagged as coordinator; want exactly 1", name, flagged)
		}
		if coord, ok := m.Coordinator(); !ok || coord.ID != bSelf.ID {
			t.Fatalf("%s coordinator = %+v, %v; want %q", name, coord, ok, bSelf.ID)
		}
	}
}

// TestStaleElectionIsIgnored checks that an old announcement cannot roll the
// term back.
func TestStaleElectionIsIgnored(t *testing.T) {
	net, coordA, aSelf := newTestRoom(t, "room-1")
	peerB, bSelf, _ := joinTestRoom(t, net, "room-1")
	termBefore := currentTerm(peerB)

	stale := protocol.NewControlMessage(protocol.MsgElectCoordinator, "room-1", aSelf.ID)
	stale.Election = &protocol.ElectionInfo{CoordinatorID: bSelf.ID, Term: termBefore - 1, Reason: "stale"}
	peerB.handleElection(*stale)

	if got := coordinatorID(peerB); got != aSelf.ID {
		t.Fatalf("coordinator after a stale election = %q; want %q", got, aSelf.ID)
	}
	if got := currentTerm(peerB); got != termBefore {
		t.Fatalf("term after a stale election = %d; want %d", got, termBefore)
	}
	_ = coordA
}

// TestElectionWhenRoomEmpties checks the end of a room: the last member leaves,
// nothing panics and no coordinator is left behind.
func TestElectionWhenRoomEmpties(t *testing.T) {
	m, ctx, stop := newTestManager(t)
	self, err := m.CreateRoom(ctx, "room-1", "secret")
	if err != nil {
		t.Fatalf("CreateRoom: %v", err)
	}
	stop()

	// The only other member is already offline.
	addRosterPeer(t, m, "peer-gone", self.JoinedAt.Add(time.Second), self.JoinedAt.Add(time.Hour))
	markOffline(t, m, "peer-gone")

	if err := m.LeaveRoom(context.Background()); err != nil {
		t.Fatalf("LeaveRoom: %v", err)
	}

	if got := coordinatorID(m); got != "" {
		t.Fatalf("coordinator with no survivors = %q; want empty", got)
	}
	if coord, ok := m.Coordinator(); ok {
		t.Fatalf("Coordinator() = %+v, true with no survivors", coord)
	}
	if got := len(m.Peers()); got != 0 {
		t.Fatalf("roster size = %d; want 0", got)
	}
}
