package room

import (
	"context"
	"testing"
	"time"

	"github.com/huykhanh2141/croc-p2p/pkg/protocol"
)

// newTestManager returns a Manager that is not in any room yet.
//
// The returned cancel func stops the background heartbeat goroutine: call it
// once the test has joined the room so that only the test's explicit sweep()
// calls (with a controlled clock) can change the roster. This keeps the
// election scenarios deterministic instead of racing a real 5s ticker.
func newTestManager(t *testing.T) (*Manager, context.Context, context.CancelFunc) {
	t.Helper()
	m := New("")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = m.Close()
	})
	return m, ctx, cancel
}

// addRosterPeer injects a member into the roster exactly as if it had arrived
// in a PEER_LIST message from the coordinator.
//
// TODO(member-3): replaced by the in-memory Signaler in Phase 3; until the
// relay handshake is agreed there is no real peer list to receive.
func addRosterPeer(t *testing.T, m *Manager, id string, joinedAt, lastSeen time.Time) protocol.PeerInfo {
	t.Helper()
	peer := protocol.PeerInfo{
		ID:       id,
		Name:     id,
		Status:   protocol.PeerOnline,
		JoinedAt: joinedAt,
		LastSeen: lastSeen,
	}
	m.mu.Lock()
	m.peers[id] = peer
	m.mu.Unlock()
	return peer
}

// setCoordinator marks id as the coordinator, the way a PEER_LIST/JOIN_ACK
// from the coordinator would.
func setCoordinator(t *testing.T, m *Manager, id string) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	for peerID, peer := range m.peers {
		peer.Coordinator = peerID == id
		m.peers[peerID] = peer
	}
	m.coordinatorID = id
}

func coordinatorID(m *Manager) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.coordinatorID
}

func currentTerm(m *Manager) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.term
}

func rosterPeer(t *testing.T, m *Manager, id string) protocol.PeerInfo {
	t.Helper()
	m.mu.RLock()
	defer m.mu.RUnlock()
	peer, ok := m.peers[id]
	if !ok {
		t.Fatalf("peer %q missing from roster", id)
	}
	return peer
}

// collectElections subscribes to the manager and forwards every
// ELECT_COORDINATOR message to the returned channel.
func collectElections(t *testing.T, m *Manager) <-chan protocol.ControlMessage {
	t.Helper()
	ch := make(chan protocol.ControlMessage, 8)
	unsubscribe := m.Subscribe(func(msg protocol.ControlMessage) {
		if msg.Type != protocol.MsgElectCoordinator {
			return
		}
		select {
		case ch <- msg:
		default:
		}
	})
	t.Cleanup(unsubscribe)
	return ch
}

func nextElection(t *testing.T, ch <-chan protocol.ControlMessage) protocol.ElectionInfo {
	t.Helper()
	select {
	case msg := <-ch:
		if msg.Election == nil {
			t.Fatalf("ELECT_COORDINATOR without election payload: %+v", msg)
		}
		return *msg.Election
	case <-time.After(2 * time.Second):
		t.Fatal("no ELECT_COORDINATOR message was published")
		return protocol.ElectionInfo{}
	}
}

// TestCreateRoomMakesFirstPeerCoordinator is the baseline: whoever creates the
// room is the first coordinator and opens term 1.
func TestCreateRoomMakesFirstPeerCoordinator(t *testing.T) {
	m, ctx, stop := newTestManager(t)

	self, err := m.CreateRoom(ctx, "room-1", "secret")
	if err != nil {
		t.Fatalf("CreateRoom: %v", err)
	}
	stop()

	if !self.Coordinator {
		t.Fatalf("creator %q is not flagged as coordinator", self.ID)
	}
	coord, ok := m.Coordinator()
	if !ok || coord.ID != self.ID {
		t.Fatalf("Coordinator() = %+v, %v; want creator %q", coord, ok, self.ID)
	}
	if got := currentTerm(m); got != 1 {
		t.Fatalf("term after CreateRoom = %d; want 1", got)
	}
}

// TestJoinRoomDoesNotSelfPromote proves the bug reported for Phase 1: a peer
// that joins an existing room used to set coordinatorID to itself whenever its
// local coordinatorID was empty, so every joiner believed it was coordinator.
func TestJoinRoomDoesNotSelfPromote(t *testing.T) {
	owner, ownerCtx, stopOwner := newTestManager(t)
	ownerSelf, err := owner.CreateRoom(ownerCtx, "room-1", "secret")
	if err != nil {
		t.Fatalf("CreateRoom: %v", err)
	}
	stopOwner()

	joiner, joinerCtx, stopJoiner := newTestManager(t)
	joined, peers, err := joiner.JoinRoom(joinerCtx, "room-1", "secret")
	if err != nil {
		t.Fatalf("JoinRoom: %v", err)
	}
	stopJoiner()

	if joined.Coordinator {
		t.Fatalf("peer %q joined after the creator but claims to be coordinator; only the earliest member may hold that role", joined.ID)
	}
	if coord, ok := joiner.Coordinator(); ok {
		t.Fatalf("joiner reports %q as coordinator although it never received a peer list", coord.ID)
	}
	if got := currentTerm(joiner); got != 0 {
		t.Fatalf("joiner started term %d on join; a join must not open a new term", got)
	}
	// Locally the joiner only knows itself. Receiving the real roster from the
	// coordinator is Phase 3 work (Signaler).
	if len(peers) != 1 || peers[0].ID != joined.ID {
		t.Fatalf("joiner roster = %+v; want only itself", peers)
	}
	if coord, ok := owner.Coordinator(); !ok || coord.ID != ownerSelf.ID {
		t.Fatalf("existing coordinator changed: Coordinator() = %+v, %v", coord, ok)
	}
}

// TestElectionAfterCoordinatorTimeout checks the liveness path: the earliest
// member (by JoinedAt) is the coordinator; when it stays silent past the peer
// timeout, the earliest *surviving* member must take over and bump the term.
func TestElectionAfterCoordinatorTimeout(t *testing.T) {
	base := time.Now().UTC().Add(-time.Hour)
	sweepAt := base.Add(50 * time.Minute)

	m, ctx, stop := newTestManager(t)
	if _, _, err := m.JoinRoom(ctx, "room-1", "secret"); err != nil {
		t.Fatalf("JoinRoom: %v", err)
	}
	stop()

	// Roster as the coordinator would have sent it: peer-early joined first and
	// is the coordinator, peer-next and peer-last joined later, this peer last.
	addRosterPeer(t, m, "peer-early", base, base) // stale at sweepAt
	addRosterPeer(t, m, "peer-next", base.Add(time.Minute), sweepAt)
	addRosterPeer(t, m, "peer-last", base.Add(2*time.Minute), sweepAt)
	setCoordinator(t, m, "peer-early")

	elections := collectElections(t, m)
	before := currentTerm(m)

	m.sweep(sweepAt)

	if got := coordinatorID(m); got != "peer-next" {
		t.Fatalf("coordinator after timeout = %q; want %q (earliest surviving member)", got, "peer-next")
	}
	if got := currentTerm(m); got != before+1 {
		t.Fatalf("term after election = %d; want %d", got, before+1)
	}
	if !rosterPeer(t, m, "peer-next").Coordinator {
		t.Fatal("roster entry for the new coordinator does not have Coordinator=true")
	}
	if rosterPeer(t, m, "peer-early").Coordinator {
		t.Fatal("timed out coordinator is still flagged as coordinator")
	}
	if got := rosterPeer(t, m, "peer-early").Status; got != protocol.PeerOffline {
		t.Fatalf("timed out coordinator status = %q; want %q", got, protocol.PeerOffline)
	}

	info := nextElection(t, elections)
	if info.CoordinatorID != "peer-next" {
		t.Fatalf("ELECT_COORDINATOR announced %q; want %q", info.CoordinatorID, "peer-next")
	}
	if info.Term != before+1 {
		t.Fatalf("ELECT_COORDINATOR term = %d; want %d", info.Term, before+1)
	}
}

// TestElectionAfterCoordinatorLeave checks the graceful path: when the
// coordinator leaves, the earliest surviving member takes over.
func TestElectionAfterCoordinatorLeave(t *testing.T) {
	m, ctx, stop := newTestManager(t)
	self, err := m.CreateRoom(ctx, "room-1", "secret")
	if err != nil {
		t.Fatalf("CreateRoom: %v", err)
	}
	stop()

	// The creator joined first; the other two joined afterwards.
	addRosterPeer(t, m, "peer-next", self.JoinedAt.Add(time.Second), self.JoinedAt.Add(time.Hour))
	addRosterPeer(t, m, "peer-last", self.JoinedAt.Add(2*time.Second), self.JoinedAt.Add(time.Hour))

	elections := collectElections(t, m)
	before := currentTerm(m)

	if err := m.LeaveRoom(context.Background()); err != nil {
		t.Fatalf("LeaveRoom: %v", err)
	}

	if got := coordinatorID(m); got != "peer-next" {
		t.Fatalf("coordinator after leave = %q; want %q (earliest surviving member)", got, "peer-next")
	}
	if got := currentTerm(m); got != before+1 {
		t.Fatalf("term after leave = %d; want %d", got, before+1)
	}

	info := nextElection(t, elections)
	if info.CoordinatorID != "peer-next" {
		t.Fatalf("ELECT_COORDINATOR announced %q; want %q", info.CoordinatorID, "peer-next")
	}
}

// TestElectionTieBreaksOnID checks that equal JoinedAt values are resolved by
// peer ID so every machine computes the same winner without extra chatter.
func TestElectionTieBreaksOnID(t *testing.T) {
	base := time.Now().UTC().Add(-time.Hour)
	sweepAt := base.Add(50 * time.Minute)
	sameJoin := base.Add(time.Minute)

	m, ctx, stop := newTestManager(t)
	if _, _, err := m.JoinRoom(ctx, "room-1", "secret"); err != nil {
		t.Fatalf("JoinRoom: %v", err)
	}
	stop()

	addRosterPeer(t, m, "peer-left", base, base) // coordinator, times out
	addRosterPeer(t, m, "peer-b", sameJoin, sweepAt)
	addRosterPeer(t, m, "peer-a", sameJoin, sweepAt)
	setCoordinator(t, m, "peer-left")

	m.sweep(sweepAt)

	if got := coordinatorID(m); got != "peer-a" {
		t.Fatalf("coordinator with tied JoinedAt = %q; want %q (lower peer ID)", got, "peer-a")
	}
}
