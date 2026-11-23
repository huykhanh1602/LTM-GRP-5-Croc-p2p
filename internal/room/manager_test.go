package room

import (
	"context"
	"errors"
	"fmt"
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

// newTestRoom starts a coordinator on a fresh in-memory network. Its heartbeat
// interval is pushed out of the way so tests drive sweeps with a fake clock.
func newTestRoom(t *testing.T, roomID string) (*LocalNetwork, *Manager, protocol.PeerInfo) {
	t.Helper()
	net := NewLocalNetwork()
	coord := NewWithSignaler(net.Signaler())
	coord.heartbeatInterval = time.Hour
	t.Cleanup(func() { _ = coord.Close() })

	self, err := coord.CreateRoom(context.Background(), roomID, "secret")
	if err != nil {
		t.Fatalf("CreateRoom: %v", err)
	}
	return net, coord, self
}

// joinTestRoom runs the real waiting-room handshake for a fresh manager.
func joinTestRoom(t *testing.T, net *LocalNetwork, roomID string) (*Manager, protocol.PeerInfo, []protocol.PeerInfo) {
	t.Helper()
	m := NewWithSignaler(net.Signaler())
	m.heartbeatInterval = time.Hour
	t.Cleanup(func() { _ = m.Close() })

	self, roster, err := m.JoinRoom(context.Background(), roomID, "secret")
	if err != nil {
		t.Fatalf("JoinRoom: %v", err)
	}
	return m, self, roster
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

// markOffline flips a roster entry to PeerOffline, as a sweep would.
func markOffline(t *testing.T, m *Manager, id string) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	peer, ok := m.peers[id]
	if !ok {
		t.Fatalf("peer %q missing from roster", id)
	}
	peer.Status = protocol.PeerOffline
	m.peers[id] = peer
}

// waitForRosterSize polls until the roster has want members. Roster updates
// arrive on the receive-loop goroutine, so tests must wait for them.
func waitForRosterSize(t *testing.T, m *Manager, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(m.Peers()) == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("roster size = %d; want %d (PEER_LIST was not applied in time)", len(m.Peers()), want)
}

func roomIDOf(m *Manager) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.roomID
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

// TestJoinRoomDoesNotSelfPromote proves the Phase 1 bug stays fixed with the
// real handshake: the newcomer gets the coordinator from PEER_LIST and never
// promotes itself.
func TestJoinRoomDoesNotSelfPromote(t *testing.T) {
	net, coord, coordSelf := newTestRoom(t, "room-1")

	joiner, joined, roster := joinTestRoom(t, net, "room-1")

	if joined.ID == coordSelf.ID {
		t.Fatal("newcomer reused the coordinator identity")
	}
	if joined.Coordinator {
		t.Fatalf("peer %q joined after the creator but claims to be coordinator", joined.ID)
	}
	current, ok := joiner.Coordinator()
	if !ok || current.ID != coordSelf.ID {
		t.Fatalf("joiner coordinator = %+v, %v; want %q", current, ok, coordSelf.ID)
	}
	if got, want := currentTerm(joiner), currentTerm(coord); got != want {
		t.Fatalf("joiner term = %d; want the coordinator term %d", got, want)
	}
	// The priority order in the roster: coordinator first, newcomer second.
	if len(roster) != 2 {
		t.Fatalf("joiner roster = %+v; want two members", roster)
	}
	if roster[0].ID != coordSelf.ID || roster[1].ID != joined.ID {
		t.Fatalf("roster order = [%q %q]; want [%q %q]", roster[0].ID, roster[1].ID, coordSelf.ID, joined.ID)
	}
	if roster[1].JoinedAt.Before(roster[0].JoinedAt) {
		t.Fatal("newcomer was given a priority earlier than the coordinator")
	}

	// The coordinator learned about the newcomer too.
	peers := coord.Peers()
	if len(peers) != 2 || peers[0].ID != coordSelf.ID || peers[1].ID != joined.ID {
		t.Fatalf("coordinator roster = %+v; want [coordinator, newcomer]", peers)
	}
	if c, ok := coord.Coordinator(); !ok || c.ID != coordSelf.ID {
		t.Fatalf("coordinator changed: %+v, %v", c, ok)
	}
}

// TestJoinManyPeersOnlyFirstIsCoordinator joins several peers one after the
// other through the real handshake. None of them may claim the coordinator
// role, each one sees the growing roster from PEER_LIST, and the creator is
// never displaced.
func TestJoinManyPeersOnlyFirstIsCoordinator(t *testing.T) {
	net, coord, coordSelf := newTestRoom(t, "room-1")
	coordTerm := currentTerm(coord)

	for i := 1; i <= 3; i++ {
		t.Run(fmt.Sprintf("joiner-%d", i), func(t *testing.T) {
			joiner, joined, roster := joinTestRoom(t, net, "room-1")

			if joined.Coordinator {
				t.Fatalf("joiner %d claims to be coordinator", i)
			}
			if c, ok := joiner.Coordinator(); !ok || c.ID != coordSelf.ID {
				t.Fatalf("joiner %d coordinator = %+v, %v; want %q", i, c, ok, coordSelf.ID)
			}
			if got := currentTerm(joiner); got != coordTerm {
				t.Fatalf("joiner %d term = %d; want %d", i, got, coordTerm)
			}
			if len(roster) != i+1 {
				t.Fatalf("joiner %d roster = %d members; want %d", i, len(roster), i+1)
			}
			if roster[0].ID != coordSelf.ID || roster[len(roster)-1].ID != joined.ID {
				t.Fatalf("joiner %d roster order = first %q last %q; want %q..%q",
					i, roster[0].ID, roster[len(roster)-1].ID, coordSelf.ID, joined.ID)
			}
		})
	}

	if got := len(coord.Peers()); got != 4 {
		t.Fatalf("coordinator roster size = %d; want 4", got)
	}
	if c, ok := coord.Coordinator(); !ok || c.ID != coordSelf.ID {
		t.Fatalf("room creator is no longer coordinator: Coordinator() = %+v, %v", c, ok)
	}
	if got := currentTerm(coord); got != coordTerm {
		t.Fatalf("creator term changed from %d to %d after joins", coordTerm, got)
	}
}

// TestJoinRoomTwiceReturnsSameIdentity checks that a repeated JoinRoom for the
// same room is idempotent: same peer ID, same roster, no new term, and no
// duplicate entry that could later win an election.
func TestJoinRoomTwiceReturnsSameIdentity(t *testing.T) {
	net, _, coordSelf := newTestRoom(t, "room-1")
	m, self, roster := joinTestRoom(t, net, "room-1")
	termBefore := currentTerm(m)

	again, rosterAgain, err := m.JoinRoom(context.Background(), "room-1", "secret")
	if err != nil {
		t.Fatalf("second JoinRoom: %v", err)
	}

	if again.ID != self.ID {
		t.Fatalf("second join returned peer %q; want the existing identity %q", again.ID, self.ID)
	}
	if len(rosterAgain) != len(roster) {
		t.Fatalf("roster grew from %d to %d entries on a repeated join", len(roster), len(rosterAgain))
	}
	if len(rosterAgain) != 2 || rosterAgain[0].ID != coordSelf.ID || rosterAgain[1].ID != self.ID {
		t.Fatalf("roster after repeated join = %+v; want [coordinator, self]", rosterAgain)
	}
	if got := currentTerm(m); got != termBefore {
		t.Fatalf("repeated join changed term from %d to %d", termBefore, got)
	}
	if got := coordinatorID(m); got != coordSelf.ID {
		t.Fatalf("coordinator after repeated join = %q; want %q", got, coordSelf.ID)
	}
}

// TestLeaveThenRejoinSameRoom covers leaving a room and coming back later: the
// peer gets a fresh identity, stays a plain member, and the join does not open
// a new term.
func TestLeaveThenRejoinSameRoom(t *testing.T) {
	net, coord, coordSelf := newTestRoom(t, "room-1")
	m, first, _ := joinTestRoom(t, net, "room-1")
	termBefore := currentTerm(m)

	if err := m.LeaveRoom(context.Background()); err != nil {
		t.Fatalf("LeaveRoom: %v", err)
	}
	if got := roomIDOf(m); got != "" {
		t.Fatalf("roomID after leave = %q; want empty", got)
	}

	second, roster, err := m.JoinRoom(context.Background(), "room-1", "secret")
	if err != nil {
		t.Fatalf("re-join: %v", err)
	}

	if second.ID == first.ID {
		t.Fatal("re-join reused the identity of the peer that left")
	}
	if second.Coordinator {
		t.Fatal("peer re-joining the room claims to be coordinator")
	}
	if c, ok := m.Coordinator(); !ok || c.ID != coordSelf.ID {
		t.Fatalf("re-joining peer coordinator = %+v, %v; want %q", c, ok, coordSelf.ID)
	}
	if got := currentTerm(m); got != termBefore {
		t.Fatalf("term after re-join = %d; want %d (a join must not open a term)", got, termBefore)
	}
	if got := roomIDOf(m); got != "room-1" {
		t.Fatalf("roomID after re-join = %q; want %q", got, "room-1")
	}
	// The new identity must be in the roster, and it must not be the
	// coordinator. The roster may still contain the identity that left until
	// LEAVE is broadcast (that is the election/leave cluster).
	if len(roster) < 2 || roster[0].ID != coordSelf.ID {
		t.Fatalf("roster after re-join = %+v; want the coordinator first", roster)
	}
	found := false
	for _, peer := range roster {
		if peer.ID == second.ID {
			found = true
			if peer.Coordinator {
				t.Fatal("re-joined peer is flagged as coordinator in the roster")
			}
		}
	}
	if !found {
		t.Fatalf("re-joined peer %q missing from roster %+v", second.ID, roster)
	}
	if got := len(coord.Peers()); got < 2 {
		t.Fatalf("coordinator roster size = %d; want at least 2", got)
	}
}

// TestJoinRoomReceivesRosterWithAssignedOrder covers the JOIN/PEER_LIST
// handshake: the coordinator assigns the arrival order, the newcomer adopts it,
// and the waiting-room slot is free again for the next peer.
func TestJoinRoomReceivesRosterWithAssignedOrder(t *testing.T) {
	net, coord, coordSelf := newTestRoom(t, "room-1")

	first, firstSelf, firstRoster := joinTestRoom(t, net, "room-1")
	if len(firstRoster) != 2 {
		t.Fatalf("first newcomer roster = %+v; want coordinator + self", firstRoster)
	}
	if firstRoster[0].ID != coordSelf.ID || firstRoster[1].ID != firstSelf.ID {
		t.Fatalf("roster order = [%q %q]; want [%q %q]",
			firstRoster[0].ID, firstRoster[1].ID, coordSelf.ID, firstSelf.ID)
	}
	if !firstSelf.JoinedAt.Equal(firstRoster[1].JoinedAt) {
		t.Fatalf("newcomer JoinedAt = %v; roster says %v (the coordinator must assign it)",
			firstSelf.JoinedAt, firstRoster[1].JoinedAt)
	}
	if firstRoster[1].JoinedAt.Before(firstRoster[0].JoinedAt) {
		t.Fatal("newcomer was ordered before the coordinator")
	}

	// The first newcomer left the waiting room, so a second one can use it.
	second, secondSelf, secondRoster := joinTestRoom(t, net, "room-1")
	if len(secondRoster) != 3 {
		t.Fatalf("second newcomer roster = %+v; want three members", secondRoster)
	}
	if secondRoster[0].ID != coordSelf.ID || secondRoster[2].ID != secondSelf.ID {
		t.Fatalf("second newcomer order = %+v; want coordinator first and self last", secondRoster)
	}

	// Everyone converges on the same roster.
	waitForRosterSize(t, coord, 3)
	waitForRosterSize(t, first, 3)
	waitForRosterSize(t, second, 3)
	if got := coordinatorID(first); got != coordSelf.ID {
		t.Fatalf("first newcomer coordinator = %q; want %q", got, coordSelf.ID)
	}
}

// TestJoinRoomTimesOutWithoutCoordinator covers an empty waiting room: the
// JOIN is sent but nobody answers, so the join fails and the slot is freed.
func TestJoinRoomTimesOutWithoutCoordinator(t *testing.T) {
	ctx := context.Background()
	net := NewLocalNetwork()

	// Somebody sits in the waiting room but never answers JOIN.
	silent := net.Signaler()
	if err := silent.Join(ctx, lobbyRoomID("room-1"), testPeer("silent")); err != nil {
		t.Fatalf("silent join: %v", err)
	}

	m := NewWithSignaler(net.Signaler())
	m.heartbeatInterval = time.Hour
	m.joinTimeout = 100 * time.Millisecond
	m.retryBase, m.retryMax = time.Millisecond, 2*time.Millisecond
	t.Cleanup(func() { _ = m.Close() })

	if _, _, err := m.JoinRoom(ctx, "room-1", "secret"); !errors.Is(err, protocol.ErrPeerOffline) {
		t.Fatalf("JoinRoom error = %v; want it to wrap protocol.ErrPeerOffline", err)
	}
	if got := roomIDOf(m); got != "" {
		t.Fatalf("failed join left roomID = %q", got)
	}

	// The failed attempt must not leak the waiting-room slot.
	late := net.Signaler()
	if err := late.Join(ctx, lobbyRoomID("room-1"), testPeer("late")); err != nil {
		t.Fatalf("waiting room was not freed after the failed join: %v", err)
	}
}

// TestStalePeerListIsIgnored checks the term guard: an older roster cannot roll
// the membership back.
func TestStalePeerListIsIgnored(t *testing.T) {
	net, _, coordSelf := newTestRoom(t, "room-1")
	m, self, _ := joinTestRoom(t, net, "room-1")
	termBefore := currentTerm(m)

	stale := protocol.NewControlMessage(protocol.MsgPeerList, "room-1", coordSelf.ID)
	stale.Peers = []protocol.PeerInfo{coordSelf}
	stale.Election = &protocol.ElectionInfo{CoordinatorID: coordSelf.ID, Term: termBefore - 1, Reason: "stale"}
	m.handlePeerList(*stale)

	if got := len(m.Peers()); got != 2 {
		t.Fatalf("roster after the stale PEER_LIST = %d members; want 2", got)
	}
	if got := currentTerm(m); got != termBefore {
		t.Fatalf("term after the stale PEER_LIST = %d; want %d", got, termBefore)
	}
	if got := coordinatorID(m); got != coordSelf.ID {
		t.Fatalf("coordinator after the stale PEER_LIST = %q; want %q", got, coordSelf.ID)
	}

	// An update of the current term is still accepted. Without Seq two updates
	// of the same term cannot be ordered; that needs the types.go change.
	fresh := protocol.NewControlMessage(protocol.MsgPeerList, "room-1", coordSelf.ID)
	fresh.Peers = []protocol.PeerInfo{coordSelf, self}
	fresh.Election = &protocol.ElectionInfo{CoordinatorID: coordSelf.ID, Term: termBefore, Reason: "roster changed"}
	m.handlePeerList(*fresh)
	if got := len(m.Peers()); got != 2 {
		t.Fatalf("roster after a current-term PEER_LIST = %d members; want 2", got)
	}
}
