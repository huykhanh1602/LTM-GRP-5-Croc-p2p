// Package room implements the croc-p2p control plane: relay signaling, room
// membership, heartbeat liveness, coordinator rotation and file/bitfield
// fan-out.
//
// The relay wire protocol and PAKE-authenticated JOIN flow are owned by the
// Control Plane module owner (Member 3). This scaffold keeps an in-memory
// membership model so the rest of the project can be wired and tested early.
package room

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/huykhanh2141/croc-p2p/pkg/protocol"
)

// Manager is the default protocol.RoomManager implementation.
//
// TODOs for Member 3:
//  1. Dial the croc relay (m.relayAddr) and implement the JOIN handshake.
//  2. Route ControlMessage envelopes to/from the relay.
//  3. Send real PING frames and compute RTT from PONG replies.
//  4. Persist/replicate the member list so a new coordinator can take over.
var _ protocol.RoomManager = (*Manager)(nil)

// Manager tracks the local peer, the member list and subscribers.
type Manager struct {
	relayAddr string
	// signaler carries the control messages. Manager never dials anything
	// directly, which keeps the roster/election logic testable.
	signaler Signaler

	mu            sync.RWMutex
	roomID        string
	self          protocol.PeerInfo
	peers         map[string]protocol.PeerInfo
	coordinatorID string
	term          uint64
	subscribers   map[uint64]func(protocol.ControlMessage)
	nextSub       uint64

	heartbeatInterval time.Duration
	peerTimeout       time.Duration
	joinTimeout       time.Duration
	retryAttempts     int
	retryBase         time.Duration
	retryMax          time.Duration
	pendingPings      map[pingKey]chan struct{}
	// lastJoinAt is the newest arrival stamp this peer handed out as
	// coordinator (see stampArrivalLocked).
	lastJoinAt time.Time
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// pingKey identifies one outstanding PING. The echoed timestamp works as a
// correlation ID without adding a request ID to the wire format.
type pingKey struct {
	peerID string
	sent   time.Time
}

// defaultJoinTimeout bounds how long a newcomer waits for PEER_LIST.
const defaultJoinTimeout = 5 * time.Second

// New returns a room manager targeting relayAddr (empty uses DefaultRelay).
//
// TODO(member-3): wire this to signaler_relay.go. Until the relay room naming
// and the PAKE-covered channel are agreed with Khanh/Hoang it keeps using a
// private in-memory network, so a Manager started this way cannot reach another
// process yet.
func New(relayAddr string) *Manager {
	if relayAddr == "" {
		relayAddr = protocol.DefaultRelay
	}
	m := NewWithSignaler(nil)
	m.relayAddr = relayAddr
	return m
}

// NewWithSignaler returns a room manager whose control messages travel through
// sig. A nil sig falls back to a private in-memory network.
func NewWithSignaler(sig Signaler) *Manager {
	if sig == nil {
		sig = NewLocalNetwork().Signaler()
	}
	return &Manager{
		relayAddr:         protocol.DefaultRelay,
		signaler:          sig,
		peers:             make(map[string]protocol.PeerInfo),
		subscribers:       make(map[uint64]func(protocol.ControlMessage)),
		heartbeatInterval: protocol.DefaultHeartbeatInterval,
		peerTimeout:       protocol.DefaultPeerTimeout,
		joinTimeout:       defaultJoinTimeout,
		retryAttempts:     DefaultJoinAttempts,
		retryBase:         DefaultRetryBase,
		retryMax:          DefaultRetryMax,
		pendingPings:      make(map[pingKey]chan struct{}),
	}
}

// CreateRoom starts a room and makes this peer the first coordinator.
//
// The coordinator keeps the waiting room open for the whole session so
// newcomers can find it (spec #1).
func (m *Manager) CreateRoom(ctx context.Context, roomID, secret string) (protocol.PeerInfo, error) {
	if roomID == "" {
		return protocol.PeerInfo{}, fmt.Errorf("room: room id is required: %w", protocol.ErrInvalidMessage)
	}
	if err := ctx.Err(); err != nil {
		return protocol.PeerInfo{}, err
	}

	self := newSelfPeer(true)
	lobby := lobbyRoomID(roomID)
	if err := joinWithRetry(ctx, m.signaler, lobby, self, m.retryAttempts, m.retryBase, m.retryMax); err != nil {
		return protocol.PeerInfo{}, fmt.Errorf("room: open waiting room %q: %w", lobby, err)
	}

	m.mu.Lock()
	m.roomID = roomID
	m.self = self
	m.coordinatorID = self.ID
	m.term++
	m.peers = map[string]protocol.PeerInfo{self.ID: self}
	m.lastJoinAt = self.JoinedAt
	m.mu.Unlock()

	// TODO(member-3): authenticate the room with the PAKE-derived key before
	// accepting any control message (Hoang, internal/security).
	_ = secret

	m.startLoops(ctx)
	return self, nil
}

// JoinRoom adds this peer to an existing room.
//
// Handshake (spec #1/#2):
//  1. join the coordinator's waiting room, retrying while it is occupied;
//  2. send JOIN and wait for PEER_LIST, which carries the roster, the priority
//     order and the roster term;
//  3. leave the waiting room so the slot is free for the next newcomer.
//
// Joining never promotes the caller to coordinator: a peer only takes the role
// over through an election once the current coordinator is gone.
func (m *Manager) JoinRoom(ctx context.Context, roomID, secret string) (protocol.PeerInfo, []protocol.PeerInfo, error) {
	if roomID == "" {
		return protocol.PeerInfo{}, nil, fmt.Errorf("room: room id is required: %w", protocol.ErrInvalidMessage)
	}
	if err := ctx.Err(); err != nil {
		return protocol.PeerInfo{}, nil, err
	}

	m.mu.RLock()
	currentRoom, selfID := m.roomID, m.self.ID
	currentSelf, currentRoster := m.self, snapshotPeers(m.peers)
	m.mu.RUnlock()

	if currentRoom != "" && currentRoom != roomID {
		return protocol.PeerInfo{}, nil, fmt.Errorf("room: already in room %q: %w", currentRoom, protocol.ErrInvalidMessage)
	}
	if currentRoom == roomID && selfID != "" {
		// Already a member: a repeated join keeps the identity and the roster so
		// it cannot leave a second, permanent entry behind.
		return currentSelf, currentRoster, nil
	}

	self := newSelfPeer(false)
	// The handshake below reads the inbound stream itself, so stop any loops
	// left over from an earlier membership; they are restarted after the join.
	m.stopLoops()

	lobby := lobbyRoomID(roomID)
	if err := joinWithRetry(ctx, m.signaler, lobby, self, m.retryAttempts, m.retryBase, m.retryMax); err != nil {
		return protocol.PeerInfo{}, nil, fmt.Errorf("room: join waiting room %q: %w", lobby, err)
	}
	// Whatever happens next, the slot must be free for the next newcomer.
	defer func() { _ = m.signaler.Leave(context.Background(), lobby) }()

	join := protocol.NewControlMessage(protocol.MsgJoin, roomID, self.ID)
	join.Peer = &self
	if err := m.signaler.SendRoom(ctx, lobby, *join); err != nil {
		return protocol.PeerInfo{}, nil, fmt.Errorf("room: send JOIN: %w", err)
	}

	list, err := m.waitForPeerList(ctx, m.joinTimeout)
	if err != nil {
		return protocol.PeerInfo{}, nil, err
	}

	m.mu.Lock()
	m.roomID = roomID
	m.self = self
	m.applyRosterLocked(list)
	joined, roster := m.self, snapshotPeers(m.peers)
	m.mu.Unlock()

	// TODO(member-3): run the PAKE handshake (Hoang) before trusting the roster.
	_ = secret

	m.startLoops(ctx)
	return joined, roster, nil
}

// LeaveRoom removes this peer from the room.
//
// The departure is announced, and a leaving coordinator hands the role to its
// successor with a new term, so the room does not have to wait for a timeout.
// When the room empties there is no coordinator left.
func (m *Manager) LeaveRoom(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	if m.roomID == "" {
		m.mu.Unlock()
		return nil
	}
	roomID, self := m.roomID, m.self
	wasCoordinator := m.coordinatorID == self.ID
	next, hasNext := m.successorLocked()
	term := m.term

	others := make([]protocol.PeerInfo, 0, len(m.peers))
	for _, peer := range m.peers {
		if peer.ID != self.ID {
			others = append(others, peer)
		}
	}

	m.peers = make(map[string]protocol.PeerInfo)
	m.self = protocol.PeerInfo{}
	m.roomID = ""
	m.coordinatorID = ""
	m.mu.Unlock()

	leave := *protocol.NewControlMessage(protocol.MsgLeave, roomID, self.ID)
	var handoff protocol.ControlMessage
	if wasCoordinator && hasNext {
		handoff = *protocol.NewControlMessage(protocol.MsgElectCoordinator, roomID, self.ID)
		handoff.Election = &protocol.ElectionInfo{
			CoordinatorID: next.ID,
			Term:          term + 1,
			Reason:        "coordinator left",
		}
	}

	for _, peer := range others {
		if err := m.signaler.Send(context.Background(), peer.ID, leave); err != nil {
			// Already unreachable; its own timeout handles the rest.
			continue
		}
		if wasCoordinator && hasNext {
			_ = m.signaler.Send(context.Background(), peer.ID, handoff)
		}
	}

	m.publish(leave)
	if wasCoordinator && hasNext {
		m.publish(handoff)
	}

	// Free the waiting-room slot. Pair-room teardown belongs to the relay
	// signaler (TODO: agree the room lifecycle with Khanh).
	if err := m.signaler.Leave(context.Background(), lobbyRoomID(roomID)); err != nil {
		return fmt.Errorf("room: leave waiting room %q: %w", lobbyRoomID(roomID), err)
	}
	return nil
}

// Peers returns a sorted snapshot of the member list.
func (m *Manager) Peers() []protocol.PeerInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return snapshotPeers(m.peers)
}

// Coordinator returns the current coordinator, if any.
func (m *Manager) Coordinator() (protocol.PeerInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	peer, ok := m.peers[m.coordinatorID]
	return peer, ok && m.coordinatorID != ""
}

// AnnounceFile broadcasts a FILE_ANNOUNCE envelope.
func (m *Manager) AnnounceFile(ctx context.Context, meta protocol.FileMeta) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if meta.ID == "" {
		return fmt.Errorf("room: announce file with empty id: %w", protocol.ErrInvalidMessage)
	}

	m.mu.RLock()
	roomID, selfID := m.roomID, m.self.ID
	m.mu.RUnlock()

	msg := protocol.NewControlMessage(protocol.MsgFileAnnounce, roomID, selfID)
	msg.File = &meta
	// TODO(member-3): send over the relay and await coordinator acknowledgement.
	m.publish(*msg)
	return nil
}

// PublishBitfield broadcasts a BITFIELD_UPDATE envelope.
func (m *Manager) PublishBitfield(ctx context.Context, update protocol.BitfieldUpdate) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if update.FileID == "" {
		return fmt.Errorf("room: bitfield update with empty file id: %w", protocol.ErrInvalidMessage)
	}

	m.mu.RLock()
	roomID, selfID := m.roomID, m.self.ID
	m.mu.RUnlock()
	if update.PeerID == "" {
		update.PeerID = selfID
	}

	msg := protocol.NewControlMessage(protocol.MsgBitfieldUpdate, roomID, selfID)
	msg.Bitfield = &update
	m.publish(*msg)
	return nil
}

// Heartbeat refreshes liveness for peerID.
//
// TODO(member-3): send a PING over the relay and return the measured RTT when
// the matching PONG arrives. The scaffold updates LastSeen immediately.
// Heartbeat pings peerID and returns the measured round-trip time.
//
// The PING carries its send timestamp and the PONG echoes it, which correlates
// the reply with its request without adding a request ID to the wire format.
func (m *Manager) Heartbeat(ctx context.Context, peerID string) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	m.mu.RLock()
	roomID, selfID := m.roomID, m.self.ID
	_, known := m.peers[peerID]
	m.mu.RUnlock()
	if roomID == "" || !known {
		return 0, fmt.Errorf("room: heartbeat %q: %w", peerID, protocol.ErrPeerOffline)
	}

	start := time.Now()
	ping := protocol.NewControlMessage(protocol.MsgPing, roomID, selfID)
	ping.Timestamp = start.UTC()

	key := pingKey{peerID: peerID, sent: ping.Timestamp}
	wait := make(chan struct{}, 1)

	m.mu.Lock()
	if m.pendingPings == nil {
		m.pendingPings = make(map[pingKey]chan struct{})
	}
	m.pendingPings[key] = wait
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		delete(m.pendingPings, key)
		m.mu.Unlock()
	}()

	if err := m.signaler.Send(ctx, peerID, *ping); err != nil {
		return 0, fmt.Errorf("room: ping %q: %w", peerID, err)
	}

	timer := time.NewTimer(m.peerTimeout)
	defer timer.Stop()
	select {
	case <-wait:
		return time.Since(start), nil
	case <-timer.C:
		return 0, fmt.Errorf("room: heartbeat %q timed out: %w", peerID, protocol.ErrPeerOffline)
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// Subscribe registers a handler and returns an idempotent unsubscribe func.
func (m *Manager) Subscribe(handler func(protocol.ControlMessage)) func() {
	m.mu.Lock()
	if m.subscribers == nil {
		m.subscribers = make(map[uint64]func(protocol.ControlMessage))
	}
	m.nextSub++
	id := m.nextSub
	m.subscribers[id] = handler
	m.mu.Unlock()

	return func() {
		m.mu.Lock()
		delete(m.subscribers, id)
		m.mu.Unlock()
	}
}

// Close stops the background loops and releases the signaler.
func (m *Manager) Close() error {
	m.stopLoops()

	if m.signaler != nil {
		if err := m.signaler.Close(); err != nil {
			return fmt.Errorf("room: close signaler: %w", err)
		}
	}
	return nil
}

// stopLoops stops the receive and heartbeat loops. Calling it twice is safe.
func (m *Manager) stopLoops() {
	m.mu.Lock()
	cancel := m.cancel
	m.cancel = nil
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	m.wg.Wait()
}

// startLoops starts the receive loop and the heartbeat loop.
func (m *Manager) startLoops(ctx context.Context) {
	m.mu.Lock()
	if m.cancel != nil {
		m.mu.Unlock()
		return
	}
	loopCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.mu.Unlock()

	m.wg.Add(2)
	go func() {
		defer m.wg.Done()
		m.recvLoop(loopCtx)
	}()
	go func() {
		defer m.wg.Done()
		m.heartbeatLoop(loopCtx)
	}()
}

func (m *Manager) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(m.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.tick(ctx)
		}
	}
}

// tick runs one liveness round.
//
// Roles (spec #4):
//   - the coordinator pings every member, so a dead member is noticed and the
//     roster is re-broadcast;
//   - the successor (the next peer in priority order) pings the coordinator, so
//     a silent coordinator is noticed;
//   - everybody else stays quiet and follows PEER_LIST updates.
func (m *Manager) tick(ctx context.Context) {
	m.mu.RLock()
	roomID := m.roomID
	m.mu.RUnlock()
	if roomID == "" {
		return
	}

	m.sweep(time.Now())

	m.mu.RLock()
	selfID, coordID := m.self.ID, m.coordinatorID
	peers := snapshotPeers(m.peers)
	m.mu.RUnlock()
	if coordID == "" {
		return
	}

	if coordID == selfID {
		for _, peer := range peers {
			if peer.ID == selfID || peer.Status == protocol.PeerOffline {
				continue
			}
			if _, err := m.Heartbeat(ctx, peer.ID); err != nil {
				m.markOffline(peer.ID)
				m.broadcastRoster("peer timed out")
			}
		}
		return
	}

	next, ok := m.successor()
	if !ok || next.ID != selfID {
		return
	}
	if _, err := m.Heartbeat(ctx, coordID); err != nil {
		m.coordinatorSilent("coordinator silent")
	}
}

// successor returns the next coordinator in priority order: the earliest-joined
// online member after the current coordinator. Every peer computes the same
// answer from the same roster, which keeps hand-overs deterministic.
func (m *Manager) successor() (protocol.PeerInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.successorLocked()
}

// successorLocked is successor for callers that already hold m.mu.
func (m *Manager) successorLocked() (protocol.PeerInfo, bool) {
	coord, ok := m.peers[m.coordinatorID]
	if !ok {
		return protocol.PeerInfo{}, false
	}

	var best protocol.PeerInfo
	found := false
	for _, peer := range m.peers {
		if peer.ID == coord.ID || peer.Status == protocol.PeerOffline {
			continue
		}
		if !joinedBefore(coord, peer) {
			continue
		}
		if !found || joinedBefore(peer, best) {
			best, found = peer, true
		}
	}
	return best, found
}

// sweep marks silent peers offline and starts an election if needed.
func (m *Manager) sweep(now time.Time) {
	m.mu.Lock()
	roomID := m.roomID
	selfID := m.self.ID

	var stale []protocol.PeerInfo
	for id, peer := range m.peers {
		if id == selfID {
			continue
		}
		if now.Sub(peer.LastSeen) > m.peerTimeout && peer.Status != protocol.PeerOffline {
			peer.Status = protocol.PeerOffline
			m.peers[id] = peer
			stale = append(stale, peer)
		}
	}

	coordinatorDown := false
	if len(m.peers) > 1 {
		cur, ok := m.peers[m.coordinatorID]
		coordinatorDown = !ok || cur.Status == protocol.PeerOffline
	}
	m.mu.Unlock()

	for _, peer := range stale {
		msg := protocol.NewControlMessage(protocol.MsgPing, roomID, selfID)
		p := peer
		msg.Peer = &p
		msg.Error = "liveness timeout"
		m.publish(*msg)
	}

	if coordinatorDown {
		// Only the successor takes the role over; every other peer waits for
		// its announcement (spec #4).
		m.coordinatorSilent("coordinator timeout")
	}
}

// coordinatorSilent hands the role to this peer when it is the successor and
// the coordinator stopped answering. It bumps the term and announces the result
// to everybody so the room converges on one coordinator (spec #4).
//
// A peer that is not the successor does nothing: the peer in front of it is
// still alive and reacts to the same silence.
func (m *Manager) coordinatorSilent(reason string) {
	m.mu.Lock()
	if m.roomID == "" || m.self.ID == "" || m.coordinatorID == m.self.ID {
		m.mu.Unlock()
		return
	}
	next, ok := m.successorLocked()
	if !ok || next.ID != m.self.ID {
		m.mu.Unlock()
		return
	}

	oldCoordinator := m.coordinatorID
	if peer, ok := m.peers[oldCoordinator]; ok && peer.Status != protocol.PeerOffline {
		peer.Status = protocol.PeerOffline
		m.peers[oldCoordinator] = peer
	}

	m.coordinatorID = m.self.ID
	m.term++
	term := m.term
	self := m.self
	self.Coordinator = true
	m.self = self
	m.peers[self.ID] = self
	for id, peer := range m.peers {
		if id != self.ID && peer.Coordinator {
			peer.Coordinator = false
			m.peers[id] = peer
		}
	}
	roomID := m.roomID
	m.mu.Unlock()

	msg := protocol.NewControlMessage(protocol.MsgElectCoordinator, roomID, self.ID)
	msg.Election = &protocol.ElectionInfo{
		CoordinatorID: self.ID,
		Term:          term,
		Reason:        reason,
	}
	m.broadcast(*msg)
	m.broadcastRoster("new coordinator")
	m.publish(*msg)
}

func (m *Manager) publish(msg protocol.ControlMessage) {
	m.mu.RLock()
	handlers := make([]func(protocol.ControlMessage), 0, len(m.subscribers))
	for _, handler := range m.subscribers {
		handlers = append(handlers, handler)
	}
	m.mu.RUnlock()

	for _, handler := range handlers {
		handler(msg)
	}
}

// joinedBefore reports whether a has coordinator priority over b.
//
// Priority is arrival order: the peer that entered the room first is the
// coordinator, and the earliest surviving member takes over when that peer
// leaves or times out. Arrival order comes from PeerInfo.JoinedAt; peers that
// publish the same timestamp (or none at all) are ordered by peer ID so every
// machine computes the same winner.
//
// NOTE: JoinedAt is currently stamped by each peer's own clock, and clocks can
// drift between machines (a peer with a fast clock looks "earlier" than it
// really is). Long term the coordinator should assign JoinedAt/join order when
// it accepts a JOIN and re-broadcast it in PEER_LIST; that is not implemented
// yet because it depends on the relay handshake still being designed with
// Khanh/Hoang.
func joinedBefore(a, b protocol.PeerInfo) bool {
	switch {
	case a.JoinedAt.IsZero() && b.JoinedAt.IsZero():
		return a.ID < b.ID
	case a.JoinedAt.IsZero():
		// Unknown join time counts as "joined last": a peer must not inherit
		// the coordinator role because its timestamp was lost on the wire.
		return false
	case b.JoinedAt.IsZero():
		return true
	case !a.JoinedAt.Equal(b.JoinedAt):
		return a.JoinedAt.Before(b.JoinedAt)
	default:
		return a.ID < b.ID
	}
}

// snapshotPeers returns the roster in coordinator priority order: earliest
// arrival first, ties broken by peer ID (see joinedBefore).
func snapshotPeers(peers map[string]protocol.PeerInfo) []protocol.PeerInfo {
	out := make([]protocol.PeerInfo, 0, len(peers))
	for _, peer := range peers {
		out = append(out, peer)
	}
	sort.Slice(out, func(i, j int) bool { return joinedBefore(out[i], out[j]) })
	return out
}

func newSelfPeer(coordinator bool) protocol.PeerInfo {
	now := time.Now().UTC()
	name, err := os.Hostname()
	if err != nil || name == "" {
		name = "unknown"
	}
	return protocol.PeerInfo{
		ID:          newPeerID(),
		Name:        name,
		Coordinator: coordinator,
		Status:      protocol.PeerOnline,
		JoinedAt:    now,
		LastSeen:    now,
	}
}

func newPeerID() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("peer-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}
