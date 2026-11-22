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
	cancel            context.CancelFunc
	wg                sync.WaitGroup
}

// New returns a room manager targeting relayAddr (empty uses DefaultRelay).
func New(relayAddr string) *Manager {
	if relayAddr == "" {
		relayAddr = protocol.DefaultRelay
	}
	return &Manager{
		relayAddr:         relayAddr,
		peers:             make(map[string]protocol.PeerInfo),
		subscribers:       make(map[uint64]func(protocol.ControlMessage)),
		heartbeatInterval: protocol.DefaultHeartbeatInterval,
		peerTimeout:       protocol.DefaultPeerTimeout,
	}
}

// CreateRoom starts a room and makes this peer the first coordinator.
func (m *Manager) CreateRoom(ctx context.Context, roomID, secret string) (protocol.PeerInfo, error) {
	if roomID == "" {
		return protocol.PeerInfo{}, fmt.Errorf("room: room id is required: %w", protocol.ErrInvalidMessage)
	}
	if err := ctx.Err(); err != nil {
		return protocol.PeerInfo{}, err
	}

	self := newSelfPeer(true)

	m.mu.Lock()
	m.roomID = roomID
	m.self = self
	m.coordinatorID = self.ID
	m.term++
	m.peers = map[string]protocol.PeerInfo{self.ID: self}
	m.mu.Unlock()

	// TODO(member-3): connect to m.relayAddr and authenticate the room with
	// the PAKE-derived key. Until then membership is process-local.
	_ = secret

	m.startHeartbeat(ctx)
	return self, nil
}

// JoinRoom adds this peer to an existing room. The first member in a room
// becomes the coordinator; otherwise the coordinator is discovered via the
// relay PEER_LIST response.
func (m *Manager) JoinRoom(ctx context.Context, roomID, secret string) (protocol.PeerInfo, []protocol.PeerInfo, error) {
	if roomID == "" {
		return protocol.PeerInfo{}, nil, fmt.Errorf("room: room id is required: %w", protocol.ErrInvalidMessage)
	}
	if err := ctx.Err(); err != nil {
		return protocol.PeerInfo{}, nil, err
	}

	self := newSelfPeer(false)

	m.mu.Lock()
	if m.roomID != "" && m.roomID != roomID {
		m.mu.Unlock()
		return protocol.PeerInfo{}, nil, fmt.Errorf("room: already in room %q: %w", m.roomID, protocol.ErrInvalidMessage)
	}
	if m.peers == nil {
		m.peers = make(map[string]protocol.PeerInfo)
	}
	m.roomID = roomID
	if m.coordinatorID == "" {
		self.Coordinator = true
		m.coordinatorID = self.ID
		m.term++
	}
	m.self = self
	m.peers[self.ID] = self
	peers := snapshotPeers(m.peers)
	m.mu.Unlock()

	// TODO(member-3): run the PAKE JOIN handshake through the relay before
	// accepting the member list from the coordinator.
	_ = secret

	m.startHeartbeat(ctx)
	return self, peers, nil
}

// LeaveRoom removes this peer and triggers an election if it was coordinator.
func (m *Manager) LeaveRoom(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	if m.roomID == "" {
		m.mu.Unlock()
		return nil
	}
	roomID := m.roomID
	self := m.self
	wasCoordinator := m.coordinatorID == self.ID
	delete(m.peers, self.ID)
	m.self = protocol.PeerInfo{}
	m.roomID = ""
	m.mu.Unlock()

	leave := protocol.NewControlMessage(protocol.MsgLeave, roomID, self.ID)
	m.publish(*leave)

	if wasCoordinator {
		m.electCoordinator()
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
func (m *Manager) Heartbeat(ctx context.Context, peerID string) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	m.mu.Lock()
	peer, ok := m.peers[peerID]
	if !ok {
		m.mu.Unlock()
		return 0, fmt.Errorf("room: heartbeat %q: %w", peerID, protocol.ErrPeerOffline)
	}
	peer.LastSeen = time.Now().UTC()
	if peer.Status == protocol.PeerOffline {
		peer.Status = protocol.PeerOnline
	}
	m.peers[peerID] = peer
	m.mu.Unlock()

	return 0, nil
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

// Close stops the background heartbeat loop.
func (m *Manager) Close() error {
	m.mu.Lock()
	cancel := m.cancel
	m.cancel = nil
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	m.wg.Wait()
	return nil
}

func (m *Manager) startHeartbeat(ctx context.Context) {
	m.mu.Lock()
	if m.cancel != nil {
		m.mu.Unlock()
		return
	}
	loopCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.mu.Unlock()

	m.wg.Add(1)
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
		case now := <-ticker.C:
			m.sweep(now)
		}
	}
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
		m.electCoordinator()
	}
}

// electCoordinator rotates the coordinator to the lowest online peer ID.
func (m *Manager) electCoordinator() {
	m.mu.Lock()
	if len(m.peers) == 0 {
		m.mu.Unlock()
		return
	}
	if cur, ok := m.peers[m.coordinatorID]; ok && cur.Status != protocol.PeerOffline {
		m.mu.Unlock()
		return
	}

	online := make([]string, 0, len(m.peers))
	for id, peer := range m.peers {
		if peer.Status != protocol.PeerOffline {
			online = append(online, id)
		}
	}
	if len(online) == 0 {
		m.mu.Unlock()
		return
	}
	sort.Strings(online)
	newID := online[0]

	m.coordinatorID = newID
	m.term++
	term := m.term

	if peer, ok := m.peers[newID]; ok {
		peer.Coordinator = true
		m.peers[newID] = peer
		if newID == m.self.ID {
			m.self = peer
		}
	}
	for id, peer := range m.peers {
		if id != newID && peer.Coordinator {
			peer.Coordinator = false
			m.peers[id] = peer
		}
	}
	roomID := m.roomID
	selfID := m.self.ID
	m.mu.Unlock()

	msg := protocol.NewControlMessage(protocol.MsgElectCoordinator, roomID, selfID)
	msg.Election = &protocol.ElectionInfo{
		CoordinatorID: newID,
		Term:          term,
		Reason:        "coordinator timeout",
	}
	// TODO(member-3): broadcast the election result through the relay so all
	// peers converge on the same term.
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

func snapshotPeers(peers map[string]protocol.PeerInfo) []protocol.PeerInfo {
	out := make([]protocol.PeerInfo, 0, len(peers))
	for _, peer := range peers {
		out = append(out, peer)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
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
