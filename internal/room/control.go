package room

import (
	"context"
	"fmt"
	"time"

	"github.com/huykhanh2141/croc-p2p/pkg/protocol"
)

// lobbyRoomID is the relay room a coordinator keeps open so newcomers can find
// it. Pair rooms follow Khanh's rule roomID#idA-idB; the waiting room is the
// special "#lobby" channel.
//
// TODO(member-3): confirm the suffix and the "coordinator keeps one lobby open"
// rule with Khanh; signaler_relay.go will map this name onto a real relay room.
func lobbyRoomID(roomID string) string {
	return roomID + "#lobby"
}

// recvLoop feeds inbound control messages to the handlers until the manager is
// closed.
func (m *Manager) recvLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-m.signaler.Recv():
			if !ok {
				return
			}
			m.handleMessage(msg)
		}
	}
}

// handleMessage routes one inbound envelope.
func (m *Manager) handleMessage(msg protocol.ControlMessage) {
	switch msg.Type {
	case protocol.MsgJoin:
		m.handleJoin(msg)
	case protocol.MsgPeerList:
		m.handlePeerList(msg)
	case protocol.MsgPing:
		m.handlePing(msg)
	case protocol.MsgPong:
		m.handlePong(msg)
	case protocol.MsgElectCoordinator:
		m.handleElection(msg)
	case protocol.MsgLeave:
		m.handleLeave(msg)
	}
}

// handleElection adopts a coordinator announced by another peer.
//
// A higher term wins and makes the previous coordinator step down, which is how
// a slow-but-alive coordinator and its successor converge on one leader (split
// brain). At the same term the coordinator we already know is kept so two peers
// cannot trade the role back and forth.
func (m *Manager) handleElection(msg protocol.ControlMessage) {
	if msg.Election == nil || msg.Election.CoordinatorID == "" {
		return
	}
	coordID, term := msg.Election.CoordinatorID, msg.Election.Term

	m.mu.Lock()
	if m.roomID == "" || msg.RoomID != m.roomID {
		m.mu.Unlock()
		return
	}
	stale := term < m.term || (term == m.term && coordID == m.coordinatorID)
	// Equal term but a different name: only step down if we do not own that
	// term ourselves.
	contested := term == m.term && m.coordinatorID == m.self.ID
	if stale || contested {
		m.mu.Unlock()
		return
	}

	for id, peer := range m.peers {
		peer.Coordinator = id == coordID
		m.peers[id] = peer
	}
	m.term = term
	m.coordinatorID = coordID
	if self, ok := m.peers[m.self.ID]; ok {
		m.self = self
	} else {
		self := m.self
		self.Coordinator = coordID == m.self.ID
		m.self = self
	}
	becameCoordinator := coordID == m.self.ID
	m.mu.Unlock()

	m.publish(msg)
	if becameCoordinator {
		// Announce the new role with the new term so every peer converges.
		m.broadcastRoster("new coordinator")
	}
}

// handleLeave removes a peer that announced it is going away. Only the
// coordinator has to fan the new roster out.
func (m *Manager) handleLeave(msg protocol.ControlMessage) {
	m.mu.Lock()
	if m.roomID == "" || msg.RoomID != m.roomID {
		m.mu.Unlock()
		return
	}
	if _, ok := m.peers[msg.SenderID]; !ok {
		m.mu.Unlock()
		return
	}
	delete(m.peers, msg.SenderID)
	isCoordinator := m.coordinatorID == m.self.ID
	m.mu.Unlock()

	if isCoordinator {
		m.broadcastRoster("peer left")
	}
}

// handlePing answers with a PONG that echoes the timestamp and refreshes the
// sender's liveness.
func (m *Manager) handlePing(msg protocol.ControlMessage) {
	m.mu.Lock()
	roomID, selfID := m.roomID, m.self.ID
	if roomID == "" || msg.RoomID != roomID {
		m.mu.Unlock()
		return
	}
	if peer, ok := m.peers[msg.SenderID]; ok {
		peer.LastSeen = time.Now().UTC()
		if peer.Status == protocol.PeerOffline {
			peer.Status = protocol.PeerOnline
		}
		m.peers[msg.SenderID] = peer
	}
	m.mu.Unlock()

	pong := protocol.NewControlMessage(protocol.MsgPong, roomID, selfID)
	pong.Timestamp = msg.Timestamp // echo so the pinger can match its PING
	if err := m.signaler.Send(context.Background(), msg.SenderID, *pong); err != nil {
		m.markOffline(msg.SenderID)
	}
}

// handlePong completes the matching Heartbeat call.
func (m *Manager) handlePong(msg protocol.ControlMessage) {
	key := pingKey{peerID: msg.SenderID, sent: msg.Timestamp}

	m.mu.Lock()
	wait, pending := m.pendingPings[key]
	if pending {
		delete(m.pendingPings, key)
	}
	if peer, ok := m.peers[msg.SenderID]; ok {
		peer.LastSeen = time.Now().UTC()
		if peer.Status == protocol.PeerOffline {
			peer.Status = protocol.PeerOnline
		}
		m.peers[msg.SenderID] = peer
	}
	m.mu.Unlock()

	if pending {
		select {
		case wait <- struct{}{}:
		default:
		}
	}
}

// handleJoin accepts a newcomer. Only the coordinator answers JOIN, so the
// roster has exactly one writer.
func (m *Manager) handleJoin(msg protocol.ControlMessage) {
	if msg.Peer == nil {
		return
	}
	now := time.Now().UTC()

	m.mu.Lock()
	if m.roomID == "" || msg.RoomID != m.roomID || m.coordinatorID != m.self.ID {
		// Not the coordinator (or not our room): ignore.
		m.mu.Unlock()
		return
	}
	peer := *msg.Peer
	peer.Status = protocol.PeerOnline
	peer.Coordinator = false
	peer.LastSeen = now
	// TEMPORARY priority assignment: the coordinator stamps the arrival time
	// with its own clock, so the order never depends on the newcomer's clock.
	// Once PeerInfo.JoinOrder exists this becomes a monotonic counter (the
	// types.go proposal is waiting for approval).
	peer.JoinedAt = m.stampArrivalLocked(now)
	m.peers[peer.ID] = peer
	lobby := lobbyRoomID(m.roomID)
	m.mu.Unlock()

	// Reply while the newcomer is still standing in the waiting room.
	if err := m.signaler.SendRoom(context.Background(), lobby, m.rosterMessage("join accepted")); err != nil {
		// The newcomer left before the reply; the heartbeat will drop it.
		m.markOffline(msg.Peer.ID)
	}
	// Fan the new roster out to everybody else; the newcomer just got it.
	m.broadcastRosterExcept("roster changed", msg.Peer.ID)
}

// stampArrivalLocked hands out a strictly increasing arrival stamp. The
// coordinator's clock has limited resolution, so two joins can land in the same
// tick; bumping by a nanosecond keeps the priority order total and identical on
// every peer.
//
// TEMPORARY: replaced by PeerInfo.JoinOrder (a monotonic counter) once the
// types.go change is approved. Caller must hold m.mu.
func (m *Manager) stampArrivalLocked(now time.Time) time.Time {
	if !now.After(m.lastJoinAt) {
		now = m.lastJoinAt.Add(time.Nanosecond)
	}
	m.lastJoinAt = now
	return now
}

// handlePeerList adopts the coordinator's roster.
func (m *Manager) handlePeerList(msg protocol.ControlMessage) {
	if msg.Election == nil {
		return
	}

	m.mu.Lock()
	// TEMPORARY: the term rides in Election.Term until ControlMessage.Term and
	// ControlMessage.Seq exist. Without Seq only whole-term regressions can be
	// rejected; a reordered update of the same term is still accepted.
	stale := m.roomID == "" || msg.RoomID != m.roomID || msg.Election.Term < m.term
	if !stale {
		m.applyRosterLocked(msg)
	}
	m.mu.Unlock()

	if stale {
		return
	}
	m.publish(msg)
}

// applyRosterLocked replaces the roster with the coordinator's view. The caller
// must hold m.mu.
func (m *Manager) applyRosterLocked(msg protocol.ControlMessage) {
	roster := make(map[string]protocol.PeerInfo, len(msg.Peers)+1)
	for _, peer := range msg.Peers {
		roster[peer.ID] = peer
	}
	if m.self.ID != "" {
		if self, ok := roster[m.self.ID]; ok {
			m.self = self
		} else {
			// The coordinator answered before recording us; keep our own entry
			// so we do not vanish from the roster.
			roster[m.self.ID] = m.self
		}
	}
	m.peers = roster
	m.term = msg.Election.Term
	m.coordinatorID = msg.Election.CoordinatorID
	if self, ok := m.peers[m.self.ID]; ok {
		m.self = self
	}
	// Keep the arrival-stamp generator ahead of the adopted roster so a future
	// newcomer cannot sort before an existing member.
	for _, peer := range m.peers {
		if peer.JoinedAt.After(m.lastJoinAt) {
			m.lastJoinAt = peer.JoinedAt
		}
	}
}

// rosterMessage builds a PEER_LIST with the roster in priority order.
//
// TEMPORARY: the roster term rides in Election.Term because ControlMessage has
// no Term/Seq field yet.
func (m *Manager) rosterMessage(reason string) protocol.ControlMessage {
	m.mu.RLock()
	roomID, selfID, coordID, term := m.roomID, m.self.ID, m.coordinatorID, m.term
	peers := snapshotPeers(m.peers)
	m.mu.RUnlock()

	msg := protocol.NewControlMessage(protocol.MsgPeerList, roomID, selfID)
	msg.Peers = peers
	msg.Election = &protocol.ElectionInfo{
		CoordinatorID: coordID,
		Term:          term,
		Reason:        reason,
	}
	// TODO(member-3): set ControlMessage.Seq here once it exists so a receiver
	// can reject an older roster of the same term.
	return *msg
}

// broadcastRoster pushes the current roster to every other member.
func (m *Manager) broadcastRoster(reason string) {
	m.broadcastRosterExcept(reason, "")
}

// broadcastRosterExcept is broadcastRoster without the peer named skip. The
// newcomer that just received the roster in the JOIN reply does not need a
// second copy of it.
func (m *Manager) broadcastRosterExcept(reason, skip string) {
	m.mu.RLock()
	roomID, selfID := m.roomID, m.self.ID
	peers := snapshotPeers(m.peers)
	m.mu.RUnlock()
	if roomID == "" || len(peers) < 2 {
		return
	}

	msg := m.rosterMessage(reason)
	for _, peer := range peers {
		if peer.ID == selfID || peer.ID == skip {
			continue
		}
		if err := m.signaler.Send(context.Background(), peer.ID, msg); err != nil {
			m.markOffline(peer.ID)
		}
	}
}

// broadcast sends msg to every other member.
func (m *Manager) broadcast(msg protocol.ControlMessage) {
	m.mu.RLock()
	selfID := m.self.ID
	peers := snapshotPeers(m.peers)
	m.mu.RUnlock()

	for _, peer := range peers {
		if peer.ID == selfID {
			continue
		}
		if err := m.signaler.Send(context.Background(), peer.ID, msg); err != nil {
			m.markOffline(peer.ID)
		}
	}
}

// markOffline flips a roster entry to offline.
func (m *Manager) markOffline(peerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	peer, ok := m.peers[peerID]
	if !ok || peer.Status == protocol.PeerOffline {
		return
	}
	peer.Status = protocol.PeerOffline
	m.peers[peerID] = peer
}

// waitForPeerList blocks until the coordinator answers the JOIN.
func (m *Manager) waitForPeerList(ctx context.Context, timeout time.Duration) (protocol.ControlMessage, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case msg, ok := <-m.signaler.Recv():
			if !ok {
				return protocol.ControlMessage{}, fmt.Errorf("room: signaler closed while waiting for PEER_LIST: %w", protocol.ErrPeerOffline)
			}
			switch msg.Type {
			case protocol.MsgPeerList:
				return msg, nil
			case protocol.MsgError:
				return protocol.ControlMessage{}, fmt.Errorf("room: coordinator rejected the join: %s: %w", msg.Error, protocol.ErrUnauthorized)
			}
		case <-timer.C:
			return protocol.ControlMessage{}, fmt.Errorf("room: no PEER_LIST from the coordinator: %w", protocol.ErrPeerOffline)
		case <-ctx.Done():
			return protocol.ControlMessage{}, ctx.Err()
		}
	}
}
