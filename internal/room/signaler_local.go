package room

import (
	"context"
	"fmt"
	"sync"

	"github.com/huykhanh2141/croc-p2p/pkg/protocol"
)

const (
	// localRoomCapacity mirrors the croc relay: two peers per signalling room.
	localRoomCapacity = 2
	// localInboxSize is generous so slow test receivers do not drop messages.
	localInboxSize = 64
)

// LocalNetwork is an in-process stand-in for the croc relay. It lets several
// Managers exchange control messages inside one process, which is what the
// tests use until signaler_relay.go exists.
//
// TODO(member-3): signaler_relay.go must reproduce the same behaviour over the
// real relay (two peers per room, lobby slot, pair rooms roomID#idA-idB). The
// room naming and the "who dials whom" rules have to be agreed with Khanh.
type LocalNetwork struct {
	mu        sync.Mutex
	rooms     map[string][]string       // room name -> member peer IDs (max 2)
	signalers map[string]*localSignaler // peer ID -> signaller
	reserved  int
}

// NewLocalNetwork creates an empty in-process signalling fabric.
func NewLocalNetwork() *LocalNetwork {
	return &LocalNetwork{
		rooms:     make(map[string][]string),
		signalers: make(map[string]*localSignaler),
	}
}

// Signaler returns a Signaler for one peer. The peer identity is attached on
// the first Join call.
func (n *LocalNetwork) Signaler() Signaler {
	return &localSignaler{
		net:   n,
		inbox: make(chan protocol.ControlMessage, localInboxSize),
		rooms: make(map[string]bool),
	}
}

// Reserve takes one slot in roomID as if a peer we do not control were sitting
// there. Tests use it to produce ErrRoomFull. The returned func frees the slot
// and must be called once.
func (n *LocalNetwork) Reserve(roomID string) func() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.reserved++
	id := fmt.Sprintf("reserved-%d", n.reserved)
	n.rooms[roomID] = append(n.rooms[roomID], id)

	return func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		n.removeMemberLocked(roomID, id)
	}
}

// removeMemberLocked drops id from roomID and deletes the room when it empties.
// Callers must hold n.mu.
func (n *LocalNetwork) removeMemberLocked(roomID, id string) {
	members := n.rooms[roomID]
	for i, member := range members {
		if member == id {
			members = append(members[:i], members[i+1:]...)
			break
		}
	}
	if len(members) == 0 {
		delete(n.rooms, roomID)
		return
	}
	n.rooms[roomID] = members
}

// localSignaler implements Signaler on top of a LocalNetwork.
//
// It is a test double, not a network stack: Send is plain unicast (the relay
// implementation will need to open a pair room), and a full inbox drops the
// message like a congested socket would.
type localSignaler struct {
	net   *LocalNetwork
	inbox chan protocol.ControlMessage

	mu     sync.Mutex // guards closed
	closed bool

	// The fields below are guarded by net.mu.
	selfID string
	rooms  map[string]bool
}

// Join takes the peer's slot in roomID.
func (s *localSignaler) Join(ctx context.Context, roomID string, self protocol.PeerInfo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if roomID == "" {
		return fmt.Errorf("room: local signaler: empty room id: %w", protocol.ErrInvalidMessage)
	}
	if self.ID == "" {
		return fmt.Errorf("room: local signaler: empty peer id: %w", protocol.ErrInvalidMessage)
	}

	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return fmt.Errorf("room: local signaler: closed: %w", protocol.ErrPeerOffline)
	}

	s.net.mu.Lock()
	defer s.net.mu.Unlock()

	members := s.net.rooms[roomID]
	for _, member := range members {
		if member == self.ID {
			// Joining twice is idempotent, like a reconnecting relay client.
			s.rooms[roomID] = true
			return nil
		}
	}
	if len(members) >= localRoomCapacity {
		return fmt.Errorf("room: waiting room %q full: %w", roomID, ErrRoomFull)
	}

	s.selfID = self.ID
	s.net.rooms[roomID] = append(members, self.ID)
	s.net.signalers[self.ID] = s
	s.rooms[roomID] = true
	return nil
}

// Leave frees the peer's slot in roomID.
func (s *localSignaler) Leave(ctx context.Context, roomID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.net.mu.Lock()
	defer s.net.mu.Unlock()
	if !s.rooms[roomID] {
		return nil
	}
	delete(s.rooms, roomID)
	s.net.removeMemberLocked(roomID, s.selfID)
	return nil
}

// SendRoom delivers msg to every other member of roomID.
func (s *localSignaler) SendRoom(ctx context.Context, roomID string, msg protocol.ControlMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.net.mu.Lock()
	members, joined := s.net.rooms[roomID]
	selfID := s.selfID
	var targets []*localSignaler
	for _, id := range members {
		if id == selfID {
			continue
		}
		if target, ok := s.net.signalers[id]; ok {
			targets = append(targets, target)
		}
	}
	s.net.mu.Unlock()

	if !joined {
		return fmt.Errorf("room: local signaler: not in room %q: %w", roomID, protocol.ErrPeerOffline)
	}
	if len(targets) == 0 {
		return fmt.Errorf("room: no peer in signalling room %q: %w", roomID, protocol.ErrPeerOffline)
	}
	for _, target := range targets {
		target.deliver(msg)
	}
	return nil
}

// Send delivers msg to one peer's inbox.
func (s *localSignaler) Send(ctx context.Context, to string, msg protocol.ControlMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.net.mu.Lock()
	target, ok := s.net.signalers[to]
	s.net.mu.Unlock()
	if !ok {
		return fmt.Errorf("room: local signaler: peer %q is not connected: %w", to, protocol.ErrPeerOffline)
	}

	target.deliver(msg)
	return nil
}

// Recv returns the inbound message stream.
func (s *localSignaler) Recv() <-chan protocol.ControlMessage {
	return s.inbox
}

// Close leaves every room and stops accepting messages. Calling it twice is
// safe.
func (s *localSignaler) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	s.net.mu.Lock()
	defer s.net.mu.Unlock()
	for roomID := range s.rooms {
		s.net.removeMemberLocked(roomID, s.selfID)
	}
	if current, ok := s.net.signalers[s.selfID]; ok && current == s {
		delete(s.net.signalers, s.selfID)
	}
	s.rooms = nil
	return nil
}

// deliver hands msg to the inbox without blocking.
func (s *localSignaler) deliver(msg protocol.ControlMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	select {
	case s.inbox <- msg:
	default:
		// Buffer full: drop, mirroring a congested relay socket.
	}
}
