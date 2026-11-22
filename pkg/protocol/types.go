// Package protocol defines the shared wire types, constants and provider
// interfaces used by every croc-p2p module. It is intentionally dependency
// light so that the security, transport, room and chunker packages can be
// implemented by different team members without creating import cycles.
package protocol

import (
	"context"
	"errors"
	"io"
	"net"
	"time"
)

// Version and protocol-wide defaults.
const (
	// ProtocolVersion is stamped on every ControlMessage so peers can reject
	// incompatible implementations early.
	ProtocolVersion = "0.1.0"

	// DefaultRelay is the public croc relay used as the signaling channel and
	// as the last-resort data-plane fallback.
	DefaultRelay = "croc.schollz.com:9009"

	// ChunkSize is the fixed payload size (512 KiB) used by the swarm.
	ChunkSize = 512 * 1024

	// RoomKeySize is the AES-256 key length shared by all room members.
	RoomKeySize = 32

	// DefaultHeartbeatInterval and DefaultPeerTimeout drive liveness checks.
	DefaultHeartbeatInterval = 5 * time.Second
	DefaultPeerTimeout       = 20 * time.Second
)

// MessageType enumerates all control-plane message kinds.
type MessageType string

const (
	MsgJoin             MessageType = "JOIN"
	MsgJoinAck          MessageType = "JOIN_ACK"
	MsgPeerList         MessageType = "PEER_LIST"
	MsgFileAnnounce     MessageType = "FILE_ANNOUNCE"
	MsgBitfieldUpdate   MessageType = "BITFIELD_UPDATE"
	MsgPing             MessageType = "PING"
	MsgPong             MessageType = "PONG"
	MsgElectCoordinator MessageType = "ELECT_COORDINATOR"
	MsgLeave            MessageType = "LEAVE"
	MsgError            MessageType = "ERROR"
)

// PeerStatus is the liveness state of a room member.
type PeerStatus string

const (
	PeerOnline  PeerStatus = "online"
	PeerAway    PeerStatus = "away"
	PeerOffline PeerStatus = "offline"
)

// NATType describes how a peer is reachable from outside its network.
type NATType string

const (
	NATUnknown NATType = "unknown"
	NATLAN     NATType = "lan"
	NATSTUN    NATType = "stun"
	NATRelay   NATType = "relay"
)

// Sentinel errors shared across modules. Return them wrapped with %w so callers
// can use errors.Is while still receiving useful context.
var (
	ErrNotImplemented    = errors.New("protocol: not implemented")
	ErrInvalidMessage    = errors.New("protocol: invalid message")
	ErrUnauthorized      = errors.New("protocol: unauthorized")
	ErrRoomNotFound      = errors.New("protocol: room not found")
	ErrPeerOffline       = errors.New("protocol: peer offline")
	ErrP2PUnavailable    = errors.New("protocol: direct p2p connection unavailable")
	ErrFileNotFound      = errors.New("protocol: file not found")
	ErrChunkUnavailable  = errors.New("protocol: chunk unavailable")
	ErrChunkHashMismatch = errors.New("protocol: chunk hash mismatch")
	ErrFileIncomplete    = errors.New("protocol: file transfer incomplete")
)

// PeerInfo is the canonical description of a room member. It is exchanged in
// JOIN/JOIN_ACK/PEER_LIST messages and kept in the coordinator's member list.
type PeerInfo struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Addr        string     `json:"addr,omitempty"`       // public ip:port
	LocalAddr   string     `json:"local_addr,omitempty"` // LAN ip:port
	NAT         NATType    `json:"nat_type,omitempty"`
	Coordinator bool       `json:"coordinator"`
	Status      PeerStatus `json:"status"`
	JoinedAt    time.Time  `json:"joined_at"`
	LastSeen    time.Time  `json:"last_seen"`
}

// FileMeta describes an announced file and its chunk layout. Bitfield is the
// optional bitset snapshot of the announcing peer, encoded as raw bytes.
type FileMeta struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Size      int64     `json:"size"`
	ChunkSize int       `json:"chunk_size"`
	NumChunks uint32    `json:"num_chunks"`
	SHA256    string    `json:"sha256"`
	OwnerID   string    `json:"owner_id"`
	CreatedAt time.Time `json:"created_at"`
	Bitfield  []byte    `json:"bitfield,omitempty"`
}

// BitfieldUpdate advertises which chunks a peer currently owns for a file.
type BitfieldUpdate struct {
	FileID   string `json:"file_id"`
	PeerID   string `json:"peer_id"`
	Bitfield []byte `json:"bitfield"`
}

// ElectionInfo is attached to ELECT_COORDINATOR messages.
type ElectionInfo struct {
	CoordinatorID string `json:"coordinator_id"`
	Term          uint64 `json:"term"`
	Reason        string `json:"reason,omitempty"`
}

// ChunkRequest is sent peer-to-peer when a member needs a missing chunk.
type ChunkRequest struct {
	RoomID      string `json:"room_id"`
	FileID      string `json:"file_id"`
	Index       uint32 `json:"index"`
	RequesterID string `json:"requester_id"`
}

// ChunkData carries a single encrypted-at-transport chunk response.
type ChunkData struct {
	RoomID   string `json:"room_id"`
	FileID   string `json:"file_id"`
	Index    uint32 `json:"index"`
	Data     []byte `json:"data"`
	SHA256   string `json:"sha256"`
	SenderID string `json:"sender_id"`
}

// ControlMessage is the single envelope used on the signaling/control plane.
// Only the fields relevant to Type are populated; the rest stay nil/zero.
type ControlMessage struct {
	Version   string          `json:"version"`
	Type      MessageType     `json:"type"`
	RoomID    string          `json:"room_id,omitempty"`
	SenderID  string          `json:"sender_id,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
	Peer      *PeerInfo       `json:"peer,omitempty"`
	Peers     []PeerInfo      `json:"peers,omitempty"`
	File      *FileMeta       `json:"file,omitempty"`
	Files     []FileMeta      `json:"files,omitempty"`
	Bitfield  *BitfieldUpdate `json:"bitfield,omitempty"`
	Election  *ElectionInfo   `json:"election,omitempty"`
	Error     string          `json:"error,omitempty"`
}

// NewControlMessage builds an envelope with the protocol version and a UTC
// timestamp pre-filled.
func NewControlMessage(kind MessageType, roomID, senderID string) *ControlMessage {
	return &ControlMessage{
		Version:   ProtocolVersion,
		Type:      kind,
		RoomID:    roomID,
		SenderID:  senderID,
		Timestamp: time.Now().UTC(),
	}
}

// SecurityProvider handles PAKE room authentication and AES-256-GCM payload
// encryption. Implemented in internal/security.
type SecurityProvider interface {
	// Handshake runs the PAKE exchange over conn and derives the room key.
	Handshake(ctx context.Context, conn io.ReadWriteCloser, secret string) (sessionKey []byte, err error)
	// Encrypt seals plaintext with the current room key (nonce is prepended).
	Encrypt(plaintext []byte) ([]byte, error)
	// Decrypt opens a ciphertext produced by Encrypt.
	Decrypt(ciphertext []byte) ([]byte, error)
	// Fingerprint returns a short, non-secret identifier for the room key.
	Fingerprint() string
}

// TransportProvider abstracts LAN discovery, STUN, QUIC/UDP hole punching and
// relay fallback. Implemented in internal/transport.
type TransportProvider interface {
	// LocalAddr returns the preferred LAN address of this peer, if known.
	LocalAddr() net.Addr
	// PublicAddr performs STUN discovery and returns the externally visible
	// address together with the detected NAT strategy.
	PublicAddr(ctx context.Context) (net.Addr, NATType, error)
	// DialPeer connects directly to a peer, falling back to the relay.
	DialPeer(ctx context.Context, peer PeerInfo) (io.ReadWriteCloser, error)
	// AcceptPeer waits for an inbound direct connection.
	AcceptPeer(ctx context.Context) (io.ReadWriteCloser, PeerInfo, error)
	// Close releases listeners and sockets.
	Close() error
}

// RoomManager owns the control plane: relay signaling, membership, heartbeat,
// coordinator rotation and file/bitfield announcements. Implemented in
// internal/room.
type RoomManager interface {
	CreateRoom(ctx context.Context, roomID, secret string) (PeerInfo, error)
	JoinRoom(ctx context.Context, roomID, secret string) (PeerInfo, []PeerInfo, error)
	LeaveRoom(ctx context.Context) error
	Peers() []PeerInfo
	Coordinator() (PeerInfo, bool)
	AnnounceFile(ctx context.Context, meta FileMeta) error
	PublishBitfield(ctx context.Context, update BitfieldUpdate) error
	Heartbeat(ctx context.Context, peerID string) (time.Duration, error)
	Subscribe(handler func(ControlMessage)) (unsubscribe func())
	Close() error
}

// ChunkManager splits files into 512 KiB chunks, tracks bitfields and drives
// multi-source swarm downloads. Implemented in internal/chunker.
type ChunkManager interface {
	PrepareFile(path string) (FileMeta, error)
	ReadChunk(fileID string, index uint32) ([]byte, error)
	VerifyChunk(data []byte, expectedSHA256 string) bool
	SetBit(fileID string, index uint32) error
	ClearBit(fileID string, index uint32) error
	Bitfield(fileID string) ([]byte, error)
	IsComplete(fileID string) bool
	AssembleFile(fileID, outputPath string) error
	Download(ctx context.Context, meta FileMeta, sources []PeerInfo) error
}
