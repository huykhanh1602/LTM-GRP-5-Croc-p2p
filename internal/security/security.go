// Package security implements the croc-p2p handshake and end-to-end
// encryption layer.
//
// The concrete PAKE exchange and room-key distribution are owned by the
// Security & Cryptography module owner (Member 2). Everything below is a
// compiling scaffold with clearly marked extension points.
package security

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/huykhanh2141/croc-p2p/pkg/protocol"

	// TODO(member-2): replace this blank import with the real PAKE calls.
	// The dependency is pinned now so the module graph is stable from day one.
	_ "github.com/schollz/pake/v3"
)

// KeySize and FingerprintBytes match the AES-256 room-key contract.
const (
	KeySize          = protocol.RoomKeySize
	FingerprintBytes = 8
)

// PAKESecurityProvider implements protocol.SecurityProvider.
//
// TODOs for Member 2:
//  1. Run pake.InitCurve over the relay connection using the room secret.
//  2. Derive the shared Room AES-256 key from the PAKE result.
//  3. Bind the key to the room ID with HKDF (never reuse a key across rooms).
//  4. Replace the SHA-256 placeholder KDF below with the PAKE-derived key.
var _ protocol.SecurityProvider = (*PAKESecurityProvider)(nil)

// PAKESecurityProvider holds the current room key and AEAD instance.
type PAKESecurityProvider struct {
	mu          sync.RWMutex
	roomKey     []byte
	aead        cipher.AEAD
	fingerprint string
}

// NewPAKESecurityProvider returns an empty provider. Call Handshake (or
// SetRoomKey after a successful PAKE exchange) before Encrypt/Decrypt.
func NewPAKESecurityProvider() *PAKESecurityProvider {
	return &PAKESecurityProvider{}
}

// Handshake performs the PAKE exchange and installs the derived room key.
//
// TODO(member-2): this is intentionally not implemented yet. The stub validates
// inputs and returns ErrNotImplemented so callers fail closed.
func (p *PAKESecurityProvider) Handshake(ctx context.Context, conn io.ReadWriteCloser, secret string) ([]byte, error) {
	if secret == "" {
		return nil, fmt.Errorf("security: empty room secret: %w", protocol.ErrUnauthorized)
	}
	if conn == nil {
		return nil, fmt.Errorf("security: nil handshake connection: %w", protocol.ErrInvalidMessage)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// TODO(member-2): PAKE key agreement goes here. Placeholder so the module
	// builds and downstream code can be wired against the final contract.
	return nil, fmt.Errorf("security: PAKE handshake pending (member-2): %w", protocol.ErrNotImplemented)
}

// SetRoomKey installs an already-derived AES-256 room key. It is exported so
// the PAKE implementation and tests can inject the shared key directly.
func (p *PAKESecurityProvider) SetRoomKey(key []byte) error {
	if len(key) != KeySize {
		return fmt.Errorf("security: room key must be %d bytes, got %d: %w", KeySize, len(key), protocol.ErrUnauthorized)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return fmt.Errorf("security: create AES cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return fmt.Errorf("security: create GCM: %w", err)
	}
	sum := sha256.Sum256(key)

	p.mu.Lock()
	defer p.mu.Unlock()
	p.roomKey = append([]byte(nil), key...)
	p.aead = aead
	p.fingerprint = hex.EncodeToString(sum[:FingerprintBytes])
	return nil
}

// RoomKey returns a defensive copy of the installed room key.
func (p *PAKESecurityProvider) RoomKey() []byte {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return append([]byte(nil), p.roomKey...)
}

// Fingerprint returns a short, non-secret identifier of the room key.
func (p *PAKESecurityProvider) Fingerprint() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.fingerprint
}

// Encrypt seals plaintext with AES-256-GCM and prepends the nonce.
func (p *PAKESecurityProvider) Encrypt(plaintext []byte) ([]byte, error) {
	p.mu.RLock()
	aead := p.aead
	p.mu.RUnlock()
	if aead == nil {
		return nil, fmt.Errorf("security: room key not installed: %w", protocol.ErrUnauthorized)
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("security: generate nonce: %w", err)
	}
	return aead.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt opens a ciphertext produced by Encrypt.
func (p *PAKESecurityProvider) Decrypt(ciphertext []byte) ([]byte, error) {
	p.mu.RLock()
	aead := p.aead
	p.mu.RUnlock()
	if aead == nil {
		return nil, fmt.Errorf("security: room key not installed: %w", protocol.ErrUnauthorized)
	}
	if len(ciphertext) < aead.NonceSize() {
		return nil, errors.New("security: ciphertext shorter than nonce")
	}

	nonce := ciphertext[:aead.NonceSize()]
	plaintext, err := aead.Open(nil, nonce, ciphertext[aead.NonceSize():], nil)
	if err != nil {
		return nil, fmt.Errorf("security: decrypt chunk: %w", err)
	}
	return plaintext, nil
}

// DeriveRoomKey is the placeholder KDF used until PAKE lands. It binds the
// secret to the room ID via domain separation.
//
// TODO(member-2): remove this function once the PAKE-derived key is wired in.
func DeriveRoomKey(secret, roomID string) []byte {
	sum := sha256.Sum256([]byte("croc-p2p/room-key/v1|" + roomID + "|" + secret))
	return sum[:]
}
