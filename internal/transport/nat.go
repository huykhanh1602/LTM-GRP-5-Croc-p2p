// Package transport implements network discovery, NAT traversal and the data
// plane for croc-p2p.
//
// Strategy order: LAN -> STUN-assisted UDP/QUIC hole punching -> relay
// fallback. The hole-punching state machine and QUIC listener are owned by the
// Team Lead (Member 1); this file provides the compiling scaffold plus a real
// STUN binding request so discovery can be tested immediately.
package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/huykhanh2141/croc-p2p/pkg/protocol"
	"github.com/pion/stun"
	"github.com/quic-go/quic-go"
)

// Default STUN servers tried in order during discovery.
var DefaultSTUNServers = []string{
	"stun.l.google.com:19302",
	"stun.cloudflare.com:3478",
	"stun.nextcloud.com:3478",
}

// Dial/IO timeouts used by the scaffold.
const (
	defaultSTUNTimeout = 3 * time.Second
	defaultDialTimeout = 10 * time.Second
)

// Transport is the default protocol.TransportProvider implementation.
//
// TODOs for Member 1:
//  1. Add a QUIC listener and implement simultaneous-open UDP hole punching.
//  2. Cache and refresh STUN mappings; support CGNAT port prediction.
//  3. Implement the croc relay framing fallback when direct dials fail.
var _ protocol.TransportProvider = (*Transport)(nil)

// Transport coordinates discovery and peer connections.
type Transport struct {
	// Discovery performs STUN lookups.
	Discovery *NATDiscovery
	// RelayAddr is the fallback croc relay endpoint.
	RelayAddr string

	quicConf *quic.Config

	mu         sync.RWMutex
	localAddr  net.Addr
	publicAddr net.Addr
	natType    protocol.NATType
}

// New returns a transport with STUN discovery and a QUIC config ready to use.
func New() *Transport {
	return &Transport{
		Discovery: NewNATDiscovery(),
		RelayAddr: protocol.DefaultRelay,
		quicConf:  defaultQUICConfig(),
		natType:   protocol.NATUnknown,
	}
}

func defaultQUICConfig() *quic.Config {
	return &quic.Config{
		HandshakeIdleTimeout: 10 * time.Second,
		MaxIdleTimeout:       30 * time.Second,
		KeepAlivePeriod:      15 * time.Second,
	}
}

// LocalAddr returns the first usable LAN IPv4 address, if any.
func (t *Transport) LocalAddr() net.Addr {
	t.mu.RLock()
	cached := t.localAddr
	t.mu.RUnlock()
	if cached != nil {
		return cached
	}

	ips, err := LocalIPs()
	if err != nil || len(ips) == 0 {
		return nil
	}
	addr := &net.UDPAddr{IP: ips[0]}

	t.mu.Lock()
	t.localAddr = addr
	t.mu.Unlock()
	return addr
}

// PublicAddr performs STUN discovery and caches the result.
func (t *Transport) PublicAddr(ctx context.Context) (net.Addr, protocol.NATType, error) {
	t.mu.RLock()
	cached := t.publicAddr
	nat := t.natType
	t.mu.RUnlock()
	if cached != nil {
		return cached, nat, nil
	}

	if t.Discovery == nil {
		return nil, protocol.NATUnknown, fmt.Errorf("transport: discovery not configured: %w", protocol.ErrP2PUnavailable)
	}
	addr, natType, err := t.Discovery.Discover(ctx)
	if err != nil {
		return nil, protocol.NATUnknown, err
	}

	t.mu.Lock()
	t.publicAddr = addr
	t.natType = natType
	t.mu.Unlock()
	return addr, natType, nil
}

// DialPeer attempts a direct QUIC connection, then falls back to the relay.
//
// TODO(member-1): implement UDP simultaneous open + QUIC dial. For now the
// scaffold validates the peer and returns ErrNotImplemented so callers can
// exercise the fallback path deliberately.
func (t *Transport) DialPeer(ctx context.Context, peer protocol.PeerInfo) (io.ReadWriteCloser, error) {
	if peer.ID == "" {
		return nil, fmt.Errorf("transport: peer id is required: %w", protocol.ErrInvalidMessage)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Placeholder reference so the pinned QUIC dependency is part of the build.
	_ = t.quicConf

	if peer.Addr == "" {
		return t.dialRelay(ctx)
	}
	return nil, fmt.Errorf("transport: direct dial to %s pending (member-1): %w", peer.Addr, protocol.ErrNotImplemented)
}

// AcceptPeer waits for an inbound direct connection.
//
// TODO(member-1): accept on the QUIC listener and return the authenticated peer.
func (t *Transport) AcceptPeer(ctx context.Context) (io.ReadWriteCloser, protocol.PeerInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, protocol.PeerInfo{}, err
	}
	return nil, protocol.PeerInfo{}, fmt.Errorf("transport: inbound QUIC listener pending (member-1): %w", protocol.ErrNotImplemented)
}

// Close releases cached sockets/listeners.
func (t *Transport) Close() error {
	t.mu.Lock()
	t.localAddr = nil
	t.publicAddr = nil
	t.natType = protocol.NATUnknown
	t.mu.Unlock()
	return nil
}

func (t *Transport) dialRelay(ctx context.Context) (io.ReadWriteCloser, error) {
	if t.RelayAddr == "" {
		return nil, fmt.Errorf("transport: no relay configured: %w", protocol.ErrP2PUnavailable)
	}
	dialer := &net.Dialer{Timeout: defaultDialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", t.RelayAddr)
	if err != nil {
		return nil, fmt.Errorf("transport: dial relay %s: %w", t.RelayAddr, err)
	}
	// TODO(member-1): wrap conn in the croc relay handshake/framing layer.
	return conn, nil
}

// NATDiscovery resolves the public mapping of the local socket via STUN.
type NATDiscovery struct {
	Servers []string
	Timeout time.Duration
}

// NewNATDiscovery returns discovery using the default STUN server list.
func NewNATDiscovery() *NATDiscovery {
	return &NATDiscovery{
		Servers: append([]string(nil), DefaultSTUNServers...),
		Timeout: defaultSTUNTimeout,
	}
}

// Discover tries each STUN server and returns the first XOR-MAPPED-ADDRESS.
func (n *NATDiscovery) Discover(ctx context.Context) (net.Addr, protocol.NATType, error) {
	timeout := n.Timeout
	if timeout <= 0 {
		timeout = defaultSTUNTimeout
	}
	var lastErr error
	for _, server := range n.Servers {
		addr, err := stunMappedAddr(ctx, server, timeout)
		if err == nil {
			return addr, protocol.NATSTUN, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("no stun servers configured")
	}
	return nil, protocol.NATUnknown, fmt.Errorf("transport: stun discovery failed: %w", lastErr)
}

func stunMappedAddr(ctx context.Context, server string, timeout time.Duration) (net.Addr, error) {
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "udp", server)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(timeout))
	}

	// stun.Connection only needs Read/Write/Close, which net.Conn provides.
	client, err := stun.NewClient(conn)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	var (
		xorAddr stun.XORMappedAddress
		getErr  error
	)
	message := stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	if err := client.Do(message, func(res stun.Event) {
		if res.Error != nil {
			getErr = res.Error
			return
		}
		getErr = xorAddr.GetFrom(res.Message)
	}); err != nil {
		return nil, err
	}
	if getErr != nil {
		return nil, getErr
	}
	if xorAddr.IP == nil {
		return nil, errors.New("transport: stun response had no mapped address")
	}
	return &net.UDPAddr{IP: xorAddr.IP, Port: xorAddr.Port}, nil
}

// LocalIPs returns non-loopback, up IPv4 addresses in interface order.
func LocalIPs() ([]net.IP, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("transport: list interfaces: %w", err)
	}

	var ips []net.IP
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			if v4 := ip.To4(); v4 != nil {
				ips = append(ips, v4)
			}
		}
	}
	return ips, nil
}
