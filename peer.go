// Package note provides a high-level API for building P2P applications.
// Peer wraps discovery, connection management, and optional DHT into one
// composable unit. NewPeer returns a running peer — no separate Start call.
//
// Trusted mode:
//
//	p, err := note.NewPeer("0.0.0.0:9000",
//	    note.WithBootstrap("bootstrap.example.com:9000"),
//	    note.WithHandler("chat/1.0", chatHandler),
//	)
//	if err != nil { ... }
//	defer p.Close()
//
// Verified mode:
//
//	kp, _ := identity.LoadOrGenerate("./node.key")
//	p, err := note.NewVerifiedPeer(kp, "0.0.0.0:9000",
//	    note.WithBootstrap("bootstrap.example.com:9000"),
//	    note.WithHandler("chat/1.0", chatHandler),
//	)
package note

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/m-sossich/note/pkg/codec"

	"github.com/m-sossich/note/pkg/dht"
	"github.com/m-sossich/note/pkg/discovery"
	"github.com/m-sossich/note/pkg/identity"
	"github.com/m-sossich/note/pkg/node"
	"github.com/m-sossich/note/pkg/node/identify"
	"github.com/m-sossich/note/pkg/p2p"
	"github.com/m-sossich/note/pkg/transport"
	tcptransport "github.com/m-sossich/note/pkg/transport/tcp"
	tlstransport "github.com/m-sossich/note/pkg/transport/tls"
	"github.com/m-sossich/note/pkg/transport/udp"
)

// ProviderRecord is a DHT provider entry returned by FindProviders.
type ProviderRecord = dht.ProviderRecord

// StoreResult describes the outcome of an Announce operation.
type StoreResult = dht.StoreResult

// Handler is the sub-protocol message handler signature.
type Handler = p2p.Handler

// ConnInfo holds observable details about a connected peer.
type ConnInfo = p2p.ConnInfo

// Message is an outbound P2P message. A struct prevents transposing protocol and type, which are both strings.
type Message struct {
	Protocol string
	Type     string
	Payload  any
}

// Msg is a shorthand constructor for inline send calls.
func Msg(protocol, msgType string, payload any) Message {
	return Message{Protocol: protocol, Type: msgType, Payload: payload}
}

// Peer is a P2P network participant. Create with NewPeer or NewVerifiedPeer.
type Peer struct {
	nodeID string
	addr   string
	disc   *discovery.Discovery
	n      node.Node
	d      *dht.DHT // nil if DHT not enabled
}

type registeredHandler struct {
	protocol string
	handler  Handler
}

type peerConfig struct {
	advertiseAddr     string
	bootstraps        []string
	handlers          []registeredHandler
	handlerFactories  []func(node.Node)
	codec             codec.Codec
	maxFrameSize      uint32
	protocolLimits    map[string]uint32
	maxPeers          int
	maxInboundPeers   int
	maxPendingPeers   int
	pingInterval      time.Duration
	pingMaxMissed     int
	discoveryMaxPeers int
	handshakeTimeout  time.Duration
	dhtEnabled        bool
	dhtCfg            dht.Config
	identityPath      string
	keypair           *identity.Keypair
	onConnected       func(string)
	onDisconnected    func(string)
}

// Option configures a Peer.
type Option func(*peerConfig)

func WithBootstrap(addrs ...string) Option {
	return func(c *peerConfig) { c.bootstraps = append(c.bootstraps, addrs...) }
}

// WithAdvertiseAddr sets the address announced to peers. Required when listen addr is not directly routable (NAT, Docker, cloud VMs).
func WithAdvertiseAddr(addr string) Option {
	return func(c *peerConfig) { c.advertiseAddr = addr }
}

// WithHandler registers a sub-protocol handler. Must be called before Start.
func WithHandler(protocol string, handler Handler) Option {
	return func(c *peerConfig) {
		c.handlers = append(c.handlers, registeredHandler{protocol, handler})
	}
}

// WithHandlerFactory registers a factory called with node.Node after static handlers are registered but before Start.
// Use when the handler needs the node at construction time.
func WithHandlerFactory(factory func(node.Node)) Option {
	return func(c *peerConfig) {
		c.handlerFactories = append(c.handlerFactories, factory)
	}
}

// WithDHT enables Kademlia DHT. An optional dht.Config tunes bucket size, alpha, and timeouts.
func WithDHT(cfg ...dht.Config) Option {
	return func(c *peerConfig) {
		c.dhtEnabled = true
		if len(cfg) > 0 {
			c.dhtCfg = cfg[0]
		}
	}
}

func WithMaxPeers(n int) Option {
	return func(c *peerConfig) { c.maxPeers = n }
}

// WithMaxInboundPeers sets the inbound sub-limit within MaxPeers, reserving capacity for outbound dials. Default: MaxPeers * 2/3.
func WithMaxInboundPeers(n int) Option {
	return func(c *peerConfig) { c.maxInboundPeers = n }
}

// WithMaxPendingPeers caps concurrent in-flight handshakes; bounds TLS CPU cost under connection storms. Default: 32.
func WithMaxPendingPeers(n int) Option {
	return func(c *peerConfig) { c.maxPendingPeers = n }
}

// WithPingMaxMissed sets consecutive unanswered pings before eviction. Dead-peer window = PingInterval × n. Default: 3.
func WithPingMaxMissed(n int) Option {
	return func(c *peerConfig) { c.pingMaxMissed = n }
}

// WithDiscoveryMaxPeers caps the discovery peer table size. When full, the peer
// with the most missed pings is evicted on each new ANNOUNCE; ties broken randomly.
// Default 0 = unbounded. Set on bootstrap infrastructure to bound memory.
func WithDiscoveryMaxPeers(n int) Option {
	return func(c *peerConfig) { c.discoveryMaxPeers = n }
}

// WithHandshakeTimeout sets the handshake deadline (identity exchange or TLS). Default: 10s.
func WithHandshakeTimeout(d time.Duration) Option {
	return func(c *peerConfig) { c.handshakeTimeout = d }
}

// WithPingInterval controls liveness ping frequency. Shorter detects failures faster; longer reduces UDP traffic. Default: 1s.
func WithPingInterval(d time.Duration) Option {
	return func(c *peerConfig) { c.pingInterval = d }
}

// WithIdentityPath sets where the trusted-mode UUID identity is persisted. Default: ./<sanitized_addr>.id.
func WithIdentityPath(path string) Option {
	return func(c *peerConfig) { c.identityPath = path }
}

// WithPeerConnected sets a callback fired when a peer reaches CONNECTED. DHT routing is seeded before this fires.
func WithPeerConnected(fn func(peerID string)) Option {
	return func(c *peerConfig) { c.onConnected = fn }
}

func WithPeerDisconnected(fn func(peerID string)) Option {
	return func(c *peerConfig) { c.onDisconnected = fn }
}

// WithCodec sets the network codec. All peers in the network must use the same codec. Default: JSON.
func WithCodec(c codec.Codec) Option {
	return func(cfg *peerConfig) { cfg.codec = c }
}

// WithMaxFrameSize sets the global transport-level frame size ceiling. Default: 64 KiB.
// Both sides must accept the same or larger limit; use WithProtocolFrameSize for per-protocol caps.
func WithMaxFrameSize(bytes uint32) Option {
	return func(c *peerConfig) { c.maxFrameSize = bytes }
}

// WithProtocolFrameSize sets a per-protocol frame size limit enforced at dispatch time,
// layered on top of the global ceiling set by WithMaxFrameSize.
func WithProtocolFrameSize(protocol string, bytes uint32) Option {
	return func(c *peerConfig) {
		if c.protocolLimits == nil {
			c.protocolLimits = make(map[string]uint32)
		}
		c.protocolLimits[protocol] = bytes
	}
}

// NewPeer creates and starts a trusted-mode Peer. addr is used for both UDP discovery and TCP connections.
func NewPeer(addr string, opts ...Option) (*Peer, error) {
	cfg := &peerConfig{}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.identityPath == "" {
		cfg.identityPath = "./" + sanitizeAddr(addr) + ".id"
	}
	nodeID, err := loadOrGenerateID(cfg.identityPath)
	if err != nil {
		return nil, fmt.Errorf("note: identity: %w", err)
	}
	return buildAndStart(nodeID, addr, cfg)
}

// NewVerifiedPeer creates and starts a verified-mode Peer with Ed25519 identity.
func NewVerifiedPeer(kp *identity.Keypair, addr string, opts ...Option) (*Peer, error) {
	cfg := &peerConfig{keypair: kp}
	for _, o := range opts {
		o(cfg)
	}
	return buildAndStart(kp.NodeID, addr, cfg)
}

func (p *Peer) Close() error {
	if p.d != nil {
		p.d.Stop()
	}
	err := p.n.Stop()
	if discErr := p.disc.Stop(); discErr != nil && err == nil {
		err = discErr
	}
	return err
}

func (p *Peer) Send(peerID string, msg Message) (int, error) {
	return p.n.Send(peerID, msg.Protocol, msg.Type, msg.Payload)
}

func (p *Peer) Broadcast(msg Message) int {
	var sent int
	for _, id := range p.n.Peers() {
		if _, err := p.n.Send(id, msg.Protocol, msg.Type, msg.Payload); err == nil {
			sent++
		}
	}
	return sent
}

func (p *Peer) Peers() []string { return p.n.Peers() }
func (p *Peer) ID() string      { return p.nodeID }

// Addr returns the listen address; reflects OS-assigned port when started with ":0".
func (p *Peer) Addr() string { return p.n.BoundAddr() }

// Lookup performs an iterative FIND_NODE and returns the k closest nodes to key. Requires WithDHT.
func (p *Peer) Lookup(ctx context.Context, key []byte) ([]dht.NodeInfo, error) {
	if p.d == nil {
		return nil, errors.New("note: DHT not enabled — use WithDHT()")
	}
	return p.d.Lookup(ctx, key)
}

// FindProviders returns all nodes that have announced themselves as holders of key. Requires WithDHT.
func (p *Peer) FindProviders(ctx context.Context, key []byte) ([]ProviderRecord, error) {
	if p.d == nil {
		return nil, errors.New("note: DHT not enabled — use WithDHT()")
	}
	return p.d.FindProviders(ctx, key)
}

// Announce registers this peer as a provider for key. Requires WithDHT.
// Attempted==0 means no other nodes exist yet; Replicated<Attempted means partial replication.
func (p *Peer) Announce(ctx context.Context, key, value []byte) (StoreResult, error) {
	if p.d == nil {
		return StoreResult{}, errors.New("note: DHT not enabled — use WithDHT()")
	}
	return p.d.Store(ctx, key, value)
}

func (p *Peer) ConnectionInfo(peerID string) (ConnInfo, bool) {
	return p.n.ConnectionInfo(peerID)
}

// ---------------------------------------------------------------------------
// internal
// ---------------------------------------------------------------------------

func buildAndStart(nodeID, addr string, cfg *peerConfig) (*Peer, error) {
	advertiseAddr := first(cfg.advertiseAddr, addr)
	c := cfg.codec

	disc, err := newDiscovery(nodeID, addr, advertiseAddr, c, cfg)
	if err != nil {
		return nil, err
	}

	p := &Peer{nodeID: nodeID, addr: advertiseAddr, disc: disc}

	n, err := newNode(nodeID, addr, c, cfg, disc, p)
	if err != nil {
		disc.Stop()
		return nil, err
	}
	p.n = n

	registerHandlers(n, cfg)

	if cfg.dhtEnabled {
		dhtCfg := cfg.dhtCfg
		if cfg.keypair != nil && dhtCfg.EntryValidator == nil {
			dhtCfg.EntryValidator = identity.ValidateNodeEntry
		}
		p.d = dht.New(n, nodeID, advertiseAddr, dhtCfg)
	}

	if err := n.Start(); err != nil {
		disc.Stop()
		return nil, fmt.Errorf("note: node start: %w", err)
	}
	if err := disc.Start(n.RegisteredProtocols()); err != nil {
		n.Stop()
		return nil, fmt.Errorf("note: discovery start: %w", err)
	}

	return p, nil
}

func newDiscovery(nodeID, addr, advertiseAddr string, c codec.Codec, cfg *peerConfig) (*discovery.Discovery, error) {
	udpTr, err := udp.New(addr)
	if err != nil {
		return nil, fmt.Errorf("note: udp: %w", err)
	}
	disc, err := discovery.New(discovery.Config{
		NodeID:         nodeID,
		BindAddr:       addr,
		AdvertiseAddr:  advertiseAddr,
		BootstrapAddrs: cfg.bootstraps,
		Keypair:        cfg.keypair,
		Codec:          c,
		PingInterval:   cfg.pingInterval,
		PingMaxMissed:  cfg.pingMaxMissed,
		MaxPeers:       cfg.discoveryMaxPeers,
	}, udpTr)
	if err != nil {
		udpTr.Close()
		return nil, fmt.Errorf("note: discovery: %w", err)
	}
	return disc, nil
}

func first(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func newNode(nodeID, addr string, c codec.Codec, cfg *peerConfig, disc *discovery.Discovery, p *Peer) (node.Node, error) {
	hsCfg := identify.Config{Timeout: cfg.handshakeTimeout}
	var tr transport.StreamTransport
	var hs node.Handshaker
	if cfg.keypair != nil {
		tr = tlstransport.New(cfg.keypair, cfg.maxFrameSize)
		hs = identify.NewSecure(hsCfg)
	} else {
		tr = tcptransport.New(cfg.maxFrameSize)
		hs = identify.New(hsCfg)
	}
	n, err := node.New(node.Config{
		NodeID:                  nodeID,
		ListenAddr:              addr,
		Transport:               tr,
		Codec:                   c,
		Handshaker:              hs,
		MaxPeers:                cfg.maxPeers,
		MaxInboundPeers:         cfg.maxInboundPeers,
		MaxPendingPeers:         cfg.maxPendingPeers,
		ProtocolLimits:          cfg.protocolLimits,
		OnPeerConnected:         p.onPeerConnected(cfg.onConnected),
		OnPeerDisconnected:      cfg.onDisconnected,
		OnPeerCapabilitiesKnown: p.onPeerCapabilitiesKnown,
	}, disc)
	if err != nil {
		return nil, fmt.Errorf("note: node: %w", err)
	}
	return n, nil
}

func registerHandlers(n node.Node, cfg *peerConfig) {
	for _, h := range cfg.handlers {
		n.Register(h.protocol, h.handler)
	}
	for _, factory := range cfg.handlerFactories {
		factory(n)
	}
}

func (p *Peer) onPeerConnected(userCb func(string)) func(string) {
	return func(peerID string) {
		p.seedDHT(peerID)
		if userCb != nil {
			userCb(peerID)
		}
	}
}

func (p *Peer) seedDHT(peerID string) {
	if p.d == nil {
		return
	}
	protos, known := p.n.PeerProtocols(peerID)
	// Unknown capabilities: dial conservatively as DHT-capable.
	// Known but no DHT protocol: skip routing.
	if known && !containsString(protos, dht.Protocol) {
		return
	}
	info, ok := p.n.ConnectionInfo(peerID)
	if !ok || info.DeclaredAddr == "" {
		return // no declared address — ephemeral source port is not dialable (DHT-2)
	}
	p.d.SeedPeer(peerID, info.DeclaredAddr, info.PublicKey)
}

func (p *Peer) onPeerCapabilitiesKnown(peerID string, protocols []string) {
	if p.d != nil && !containsString(protocols, dht.Protocol) {
		p.d.RemovePeer(peerID)
	}
}

// LoadOrGenerateID reads a UUID identity from path, generating and persisting one if absent.
func LoadOrGenerateID(path string) (string, error) {
	return loadOrGenerateID(path)
}

func loadOrGenerateID(path string) (string, error) {
	if data, err := os.ReadFile(path); err == nil {
		if id := strings.TrimSpace(string(data)); id != "" {
			return id, nil
		}
	}
	id := uuid.New().String()
	if err := os.WriteFile(path, []byte(id+"\n"), 0600); err != nil {
		return "", fmt.Errorf("persist identity to %s: %w", path, err)
	}
	return id, nil
}

func sanitizeAddr(addr string) string {
	return strings.NewReplacer(":", "_", ".", "_").Replace(addr)
}

func containsString(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
