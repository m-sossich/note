package node

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m-sossich/note/pkg/codec"
	jsoncdc "github.com/m-sossich/note/pkg/codec/json"
	"github.com/m-sossich/note/pkg/discovery"
	"github.com/m-sossich/note/pkg/p2p"
	"github.com/m-sossich/note/pkg/transport"
	"github.com/m-sossich/note/pkg/wire"
)

const (
	acceptRetryDelay = 5 * time.Millisecond
)

type PeerSource interface {
	Events() <-chan discovery.PeerEvent
}

type Config struct {
	NodeID             string
	ListenAddr         string
	Transport          transport.StreamTransport
	Codec              codec.Codec // defaults to JSON; all peers must use the same codec
	Handshaker         Handshaker
	MaxPeers           int // default: 128
	MaxInboundPeers    int // inbound sub-limit within MaxPeers; default: MaxPeers * 2/3
	MaxPendingPeers    int // concurrent in-flight handshakes; default: 32
	OnPeerConnected    func(peerID string)
	OnPeerDisconnected func(peerID string)
	// OnPeerCapabilitiesKnown fires when capabilities become known for a connected peer. Optional.
	OnPeerCapabilitiesKnown func(peerID string, protocols []string)
	ProtocolLimits          map[string]uint32 // per-protocol frame size limits; must be ≤ transport limit
}

func (c *Config) setDefaults() {
	if c.MaxPeers == 0 {
		c.MaxPeers = 128
	}
	if c.MaxInboundPeers == 0 {
		c.MaxInboundPeers = c.MaxPeers * 2 / 3
	}
	if c.MaxPendingPeers == 0 {
		c.MaxPendingPeers = 32
	}
	if c.Codec == nil {
		c.Codec = jsoncdc.New()
	}
}

// ProtocolHandler is an alias for p2p.Handler.
type ProtocolHandler = p2p.Handler

type Node interface {
	Start() error
	Stop() error
	Register(protocol string, handler p2p.Handler) // must be called before Start
	Send(peerID string, protocol string, msgType string, payload any) (int, error)
	Peers() []string
	ConnectionInfo(peerID string) (p2p.ConnInfo, bool)
	BoundAddr() string                            // reflects OS-assigned port when ListenAddr was ":0"
	PeerProtocols(peerID string) ([]string, bool) // bool=true if capabilities are known
	RegisteredProtocols() []string
}

type nodeImpl struct {
	cfg            Config
	disc           PeerSource
	conns          map[string]*connection
	dialing        map[string]struct{}
	mu             sync.RWMutex
	protocols      map[string]ProtocolHandler
	protoMu        sync.RWMutex
	peerProtocols  map[string][]string
	peerProtoMu    sync.RWMutex
	listener       transport.Listener
	boundAddr      string
	peerSem        chan struct{}               // total peer budget (MaxPeers)
	inboundGuard   chan struct{}               // inbound sub-limit (MaxInboundPeers)
	pendingSem     chan struct{}               // in-flight handshake budget (MaxPendingPeers)
	pendingConns   map[transport.Conn]struct{} // accepted but not yet past handshake
	pendingConnsMu sync.Mutex
	stopCh         chan struct{}
	wg             sync.WaitGroup
	started        atomic.Bool
	stopOnce       sync.Once
}

// New returns a running Node. Pass nil disc to manage connections externally.
func New(cfg Config, disc PeerSource) (Node, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("node: NodeID is required")
	}
	if cfg.Handshaker == nil {
		return nil, fmt.Errorf("node: Handshaker is required (use identify.New() for trusted mode, identify.NewSecure() for verified mode)")
	}
	cfg.setDefaults()
	return &nodeImpl{
		cfg:           cfg,
		disc:          disc,
		conns:         make(map[string]*connection),
		dialing:       make(map[string]struct{}),
		protocols:     make(map[string]ProtocolHandler),
		peerProtocols: make(map[string][]string),
		peerSem:       make(chan struct{}, cfg.MaxPeers),
		inboundGuard:  make(chan struct{}, cfg.MaxInboundPeers),
		pendingSem:    make(chan struct{}, cfg.MaxPendingPeers),
		pendingConns:  make(map[transport.Conn]struct{}),
		stopCh:        make(chan struct{}),
	}, nil
}

func (n *nodeImpl) Register(protocol string, handler ProtocolHandler) {
	n.protoMu.Lock()
	defer n.protoMu.Unlock()
	n.protocols[protocol] = handler
}

func (n *nodeImpl) handshakeConfig() HandshakeConfig {
	return HandshakeConfig{
		NodeID: n.cfg.NodeID,
		Codec:  n.cfg.Codec,
	}
}

func (n *nodeImpl) handlerFor(protocol string) (ProtocolHandler, bool) {
	n.protoMu.RLock()
	defer n.protoMu.RUnlock()
	h, ok := n.protocols[protocol]
	return h, ok
}

func (n *nodeImpl) Start() error {
	if !n.started.CompareAndSwap(false, true) {
		return fmt.Errorf("node: already started")
	}
	l, err := n.cfg.Transport.Listen(n.cfg.ListenAddr)
	if err != nil {
		n.started.Store(false) // allow retry after a listen failure
		return fmt.Errorf("listen on %s: %w", n.cfg.ListenAddr, err)
	}
	n.listener = l
	n.boundAddr = l.Addr().String()

	n.wg.Add(1)
	go n.acceptLoop()

	if n.disc != nil {
		n.wg.Add(1)
		go n.discoveryLoop()
	}
	return nil
}

func (n *nodeImpl) Stop() error {
	n.stopOnce.Do(func() {
		close(n.stopCh)
		if n.listener != nil {
			if err := n.listener.Close(); err != nil {
				slog.Warn("node: listener close error", "err", err)
			}
		}
		n.closePendingConns()
		n.closeEstablishedConns()
		n.wg.Wait()
	})
	return nil
}

func (n *nodeImpl) closePendingConns() {
	n.pendingConnsMu.Lock()
	for c := range n.pendingConns {
		c.Close()
	}
	n.pendingConnsMu.Unlock()
}

func (n *nodeImpl) closeEstablishedConns() {
	n.mu.RLock()
	conns := make([]*connection, 0, len(n.conns))
	for _, conn := range n.conns {
		conns = append(conns, conn)
	}
	n.mu.RUnlock()
	for _, conn := range conns {
		sendDisconnect(conn, wire.ReasonShutdown, "node shutting down")
		if err := conn.Close(); err != nil {
			slog.Warn("node: conn close error during shutdown", "peer_id", conn.peerID, "err", err)
		}
	}
}

func (n *nodeImpl) Send(peerID string, protocol string, msgType string, payload any) (int, error) {
	n.mu.RLock()
	conn, ok := n.conns[peerID]
	n.mu.RUnlock()
	if !ok {
		return 0, fmt.Errorf("no connection to peer %s", peerID)
	}
	if conn.state() != stateConnected {
		return 0, fmt.Errorf("peer %s connection is %s", peerID, conn.state())
	}
	if err := n.checkCapability(peerID, protocol); err != nil {
		return 0, err
	}
	frame, err := encodeApplicationFrame(conn, protocol, msgType, payload)
	if err != nil {
		return 0, fmt.Errorf("encode for %s: %w", peerID, err)
	}
	return conn.Send(frame)
}

func (n *nodeImpl) checkCapability(peerID, protocol string) error {
	n.peerProtoMu.RLock()
	advertised, known := n.peerProtocols[peerID]
	n.peerProtoMu.RUnlock()
	if !known {
		return nil
	}
	for _, p := range advertised {
		if p == protocol {
			return nil
		}
	}
	return fmt.Errorf("peer %s does not advertise protocol %q", peerID, protocol)
}

func encodeApplicationFrame(conn *connection, protocol, msgType string, payload any) ([]byte, error) {
	innerBytes, err := conn.encode(payload)
	if err != nil {
		return nil, fmt.Errorf("encode payload: %w", err)
	}
	env := wire.Envelope{
		Protocol: protocol,
		Type:     msgType,
		Payload:  innerBytes,
	}
	envBytes, err := conn.encode(env)
	if err != nil {
		return nil, fmt.Errorf("encode envelope: %w", err)
	}
	return wire.Encode(wire.Frame{Type: wire.TypeApplication, Payload: envBytes}), nil
}

func (n *nodeImpl) acceptLoop() {
	defer n.wg.Done()
	for {
		c, err := n.listener.Accept()
		if err != nil {
			select {
			case <-n.stopCh:
				return
			default:
				slog.Warn("accept error", "err", err)
				time.Sleep(acceptRetryDelay)
				continue
			}
		}
		select {
		case n.pendingSem <- struct{}{}:
			n.pendingConnsMu.Lock()
			n.pendingConns[c] = struct{}{}
			n.pendingConnsMu.Unlock()
			n.wg.Add(1)
			go func() {
				defer n.wg.Done()
				defer func() { <-n.pendingSem }()
				defer func() {
					n.pendingConnsMu.Lock()
					delete(n.pendingConns, c)
					n.pendingConnsMu.Unlock()
				}()
				n.handleInbound(c)
			}()
		default:
			slog.Warn("pending peer limit reached, rejecting inbound connection",
				"limit", cap(n.pendingSem), "remote_addr", c.RemoteAddr())
			c.Close()
		}
	}
}

func (n *nodeImpl) handleInbound(c transport.Conn) {
	res, err := n.cfg.Handshaker.Accept(c, n.handshakeConfig())
	if err != nil {
		slog.Warn("inbound handshake failed", "err", err)
		c.Close()
		return
	}

	// Acquire inbound guard first, then total peer budget — order matters to avoid livelock.
	select {
	case n.inboundGuard <- struct{}{}:
	default:
		slog.Warn("inbound peer limit reached, rejecting",
			"limit", cap(n.inboundGuard), "remote_addr", c.RemoteAddr())
		c.Close()
		return
	}

	select {
	case n.peerSem <- struct{}{}:
		n.serveConn(newConnection(res, c, n.cfg.Codec), func() {
			<-n.peerSem
			<-n.inboundGuard
		})
	default:
		<-n.inboundGuard
		slog.Warn("total peer limit reached, rejecting inbound connection",
			"limit", cap(n.peerSem), "remote_addr", c.RemoteAddr())
		c.Close()
	}
}

// serveConn registers conn and starts its read loop, applying NOD-15 tie-breaking on simultaneous connections.
func (n *nodeImpl) serveConn(conn *connection, releaseSlot func()) {
	won, loser := n.addConn(conn)
	if !won {
		n.rejectConn(conn, releaseSlot)
		return
	}
	if loser != nil {
		sendDisconnect(loser, wire.ReasonShutdown, "simultaneous connection tie-break")
		loser.Close()
	}
	if n.isShuttingDown() {
		n.removeConnIfCurrent(conn.peerID, conn)
		if releaseSlot != nil {
			releaseSlot()
		}
		conn.Close()
		return
	}
	if loser == nil {
		n.notifyConnected(conn.peerID)
	}
	n.startReadLoop(conn, releaseSlot)
}

func (n *nodeImpl) rejectConn(conn *connection, releaseSlot func()) {
	if releaseSlot != nil {
		releaseSlot()
	}
	sendDisconnect(conn, wire.ReasonShutdown, "simultaneous connection tie-break")
	conn.Close()
}

func (n *nodeImpl) isShuttingDown() bool {
	select {
	case <-n.stopCh:
		return true
	default:
		return false
	}
}

func (n *nodeImpl) notifyConnected(peerID string) {
	slog.Info("peer connected", "peer_id", peerID)
	if n.cfg.OnPeerConnected != nil {
		n.cfg.OnPeerConnected(peerID)
	}
}

func (n *nodeImpl) startReadLoop(conn *connection, releaseSlot func()) {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		if releaseSlot != nil {
			defer releaseSlot()
		}
		runReadLoop(conn, n.handlerFor, n.limitFor)
		if n.removeConnIfCurrent(conn.peerID, conn) {
			slog.Info("peer disconnected", "peer_id", conn.peerID)
			if n.cfg.OnPeerDisconnected != nil {
				n.cfg.OnPeerDisconnected(conn.peerID)
			}
		}
	}()
}

func (n *nodeImpl) discoveryLoop() {
	defer n.wg.Done()
	for {
		select {
		case ev := <-n.disc.Events():
			switch ev.Type {
			case discovery.PeerFound:
				n.handlePeerFound(ev)
			case discovery.PeerLost:
				n.handlePeerLost(ev)
			}
		case <-n.stopCh:
			return
		}
	}
}

func (n *nodeImpl) handlePeerFound(ev discovery.PeerEvent) {
	if ev.PeerID == n.cfg.NodeID {
		return
	}
	if ev.Protocols != nil {
		n.peerProtoMu.Lock()
		n.peerProtocols[ev.PeerID] = ev.Protocols
		n.peerProtoMu.Unlock()
		if n.cfg.OnPeerCapabilitiesKnown != nil {
			n.mu.RLock()
			_, connected := n.conns[ev.PeerID]
			n.mu.RUnlock()
			if connected {
				n.cfg.OnPeerCapabilitiesKnown(ev.PeerID, ev.Protocols)
			}
		}
	}
	if ev.Protocols != nil && len(ev.Protocols) == 0 {
		return
	}
	select {
	case n.pendingSem <- struct{}{}:
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			defer func() { <-n.pendingSem }()
			n.connectToPeer(ev.PeerID, ev.Address)
		}()
	default:
		slog.Warn("pending peer limit reached, dropping peer-found event",
			"peer_id", ev.PeerID, "limit", cap(n.pendingSem))
	}
}

func (n *nodeImpl) handlePeerLost(ev discovery.PeerEvent) {
	n.peerProtoMu.Lock()
	delete(n.peerProtocols, ev.PeerID)
	n.peerProtoMu.Unlock()
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.disconnectPeer(ev.PeerID)
	}()
}

func (n *nodeImpl) disconnectPeer(peerID string) {
	n.mu.RLock()
	conn, ok := n.conns[peerID]
	n.mu.RUnlock()
	if !ok {
		return
	}
	sendDisconnect(conn, wire.ReasonPeerLost, "peer evicted by discovery liveness check")
	conn.Close()
}

func (n *nodeImpl) tryBeginDial(peerID string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.conns[peerID]; ok {
		return false
	}
	if _, ok := n.dialing[peerID]; ok {
		return false
	}
	n.dialing[peerID] = struct{}{}
	return true
}

func (n *nodeImpl) endDial(peerID string) {
	n.mu.Lock()
	delete(n.dialing, peerID)
	n.mu.Unlock()
}

// connectToPeer dials addr and serves the connection. pendingSem is held by the caller.
func (n *nodeImpl) connectToPeer(peerID, addr string) {
	if !n.tryBeginDial(peerID) {
		return
	}
	defer n.endDial(peerID)

	c, err := n.cfg.Transport.Dial(addr)
	if err != nil {
		slog.Warn("dial failed", "peer_id", peerID, "addr", addr, "err", err)
		return
	}
	// Pass PeerID so the outbound handshaker can return it without an ACK round-trip.
	hsCfg := n.handshakeConfig()
	hsCfg.PeerID = peerID
	res, err := n.cfg.Handshaker.Initiate(c, hsCfg)
	if err != nil {
		slog.Warn("outbound handshake failed", "peer_id", peerID, "addr", addr, "err", err)
		c.Close()
		return
	}

	select {
	case n.peerSem <- struct{}{}:
		n.serveConn(newConnection(res, c, n.cfg.Codec), func() { <-n.peerSem })
	default:
		slog.Warn("total peer limit reached, dropping outbound connection",
			"peer_id", peerID, "limit", cap(n.peerSem))
		c.Close()
	}
}

func (n *nodeImpl) Peers() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	peers := make([]string, 0, len(n.conns))
	for id := range n.conns {
		peers = append(peers, id)
	}
	return peers
}

func (n *nodeImpl) BoundAddr() string { return n.boundAddr }

func (n *nodeImpl) PeerProtocols(peerID string) ([]string, bool) {
	n.peerProtoMu.RLock()
	defer n.peerProtoMu.RUnlock()
	ps, known := n.peerProtocols[peerID]
	if !known {
		return nil, false
	}
	out := make([]string, len(ps))
	copy(out, ps)
	return out, true
}

func (n *nodeImpl) RegisteredProtocols() []string {
	n.protoMu.RLock()
	defer n.protoMu.RUnlock()
	ps := make([]string, 0, len(n.protocols))
	for p := range n.protocols {
		ps = append(ps, p)
	}
	return ps
}

func (n *nodeImpl) limitFor(protocol string) uint32 {
	if n.cfg.ProtocolLimits == nil {
		return 0
	}
	return n.cfg.ProtocolLimits[protocol]
}

func (n *nodeImpl) ConnectionInfo(peerID string) (ConnInfo, bool) {
	n.mu.RLock()
	conn, ok := n.conns[peerID]
	n.mu.RUnlock()
	if !ok {
		return ConnInfo{}, false
	}
	return ConnInfo{
		RemoteAddr: conn.RemoteAddr(),
		PublicKey:  conn.publicKey,
	}, true
}

// addConn registers conn, applying NOD-15 tie-breaking on simultaneous connections.
// won=true,loser=nil: registered. won=true,loser=other: caller closes loser. won=false: caller closes conn.
func (n *nodeImpl) addConn(conn *connection) (won bool, loser *connection) {
	n.mu.Lock()
	defer n.mu.Unlock()
	existing, ok := n.conns[conn.peerID]
	if !ok {
		n.conns[conn.peerID] = conn
		return true, nil
	}
	// NOD-15: the node with the lexicographically greater identity closes its side.
	if n.cfg.NodeID > conn.peerID {
		return false, nil
	}
	n.conns[conn.peerID] = conn
	return true, existing
}

// removeConnIfCurrent removes peerID only if the map still points to conn — a replaced connection skips removal.
func (n *nodeImpl) removeConnIfCurrent(peerID string, conn *connection) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.conns[peerID] == conn {
		delete(n.conns, peerID)
		return true
	}
	return false
}
