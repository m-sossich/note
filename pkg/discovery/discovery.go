package discovery

import (
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/m-sossich/note/pkg/codec"
	jsoncdc "github.com/m-sossich/note/pkg/codec/json"
	"github.com/m-sossich/note/pkg/identity"
	"github.com/m-sossich/note/pkg/transport"
)

// isUnspecifiedAddr returns true when the host part of addr is an unspecified
// IP (0.0.0.0 or ::). Advertising such an address is a misconfiguration —
// other nodes cannot dial it.
func isUnspecifiedAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsUnspecified()
}

type PeerEventType int

const (
	PeerFound PeerEventType = iota
	PeerLost
)

const eventChanSize = 64 // absorbs bootstrap bursts without blocking the receive loop

// PeerEvent is emitted when a peer's reachability changes.
type PeerEvent struct {
	Type      PeerEventType
	PeerID    string
	Address   string
	Protocols []string
}

// Config holds discovery construction parameters.
type Config struct {
	NodeID         string
	BindAddr       string
	AdvertiseAddr  string // must be routable; defaults to BindAddr
	BootstrapAddrs []string
	PingInterval   time.Duration     // default: 1s
	PingMaxMissed  int               // default: 3
	MaxPeers       int               // max peer table size; 0 = unbounded. When full, evicts the least-live peer.
	Protocols      []string          // advertised in ANNOUNCE so peers know capabilities
	Codec          codec.Codec       // defaults to JSON; both sides must match
	Keypair        *identity.Keypair // when set, ANNOUNCE messages are signed
}

func (c *Config) setDefaults() {
	if c.PingInterval == 0 {
		c.PingInterval = 1 * time.Second
	}
	if c.PingMaxMissed == 0 {
		c.PingMaxMissed = 3
	}
	if c.Codec == nil {
		c.Codec = jsoncdc.New()
	}
}

func (c *Config) advertiseAddr() string {
	if c.AdvertiseAddr != "" {
		return c.AdvertiseAddr
	}
	return c.BindAddr
}

// Discovery manages UDP-based peer discovery.
type Discovery struct {
	cfg           Config
	tr            transport.PacketTransport
	table         *peerTable
	events        chan PeerEvent
	stopCh        chan struct{}
	wg            sync.WaitGroup
	pendingMu     sync.Mutex
	pending       map[string]string // nonce → nodeID
	pendingByPeer map[string]string // nodeID → nonce (at most one ping per peer)
}

func New(cfg Config, tr transport.PacketTransport) (*Discovery, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("discovery: NodeID is required")
	}
	cfg.setDefaults()
	if isUnspecifiedAddr(cfg.advertiseAddr()) {
		slog.Warn("discovery: AdvertiseAddr is unspecified — peers cannot dial this address; set WithAdvertiseAddr to a routable IP",
			"addr", cfg.advertiseAddr())
	}
	return &Discovery{
		cfg:           cfg,
		tr:            tr,
		table:         newPeerTable(cfg.MaxPeers),
		events:        make(chan PeerEvent, eventChanSize),
		stopCh:        make(chan struct{}),
		pending:       make(map[string]string),
		pendingByPeer: make(map[string]string),
	}, nil
}

func (d *Discovery) protocols() []string {
	if d.cfg.Protocols == nil {
		return nil // nil = unknown; callers treat conservatively
	}
	ps := make([]string, len(d.cfg.Protocols))
	copy(ps, d.cfg.Protocols)
	return ps
}

func (d *Discovery) Start(protocols []string) error {
	d.cfg.Protocols = protocols

	d.wg.Add(1)
	go d.receiveLoop()

	for _, addr := range d.cfg.BootstrapAddrs {
		d.sendAnnounce(addr)
		d.sendFindPeers(addr)
	}

	d.wg.Add(1)
	go d.livenessLoop()

	return nil
}

func (d *Discovery) Stop() error {
	close(d.stopCh)
	d.tr.Close()
	d.wg.Wait()
	return nil
}

func (d *Discovery) Events() <-chan PeerEvent {
	return d.events
}

// receiveLoop dispatches packets synchronously — each handler is O(1); goroutine-per-packet would be unbounded.
func (d *Discovery) receiveLoop() {
	defer d.wg.Done()
	for {
		fromAddr, data, err := d.tr.ReceiveFrom()
		if err != nil {
			select {
			case <-d.stopCh:
				return
			default:
				slog.Warn("discovery: receive error", "err", err)
				time.Sleep(5 * time.Millisecond)
				continue
			}
		}
		d.dispatch(fromAddr, data)
	}
}

func (d *Discovery) dispatch(fromAddr string, data []byte) {
	var msg inboundMsg
	if err := d.cfg.Codec.Decode(data, &msg); err != nil {
		slog.Debug("discovery: malformed packet, dropping", "from", fromAddr, "err", err)
		return
	}
	if msg.Type == "" {
		slog.Debug("discovery: packet missing type field, dropping", "from", fromAddr)
		return
	}
	switch msg.Type {
	case msgAnnounce:
		d.handleAnnounce(fromAddr, announceMsg{
			Type:      msg.Type,
			NodeID:    msg.NodeID,
			Address:   msg.Address,
			PublicKey: msg.PublicKey,
			Signature: msg.Signature,
			Protocols: msg.Protocols,
		})
	case msgFindPeers:
		d.handleFindPeers(fromAddr, findPeersMsg{Type: msg.Type, NodeID: msg.NodeID})
	case msgPeers:
		d.handlePeers(peersMsg{Type: msg.Type, Peers: msg.Peers})
	case msgPing:
		d.sendPong(fromAddr, msg.Nonce)
	case msgPong:
		d.handlePong(pongMsg{Type: msg.Type, NodeID: msg.NodeID, Nonce: msg.Nonce})
	}
}

// emitEvent is non-blocking; drops and logs if the channel is full.
func (d *Discovery) emitEvent(ev PeerEvent) {
	switch ev.Type {
	case PeerFound:
		slog.Info("discovery: peer found", "peer_id", ev.PeerID, "addr", ev.Address)
	case PeerLost:
		slog.Info("discovery: peer lost", "peer_id", ev.PeerID, "addr", ev.Address)
	}
	select {
	case d.events <- ev:
	default:
		slog.Warn("discovery: event channel full, dropping event", "type", ev.Type, "peer_id", ev.PeerID)
	}
}
