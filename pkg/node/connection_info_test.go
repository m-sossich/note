package node

import (
	"testing"
	"time"

	jsoncdc "github.com/m-sossich/note/pkg/codec/json"
	"github.com/m-sossich/note/pkg/discovery"
	"github.com/m-sossich/note/pkg/transport"
	tcptransport "github.com/m-sossich/note/pkg/transport/tcp"
	"github.com/m-sossich/note/pkg/wire"
)

func TestNode_ConnectionInfo_UnknownPeer(t *testing.T) {
	n, err := New(Config{
		NodeID:     "ci-unknown",
		ListenAddr: "127.0.0.1:19540",
		Transport:  tcptransport.New(0),
		Handshaker: &minHandshaker{},
		Codec:      jsoncdc.New(),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, ok := n.ConnectionInfo("no-such-peer")
	if ok {
		t.Error("ConnectionInfo for unknown peer should return false")
	}
}

func TestNode_ConnectionInfo_ConnectedPeer(t *testing.T) {
	connected := make(chan struct{}, 1)

	nA, err := New(Config{
		NodeID:     "ci-node-A",
		ListenAddr: "127.0.0.1:19541",
		Transport:  tcptransport.New(0),
		Handshaker: &minHandshaker{},
		Codec:      jsoncdc.New(),
		OnPeerConnected: func(string) {
			select {
			case connected <- struct{}{}:
			default:
			}
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := nA.Start(); err != nil {
		t.Fatal(err)
	}
	defer nA.Stop()

	cfg := Config{
		NodeID:     "ci-node-B",
		Transport:  tcptransport.New(0),
		Codec:      jsoncdc.New(),
		Handshaker: &minHandshaker{},
	}
	cfg.setDefaults()
	nB := &nodeImpl{
		cfg:           cfg,
		conns:         make(map[string]*connection),
		dialing:       make(map[string]struct{}),
		protocols:     make(map[string]ProtocolHandler),
		peerProtocols: make(map[string][]string),
		declaredAddrs: make(map[string]string),
		peerSem:       make(chan struct{}, cfg.MaxPeers), inboundGuard: make(chan struct{}, cfg.MaxInboundPeers), pendingSem: make(chan struct{}, cfg.MaxPendingPeers),
		stopCh: make(chan struct{}),
	}
	nB.connectToPeer("ci-node-A", "127.0.0.1:19541")

	select {
	case <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for connection")
	}

	info, ok := nA.ConnectionInfo("ci-node-B")
	if !ok {
		t.Fatal("ConnectionInfo should return true for connected peer")
	}
	if info.RemoteAddr == "" {
		t.Error("ConnectionInfo.RemoteAddr should be non-empty")
	}
	// DeclaredAddr is empty: nB connected inbound without a prior PeerFound event.
	if info.DeclaredAddr != "" {
		t.Errorf("DeclaredAddr should be empty for inbound peer with no PeerFound event, got %q", info.DeclaredAddr)
	}
}

// TestNode_ConnectionInfo_DeclaredAddr verifies that a PeerFound event populates
// DeclaredAddr in ConnectionInfo — so DHT routing table entries use the dialable
// announced address rather than the ephemeral transport source port (DHT-2).
func TestNode_ConnectionInfo_DeclaredAddr(t *testing.T) {
	const declaredAddr = "127.0.0.1:19547"

	target, err := New(Config{
		NodeID:     "ci-decl-target",
		ListenAddr: declaredAddr,
		Transport:  tcptransport.New(0),
		Handshaker: &minHandshaker{},
		Codec:      jsoncdc.New(),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Start(); err != nil {
		t.Fatal(err)
	}
	defer target.Stop()

	connected := make(chan struct{}, 1)
	peerSrc := &chanPeerSource{ch: make(chan discovery.PeerEvent, 1)}
	nA, err := New(Config{
		NodeID:     "ci-decl-A",
		ListenAddr: "127.0.0.1:19548",
		Transport:  tcptransport.New(0),
		Handshaker: &minHandshaker{},
		Codec:      jsoncdc.New(),
		OnPeerConnected: func(string) {
			select {
			case connected <- struct{}{}:
			default:
			}
		},
	}, peerSrc)
	if err != nil {
		t.Fatal(err)
	}
	if err := nA.Start(); err != nil {
		t.Fatal(err)
	}
	defer nA.Stop()

	peerSrc.ch <- discovery.PeerEvent{
		Type:    discovery.PeerFound,
		PeerID:  "ci-decl-target",
		Address: declaredAddr,
	}

	select {
	case <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for connection")
	}

	info, ok := nA.ConnectionInfo("ci-decl-target")
	if !ok {
		t.Fatal("ConnectionInfo should return true for connected peer")
	}
	if info.DeclaredAddr != declaredAddr {
		t.Errorf("DeclaredAddr = %q, want %q", info.DeclaredAddr, declaredAddr)
	}
}

// TestNode_ConnectionInfo_DeclaredAddr_ClearedOnPeerLost verifies that a PeerLost
// event removes the declared address so stale entries don't persist after eviction.
func TestNode_ConnectionInfo_DeclaredAddr_ClearedOnPeerLost(t *testing.T) {
	peerSrc := &chanPeerSource{ch: make(chan discovery.PeerEvent, 2)}
	cfg := Config{
		NodeID:     "ci-clear-A",
		ListenAddr: "127.0.0.1:19549",
		Transport:  tcptransport.New(0),
		Handshaker: &minHandshaker{},
		Codec:      jsoncdc.New(),
	}
	cfg.setDefaults()
	n := &nodeImpl{
		cfg:           cfg,
		disc:          peerSrc,
		conns:         make(map[string]*connection),
		dialing:       make(map[string]struct{}),
		protocols:     make(map[string]ProtocolHandler),
		peerProtocols: make(map[string][]string),
		declaredAddrs: make(map[string]string),
		peerSem:       make(chan struct{}, cfg.MaxPeers),
		inboundGuard:  make(chan struct{}, cfg.MaxInboundPeers),
		pendingSem:    make(chan struct{}, cfg.MaxPendingPeers),
		pendingConns:  make(map[transport.Conn]struct{}),
		stopCh:        make(chan struct{}),
	}

	// Simulate PeerFound — populates declaredAddrs.
	n.handlePeerFound(discovery.PeerEvent{
		Type:    discovery.PeerFound,
		PeerID:  "ci-clear-B",
		Address: "10.0.0.1:9000",
	})
	if got := n.declaredAddrs["ci-clear-B"]; got != "10.0.0.1:9000" {
		t.Fatalf("expected declared addr after PeerFound, got %q", got)
	}

	// Simulate PeerLost — must clear declaredAddrs.
	n.handlePeerLost(discovery.PeerEvent{
		Type:   discovery.PeerLost,
		PeerID: "ci-clear-B",
	})
	if got := n.declaredAddrs["ci-clear-B"]; got != "" {
		t.Errorf("expected empty declared addr after PeerLost, got %q", got)
	}
}

// TestNode_DisconnectPeer verifies that disconnectPeer sends a DISCONNECT frame
// and closes the underlying connection.
func TestNode_DisconnectPeer_SendsDisconnectFrame(t *testing.T) {
	server, client := pipePair()
	defer client.Close()

	cfg := Config{
		NodeID:     "dp-node",
		Transport:  tcptransport.New(0),
		Handshaker: &minHandshaker{},
		Codec:      jsoncdc.New(),
	}
	cfg.setDefaults()
	n := &nodeImpl{
		cfg:       cfg,
		conns:     make(map[string]*connection),
		dialing:   make(map[string]struct{}),
		protocols: make(map[string]ProtocolHandler),
		peerSem:   make(chan struct{}, cfg.MaxPeers), inboundGuard: make(chan struct{}, cfg.MaxInboundPeers), pendingSem: make(chan struct{}, cfg.MaxPendingPeers),
		stopCh: make(chan struct{}),
	}

	conn := newConnection(HandshakeResult{PeerID: "target-peer"}, server, jsoncdc.New())
	n.addConn(conn)

	// Read concurrently so sendDisconnect does not block on the synchronous pipe.
	frameCh := make(chan wire.Frame, 1)
	go func() {
		client.conn.SetDeadline(time.Now().Add(2 * time.Second))
		data, err := client.Receive()
		if err != nil {
			frameCh <- wire.Frame{}
			return
		}
		f, _ := wire.Decode(data)
		frameCh <- f
	}()

	n.disconnectPeer("target-peer")

	select {
	case f := <-frameCh:
		if f.Type != wire.TypeDisconnect {
			t.Fatalf("expected TypeDisconnect (0x03), got 0x%02x", f.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for DISCONNECT frame")
	}
}
