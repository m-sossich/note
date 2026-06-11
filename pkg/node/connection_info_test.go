package node

import (
	"testing"
	"time"

	jsoncdc "github.com/m-sossich/note/pkg/codec/json"
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
		cfg:       cfg,
		conns:     make(map[string]*connection),
		dialing:   make(map[string]struct{}),
		protocols: make(map[string]ProtocolHandler),
		peerSem:   make(chan struct{}, cfg.MaxPeers), inboundGuard: make(chan struct{}, cfg.MaxInboundPeers), pendingSem: make(chan struct{}, cfg.MaxPendingPeers),
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
