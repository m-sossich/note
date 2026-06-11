package node

import (
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	jsoncdc "github.com/m-sossich/note/pkg/codec/json"
	"github.com/m-sossich/note/pkg/transport"
	tcptransport "github.com/m-sossich/note/pkg/transport/tcp"
)

// TestNode_TwoNodesConnect verifies two nodes connect and node.Send delivers
// a message end-to-end through the full node.Send path including protocol
// validation and two-level envelope encoding.
func TestNode_TwoNodesConnect(t *testing.T) {

	// nodeB receives the message — its handler captures the payload.
	received := make(chan []byte, 1)
	connected := make(chan struct{}, 1)

	// nodeA listens; nodeB dials in.
	nodeAIface, err := New(Config{
		NodeID:          "conn-node-A",
		ListenAddr:      "127.0.0.1:19460",
		Transport:       tcptransport.New(0),
		Handshaker:      &minHandshaker{},
		Codec:           jsoncdc.New(),
		OnPeerConnected: func(string) { close(connected) },
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// nodeA registers "test/1.0" so the handshake intersection includes it.
	nodeAIface.Register("test/1.0", func(string, string, func(any) error) error { return nil })
	if err := nodeAIface.Start(); err != nil {
		t.Fatal(err)
	}
	defer nodeAIface.Stop()

	// nodeB is a bare struct. Register "test/1.0" before connecting so the
	// HELLO it sends includes the protocol — needed for handshake intersection.
	cfg := Config{
		NodeID:     "conn-node-B",
		Transport:  tcptransport.New(0),
		Codec:      jsoncdc.New(),
		Handshaker: &minHandshaker{},
	}
	cfg.setDefaults()
	nodeB := &nodeImpl{
		cfg:       cfg,
		conns:     make(map[string]*connection),
		dialing:   make(map[string]struct{}),
		protocols: make(map[string]ProtocolHandler),
		peerSem:   make(chan struct{}, cfg.MaxPeers), inboundGuard: make(chan struct{}, cfg.MaxInboundPeers), pendingSem: make(chan struct{}, cfg.MaxPendingPeers),
		stopCh: make(chan struct{}),
	}
	nodeB.Register("test/1.0", func(_ string, _ string, decode func(any) error) error {
		var raw json.RawMessage
		decode(&raw)
		received <- []byte(raw)
		return nil
	})

	nodeB.connectToPeer("conn-node-A", "127.0.0.1:19460")

	select {
	case <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout: nodeA did not see nodeB connect")
	}

	// nodeA.Send to nodeB — flows through node.Send with protocol validation
	// and two-level codec encoding.
	type msg struct{ Text string }
	if _, err := nodeAIface.Send("conn-node-B", "test/1.0", "TEXT", msg{Text: "hello"}); err != nil {
		t.Fatalf("node.Send: %v", err)
	}

	select {
	case got := <-received:
		if len(got) == 0 {
			t.Fatal("received empty payload")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout: nodeB handler did not receive message")
	}
}

// TestNode_StopInterruptsPendingHandshake verifies that Stop unblocks even when
// an inbound connection is stuck inside Handshaker.Accept before completing.
// Without pending-conn tracking, Stop would wait for the handshake timeout
// (default 10s). With it, Stop closes the raw connection immediately and
// wg.Wait returns as soon as the goroutine notices the closed connection.
func TestNode_StopInterruptsPendingHandshake(t *testing.T) {
	// hangHandshaker signals when Accept starts and then blocks on Receive()
	// indefinitely (no deadline). This simulates a peer that connects but
	// never sends a HELLO — the exact case where pending-conn tracking matters.
	type hangHandshaker struct {
		started chan struct{}
	}
	hh := &hangHandshaker{started: make(chan struct{})}

	nA, err := New(Config{
		NodeID:     "hang-A",
		ListenAddr: "127.0.0.1:19464",
		Transport:  tcptransport.New(0),
		Handshaker: handshakeFuncPair{
			initiate: func(conn transport.Conn, cfg HandshakeConfig) (HandshakeResult, error) {
				return HandshakeResult{PeerID: "peer"}, nil
			},
			accept: func(conn transport.Conn, cfg HandshakeConfig) (HandshakeResult, error) {
				close(hh.started) // signal that Accept has started
				conn.Receive()    // blocks until conn is closed by Stop
				return HandshakeResult{}, fmt.Errorf("connection closed during handshake")
			},
		},
		Codec: jsoncdc.New(),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := nA.Start(); err != nil {
		t.Fatal(err)
	}

	// Connect a raw TCP socket but never send a HELLO — triggers the hang.
	go func() {
		c, err := net.Dial("tcp", "127.0.0.1:19464")
		if err == nil {
			defer c.Close()
			// Do not send anything — Accept will block on Receive().
			time.Sleep(10 * time.Second)
		}
	}()

	// Wait until Accept has started (the goroutine is blocked in Receive).
	select {
	case <-hh.started:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for handshake to start")
	}

	// Stop must return promptly — not wait for the handshake timeout.
	stopped := make(chan struct{})
	go func() {
		nA.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop hung waiting for in-progress handshake — pending-conn tracking broken")
	}
}

// handshakeFuncPair adapts two functions to the Handshaker interface.
type handshakeFuncPair struct {
	initiate func(transport.Conn, HandshakeConfig) (HandshakeResult, error)
	accept   func(transport.Conn, HandshakeConfig) (HandshakeResult, error)
}

func (h handshakeFuncPair) Initiate(c transport.Conn, cfg HandshakeConfig) (HandshakeResult, error) {
	return h.initiate(c, cfg)
}
func (h handshakeFuncPair) Accept(c transport.Conn, cfg HandshakeConfig) (HandshakeResult, error) {
	return h.accept(c, cfg)
}

// TestNode_StopUnblocksReadLoop verifies that Stop does not deadlock when a
// connection is being set up concurrently. The original bug: Stop's conn
// snapshot ran before nA's addConn, so Stop closed nothing, then the read loop
// goroutine was started and nobody closed it — wg.Wait hung forever.
//
// The fix has two parts:
//  1. serveConn checks stopCh after addConn and closes the conn if Stop is
//     in progress, so no read loop goroutine is left unmanaged.
//  2. The test waits on nA's OnPeerConnected (not nB's). nA fires that callback
//     only after addConn, which guarantees Stop's snapshot will include the conn.
func TestNode_StopUnblocksReadLoop(t *testing.T) {
	// nA fires connected when it has fully registered the inbound connection
	// (addConn has run). Waiting on this — not on nB's callback — is the
	// key ordering guarantee: Stop will see the conn in its snapshot.
	connected := make(chan struct{}, 1)
	disconnected := make(chan string, 1)

	nA, err := New(Config{
		NodeID:             "stop-A",
		ListenAddr:         "127.0.0.1:19462",
		Transport:          tcptransport.New(0),
		Handshaker:         &minHandshaker{},
		Codec:              jsoncdc.New(),
		OnPeerConnected:    func(string) { connected <- struct{}{} },
		OnPeerDisconnected: func(id string) { disconnected <- id },
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := nA.Start(); err != nil {
		t.Fatal(err)
	}

	// nB is a minimal outbound-only connector — no listener, never Start()ed.
	// Only nA's lifecycle is under test here.
	cfg := Config{
		NodeID:     "stop-B",
		Transport:  tcptransport.New(0),
		Codec:      jsoncdc.New(),
		Handshaker: &minHandshaker{},
	}
	cfg.setDefaults()
	nB := &nodeImpl{
		cfg:          cfg,
		conns:        make(map[string]*connection),
		dialing:      make(map[string]struct{}),
		protocols:    make(map[string]ProtocolHandler),
		peerSem:      make(chan struct{}, cfg.MaxPeers),
		inboundGuard: make(chan struct{}, cfg.MaxInboundPeers),
		pendingSem:   make(chan struct{}, cfg.MaxPendingPeers),
		stopCh:       make(chan struct{}),
	}
	nB.connectToPeer("stop-A", "127.0.0.1:19462")

	// Block until nA confirms the connection is registered — not nB.
	// This is the event that proves addConn has run on nA's side.
	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("nA did not see nB connect")
	}

	nA.Stop()

	select {
	case id := <-disconnected:
		if id != "stop-B" {
			t.Fatalf("OnPeerDisconnected: got %q, want %q", id, "stop-B")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnPeerDisconnected not called after Stop")
	}
}
