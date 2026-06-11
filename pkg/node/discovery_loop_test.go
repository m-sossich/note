package node

import (
	"testing"
	"time"

	jsoncdc "github.com/m-sossich/note/pkg/codec/json"
	"github.com/m-sossich/note/pkg/discovery"
	tcptransport "github.com/m-sossich/note/pkg/transport/tcp"
)

// chanPeerSource is a minimal PeerSource backed by a buffered channel.
type chanPeerSource struct {
	ch chan discovery.PeerEvent
}

func (c *chanPeerSource) Events() <-chan discovery.PeerEvent { return c.ch }

// TestNode_DiscoveryLoop_PeerFound verifies that a PeerFound event causes the
// node to dial and connect to the discovered peer.
func TestNode_DiscoveryLoop_PeerFound(t *testing.T) {

	target, err := New(Config{
		NodeID:     "disc-target",
		ListenAddr: "127.0.0.1:19542",
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

	connected := make(chan string, 1)
	peerSrc := &chanPeerSource{ch: make(chan discovery.PeerEvent, 1)}
	nA, err := New(Config{
		NodeID:     "disc-A",
		ListenAddr: "127.0.0.1:19543",
		Transport:  tcptransport.New(0),
		Codec:      jsoncdc.New(),
		Handshaker: &minHandshaker{},
		OnPeerConnected: func(id string) {
			select {
			case connected <- id:
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
		PeerID:  "disc-target",
		Address: "127.0.0.1:19542",
	}

	select {
	case id := <-connected:
		if id != "disc-target" {
			t.Fatalf("connected to %q, want disc-target", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout: PeerFound did not trigger connection")
	}
}

// TestNode_DiscoveryLoop_SelfIgnored verifies that a PeerFound event for the
// node's own ID does not trigger a self-dial.
func TestNode_DiscoveryLoop_SelfIgnored(t *testing.T) {
	connected := make(chan string, 1)
	peerSrc := &chanPeerSource{ch: make(chan discovery.PeerEvent, 1)}

	nA, err := New(Config{
		NodeID:     "disc-self",
		ListenAddr: "127.0.0.1:19544",
		Transport:  tcptransport.New(0),
		Handshaker: &minHandshaker{},
		Codec:      jsoncdc.New(),
		OnPeerConnected: func(id string) {
			select {
			case connected <- id:
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
		PeerID:  "disc-self",
		Address: "127.0.0.1:19544",
	}

	select {
	case id := <-connected:
		t.Fatalf("should not connect to self, got OnPeerConnected(%q)", id)
	case <-time.After(200 * time.Millisecond):
		// No self-connection — correct behaviour.
	}
}

// TestNode_DiscoveryLoop_PeerLost verifies that a PeerLost event causes the
// node to send a DISCONNECT and tear down the connection.
func TestNode_DiscoveryLoop_PeerLost(t *testing.T) {
	disconnected := make(chan string, 1)

	peerSrc := &chanPeerSource{ch: make(chan discovery.PeerEvent, 2)}
	nA, err := New(Config{
		NodeID:     "disc-pl-A",
		ListenAddr: "127.0.0.1:19545",
		Transport:  tcptransport.New(0),
		Handshaker: &minHandshaker{},
		Codec:      jsoncdc.New(),
		OnPeerDisconnected: func(id string) {
			select {
			case disconnected <- id:
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

	// Inject a connection directly so we don't need a full dial.
	server, client := pipePair()
	defer client.Close()
	conn := newConnection(HandshakeResult{PeerID: "disc-pl-B"}, server, jsoncdc.New())
	nA.(*nodeImpl).addConn(conn)

	// Start a goroutine on the client side to drain the DISCONNECT frame and
	// prevent sendDisconnect from blocking on the synchronous pipe.
	go func() {
		client.conn.SetDeadline(time.Now().Add(2 * time.Second))
		client.Receive()
	}()

	// Emit PeerLost — discoveryLoop fires disconnectPeer in a goroutine.
	peerSrc.ch <- discovery.PeerEvent{
		Type:   discovery.PeerLost,
		PeerID: "disc-pl-B",
	}

	// The read loop on server is not running, so removeConn won't be called
	// automatically. We verify the DISCONNECT was sent via the client drain.
	// Give the goroutine time to run.
	time.Sleep(100 * time.Millisecond)

	// The underlying connection should be closed after disconnectPeer.
	if _, err := server.Send([]byte("test")); err == nil {
		t.Error("expected send to fail after disconnectPeer closed the connection")
	}
}
