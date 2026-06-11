package node

import (
	"testing"
	"time"

	jsoncdc "github.com/m-sossich/note/pkg/codec/json"
	tcptransport "github.com/m-sossich/note/pkg/transport/tcp"
)

func TestNode_Peers(t *testing.T) {

	connected := make(chan string, 1)
	disconnected := make(chan string, 1)

	nodeA, err := New(Config{
		NodeID:             "node-A-peers",
		ListenAddr:         "127.0.0.1:19530",
		Transport:          tcptransport.New(0),
		Handshaker:         &minHandshaker{},
		Codec:              jsoncdc.New(),
		OnPeerConnected:    func(id string) { connected <- id },
		OnPeerDisconnected: func(id string) { disconnected <- id },
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := nodeA.Start(); err != nil {
		t.Fatal(err)
	}
	defer nodeA.Stop()

	if got := nodeA.Peers(); len(got) != 0 {
		t.Fatalf("expected 0 peers before connect, got %v", got)
	}

	cfg := Config{
		NodeID:     "node-B-peers",
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

	nodeB.connectToPeer("node-A-peers", "127.0.0.1:19530")

	select {
	case id := <-connected:
		if id != "node-B-peers" {
			t.Fatalf("OnPeerConnected: got %q, want %q", id, "node-B-peers")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for OnPeerConnected")
	}

	peers := nodeA.Peers()
	if len(peers) != 1 || peers[0] != "node-B-peers" {
		t.Fatalf("expected [node-B-peers] after connect, got %v", peers)
	}

	nodeB.mu.RLock()
	toClose := make([]*connection, 0, len(nodeB.conns))
	for _, c := range nodeB.conns {
		toClose = append(toClose, c)
	}
	nodeB.mu.RUnlock()
	for _, c := range toClose {
		c.Close()
	}

	select {
	case id := <-disconnected:
		if id != "node-B-peers" {
			t.Fatalf("OnPeerDisconnected: got %q, want %q", id, "node-B-peers")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for OnPeerDisconnected")
	}

	if got := nodeA.Peers(); len(got) != 0 {
		t.Fatalf("expected 0 peers after disconnect, got %v", got)
	}
}
