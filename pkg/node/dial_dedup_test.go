package node

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jsoncdc "github.com/m-sossich/note/pkg/codec/json"
	tcptransport "github.com/m-sossich/note/pkg/transport/tcp"
)

// TestTryBeginDial verifies the deduplication logic in isolation.
func TestTryBeginDial(t *testing.T) {
	n := &nodeImpl{
		conns:   make(map[string]*connection),
		dialing: make(map[string]struct{}),
	}

	if !n.tryBeginDial("peer-A") {
		t.Fatal("first tryBeginDial should return true")
	}
	if n.tryBeginDial("peer-A") {
		t.Fatal("concurrent tryBeginDial for the same peer should return false")
	}
	if !n.tryBeginDial("peer-B") {
		t.Fatal("tryBeginDial for a different peer should return true")
	}

	n.endDial("peer-A")
	if !n.tryBeginDial("peer-A") {
		t.Fatal("tryBeginDial should return true again after endDial")
	}

	n.endDial("peer-A")
	n.conns["peer-A"] = &connection{}
	if n.tryBeginDial("peer-A") {
		t.Fatal("tryBeginDial should return false when the peer is already connected")
	}
}

// TestNode_DialDeduplication fires 10 concurrent connectToPeer calls for the
// same peer and asserts that exactly one connection reaches the remote node.
func TestNode_DialDeduplication(t *testing.T) {

	var accepted atomic.Int32
	nodeB, err := New(Config{
		NodeID:          "node-B-dd",
		ListenAddr:      "127.0.0.1:19510",
		Transport:       tcptransport.New(0),
		Handshaker:      &minHandshaker{},
		Codec:           jsoncdc.New(),
		OnPeerConnected: func(string) { accepted.Add(1) },
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := nodeB.Start(); err != nil {
		t.Fatal(err)
	}
	defer nodeB.Stop()

	cfg := Config{
		NodeID:     "node-A-dd",
		Transport:  tcptransport.New(0),
		Codec:      jsoncdc.New(),
		Handshaker: &minHandshaker{},
	}
	cfg.setDefaults()
	nodeA := &nodeImpl{
		cfg:       cfg,
		conns:     make(map[string]*connection),
		dialing:   make(map[string]struct{}),
		protocols: make(map[string]ProtocolHandler),
		stopCh:    make(chan struct{}),
	}

	const goroutines = 10
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			nodeA.connectToPeer("node-B-dd", "127.0.0.1:19510")
		}()
	}
	wg.Wait()
	time.Sleep(100 * time.Millisecond)

	if got := accepted.Load(); got != 1 {
		t.Errorf("expected exactly 1 connection on nodeB, got %d", got)
	}
}
