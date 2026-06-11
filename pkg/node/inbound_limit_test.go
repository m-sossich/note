package node_test

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	jsoncdc "github.com/m-sossich/note/pkg/codec/json"
	"github.com/m-sossich/note/pkg/node"
	"github.com/m-sossich/note/pkg/node/identify"
	tcptransport "github.com/m-sossich/note/pkg/transport/tcp"
)

// TestNode_PendingPeerLimit verifies that connections beyond MaxPendingPeers
// are rejected at the TCP level before the handshake begins.
func TestNode_PendingPeerLimit(t *testing.T) {
	const limit = 3
	const total = 10

	var accepted atomic.Int32

	n, err := node.New(node.Config{
		NodeID:          "node-limit",
		ListenAddr:      "127.0.0.1:19520",
		Transport:       tcptransport.New(0),
		Handshaker:      identify.New(identify.Config{}),
		Codec:           jsoncdc.New(),
		MaxPendingPeers: limit,
		OnPeerConnected: func(string) { accepted.Add(1) },
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	// Open more raw TCP connections than the limit allows.
	// These are raw dials — no handshake — so they hold a semaphore slot
	// without completing, letting us saturate the limit cleanly.
	conns := make([]net.Conn, 0, total)
	for range total {
		c, err := net.Dial("tcp", "127.0.0.1:19520")
		if err != nil {
			// Connections beyond the limit are closed by the node immediately;
			// the dial itself may still succeed at the TCP level before the
			// remote close arrives, so we tolerate both outcomes.
			continue
		}
		conns = append(conns, c)
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()

	// Give the node time to accept, check the semaphore, and close excess.
	time.Sleep(150 * time.Millisecond)

	// Count how many connections the node is still holding open
	// by reading from each — rejected ones will return EOF immediately.
	alive := 0
	for _, c := range conns {
		c.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		buf := make([]byte, 1)
		_, err := c.Read(buf)
		if err == nil {
			alive++ // got data (HELLO frame started)
		} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			alive++ // no data yet but connection is still open
		}
		// io.EOF or connection reset = rejected
	}

	if alive > limit {
		t.Errorf("expected at most %d live connections, got %d", limit, alive)
	}
}
