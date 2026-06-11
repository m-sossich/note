package identify

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	jsoncdc "github.com/m-sossich/note/pkg/codec/json"
	"github.com/m-sossich/note/pkg/node"
)

type pipeConn struct{ inner net.Conn }

func (c *pipeConn) Send(data []byte) (int, error) {
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(len(data)))
	if _, err := c.inner.Write(hdr); err != nil {
		return 0, err
	}
	return c.inner.Write(data)
}

func (c *pipeConn) Receive() ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(c.inner, hdr[:]); err != nil {
		return nil, err
	}
	buf := make([]byte, binary.BigEndian.Uint32(hdr[:]))
	_, err := io.ReadFull(c.inner, buf)
	return buf, err
}

func (c *pipeConn) RemoteAddr() string            { return "pipe" }
func (c *pipeConn) SetDeadline(t time.Time) error { return c.inner.SetDeadline(t) }
func (c *pipeConn) Close() error                  { return c.inner.Close() }

func newPipe() (*pipeConn, *pipeConn) {
	a, b := net.Pipe()
	return &pipeConn{a}, &pipeConn{b}
}

func cfg(nodeID string) node.HandshakeConfig {
	return node.HandshakeConfig{NodeID: nodeID, Codec: jsoncdc.New()}
}

// cfgWithPeer returns a HandshakeConfig with both local and remote peer IDs set.
// Used for outbound connections where the peer identity is known from discovery.
func cfgWithPeer(nodeID, peerID string) node.HandshakeConfig {
	return node.HandshakeConfig{NodeID: nodeID, Codec: jsoncdc.New(), PeerID: peerID}
}

func TestIdentify_Success(t *testing.T) {
	cA, cB := newPipe()
	h := New(Config{Timeout: 5 * time.Second})

	type result struct {
		res node.HandshakeResult
		err error
	}
	chA := make(chan result, 1)
	chB := make(chan result, 1)

	// Initiator (A) knows peer is node-B (from discovery). One-way: A sends HELLO.
	go func() {
		res, err := h.Initiate(cA, cfgWithPeer("node-A", "node-B"))
		chA <- result{res, err}
	}()
	// Acceptor (B) reads the HELLO and extracts A's NodeID. No ACK sent.
	go func() {
		res, err := h.Accept(cB, cfg("node-B"))
		chB <- result{res, err}
	}()

	rA := <-chA
	rB := <-chB

	if rA.err != nil {
		t.Fatalf("initiator error: %v", rA.err)
	}
	if rB.err != nil {
		t.Fatalf("acceptor error: %v", rB.err)
	}
	// Initiator returns the pre-known peer ID.
	if rA.res.PeerID != "node-B" {
		t.Errorf("initiator got peer %q, want node-B", rA.res.PeerID)
	}
	// Acceptor extracts the peer ID from the HELLO frame.
	if rB.res.PeerID != "node-A" {
		t.Errorf("acceptor got peer %q, want node-A", rB.res.PeerID)
	}
}

func TestIdentify_Timeout(t *testing.T) {
	_, cB := newPipe() // A never sends — B times out waiting
	h := New(Config{Timeout: 50 * time.Millisecond})
	_, err := h.Accept(cB, cfg("node-B"))
	if err == nil {
		t.Error("expected timeout error, got nil")
	}
}
