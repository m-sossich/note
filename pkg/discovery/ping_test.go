package discovery

import (
	"encoding/json"
	"sync"
	"testing"
)

// captureSendTransport records every SendTo call for later inspection.
type captureSendTransport struct {
	mu      sync.Mutex
	sent    []capturedPacket
	closed  chan struct{}
	packets chan fakePacket
}

type capturedPacket struct {
	addr string
	data []byte
}

func newCaptureTransport() *captureSendTransport {
	return &captureSendTransport{
		closed:  make(chan struct{}),
		packets: make(chan fakePacket, 8),
	}
}

func (c *captureSendTransport) SendTo(addr string, data []byte) error {
	c.mu.Lock()
	cp := make([]byte, len(data))
	copy(cp, data)
	c.sent = append(c.sent, capturedPacket{addr: addr, data: cp})
	c.mu.Unlock()
	return nil
}

func (c *captureSendTransport) Close() error { close(c.closed); return nil }

func (c *captureSendTransport) ReceiveFrom() (string, []byte, error) {
	select {
	case p := <-c.packets:
		return p.addr, p.data, nil
	case <-c.closed:
		return "", nil, errClosed
	}
}

func (c *captureSendTransport) lastSent() (capturedPacket, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sent) == 0 {
		return capturedPacket{}, false
	}
	return c.sent[len(c.sent)-1], true
}

// errClosed is a sentinel error for closed transport.
var errClosed = &closedErr{}

type closedErr struct{}

func (e *closedErr) Error() string { return "transport closed" }

// newDiscovery returns a Discovery wired to a captureSendTransport for unit tests.
func newDiscovery(cfg Config, tr *captureSendTransport) *Discovery {
	cfg.setDefaults() // ensures cfg.Codec is set
	return &Discovery{
		cfg:           cfg,
		tr:            tr,
		table:         newPeerTable(0),
		events:        make(chan PeerEvent, 16),
		stopCh:        make(chan struct{}),
		pending:       make(map[string]string),
		pendingByPeer: make(map[string]string),
	}
}

// TestDispatch_PingPath_PingTest verifies PING → PONG from the ping-specific
// transport helper. This is the ping-layer integration counterpart to the
// dispatch-level ping test in dispatch_test.go.
func TestDispatch_PingPath_PingTest(t *testing.T) {
	tr := newCaptureTransport()
	d := newDiscovery(Config{NodeID: "pong-node"}, tr)

	ping, err := marshalMsg(newPingMsg("remote-peer", "test-nonce-abc"), testCodec())
	if err != nil {
		t.Fatalf("marshal ping: %v", err)
	}

	d.dispatch("10.0.0.1:9001", ping)

	pkt, ok := tr.lastSent()
	if !ok {
		t.Fatal("expected a PONG to be sent, got nothing")
	}
	if pkt.addr != "10.0.0.1:9001" {
		t.Errorf("PONG sent to %q, want 10.0.0.1:9001", pkt.addr)
	}
	var msg pongMsg
	if err := json.Unmarshal(pkt.data, &msg); err != nil {
		t.Fatalf("unmarshal PONG: %v", err)
	}
	if msg.Type != msgPong {
		t.Errorf("message type = %q, want %q", msg.Type, msgPong)
	}
	if msg.Nonce != "test-nonce-abc" {
		t.Errorf("PONG nonce = %q, want test-nonce-abc", msg.Nonce)
	}
}
