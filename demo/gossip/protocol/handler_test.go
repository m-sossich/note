package protocol

// Unit tests for the gossip handler. The E2E test covers end-to-end
// convergence in a full mesh. These tests cover the logic that the E2E
// test cannot reach:
//
//   - Relay: a received message is forwarded to all peers EXCEPT the sender.
//     In a full mesh every node is a direct peer of the origin, so the relay
//     path is never exercised at the integration level.
//
//   - Deduplication: a message seen twice is processed once.
//
//   - Publish: the originating node marks the message as seen so an incoming
//     copy is dropped and not re-forwarded.

import (
	"sync"
	"testing"

	"github.com/m-sossich/note/pkg/node"
)

// ---------------------------------------------------------------------------
// Stub node
// ---------------------------------------------------------------------------

type sentMsg struct {
	peerID  string
	msgType string
	payload any
}

type stubNode struct {
	mu      sync.Mutex
	peers   []string
	sent    []sentMsg
	sendErr error
}

func (n *stubNode) Send(peerID, _, msgType string, payload any) (int, error) {
	n.mu.Lock()
	n.sent = append(n.sent, sentMsg{peerID, msgType, payload})
	n.mu.Unlock()
	return 0, n.sendErr
}

func (n *stubNode) Register(_ string, _ node.ProtocolHandler) {}
func (n *stubNode) Start() error                              { return nil }
func (n *stubNode) Stop() error                               { return nil }
func (n *stubNode) Peers() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.peers...)
}
func (n *stubNode) ConnectionInfo(_ string) (node.ConnInfo, bool) { return node.ConnInfo{}, false }
func (n *stubNode) BoundAddr() string                             { return "" }
func (n *stubNode) PeerProtocols(_ string) ([]string, bool)       { return nil, false }
func (n *stubNode) RegisteredProtocols() []string                 { return nil }

func (n *stubNode) sentTo() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	var ids []string
	for _, m := range n.sent {
		ids = append(ids, m.peerID)
	}
	return ids
}

func (n *stubNode) sentCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.sent)
}

func invoke(h *Handler, senderID string, msg GossipMessage) error {
	return h.Handle(senderID, MsgGossip, func(v any) error {
		m := v.(*GossipMessage)
		*m = msg
		return nil
	})
}

// ---------------------------------------------------------------------------
// TestHandler_Relay
// ---------------------------------------------------------------------------

// TestHandler_Relay verifies that a received message is forwarded to all
// connected peers EXCEPT the one that sent it. This is the core gossip
// invariant and the path that the E2E integration test never reaches because
// in a full-mesh network the origin is always a direct peer of every receiver.
func TestHandler_Relay(t *testing.T) {
	n := &stubNode{peers: []string{"hub", "charlie"}}
	h := NewHandler(n, "me")

	msg := GossipMessage{ID: "id1", OriginID: "alice", Text: "hello", Hops: 0}
	if err := invoke(h, "alice", msg); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	sent := n.sentTo()
	if len(sent) != 2 {
		t.Fatalf("expected 2 forwards (to hub and charlie), got %d: %v", len(sent), sent)
	}
	for _, id := range sent {
		if id == "alice" {
			t.Error("message was forwarded back to the sender — relay loop risk")
		}
	}

	// Hop count must be incremented on each relay.
	n.mu.Lock()
	for _, m := range n.sent {
		forwarded, ok := m.payload.(GossipMessage)
		if !ok {
			t.Error("forwarded payload is not a GossipMessage")
			continue
		}
		if forwarded.Hops != 1 {
			t.Errorf("expected hops=1 after one relay, got %d", forwarded.Hops)
		}
	}
	n.mu.Unlock()
}

// ---------------------------------------------------------------------------
// TestHandler_Deduplication
// ---------------------------------------------------------------------------

// TestHandler_Deduplication verifies that a message received twice is
// processed exactly once. The second delivery must be dropped without
// firing the callback or forwarding.
func TestHandler_Deduplication(t *testing.T) {
	n := &stubNode{peers: []string{"peer1", "peer2"}}
	h := NewHandler(n, "me")

	var received int
	h.SetOnReceive(func(_ GossipMessage, _ string) { received++ })

	msg := GossipMessage{ID: "dup", OriginID: "alice", Text: "once"}

	if err := invoke(h, "peer1", msg); err != nil {
		t.Fatalf("first Handle: %v", err)
	}
	if err := invoke(h, "peer2", msg); err != nil {
		t.Fatalf("second Handle: %v", err)
	}

	if received != 1 {
		t.Errorf("onReceive fired %d times, expected 1", received)
	}
	// First delivery from peer1: forward to peer2 (skip sender peer1) = 1 send.
	// Second delivery from peer2: dropped = 0 sends. Total = 1.
	if n.sentCount() != 1 {
		t.Errorf("expected 1 forward after first delivery only, got %d", n.sentCount())
	}
}

// ---------------------------------------------------------------------------
// TestHandler_PublishMarksSeen
// ---------------------------------------------------------------------------

// TestHandler_PublishMarksSeen verifies that after a node publishes a message,
// an incoming copy of the same message ID is silently dropped. Without this,
// the message would be re-forwarded, creating a loop.
func TestHandler_PublishMarksSeen(t *testing.T) {
	n := &stubNode{peers: []string{"peer1"}}
	h := NewHandler(n, "me")

	var received int
	h.SetOnReceive(func(_ GossipMessage, _ string) { received++ })

	pub := h.Publish("hello world")

	// Simulate a peer forwarding our own message back to us.
	echo := pub
	echo.Hops = 1
	if err := invoke(h, "peer1", echo); err != nil {
		t.Fatalf("Handle echo: %v", err)
	}

	// onReceive should have fired once (from Publish), not twice.
	if received != 1 {
		t.Errorf("onReceive fired %d times after publish + echo, expected 1", received)
	}
	// No additional forwards should have happened for the echo.
	// Publish sends to peer1 (1 send). Echo should be dropped (no send).
	if n.sentCount() != 1 {
		t.Errorf("expected 1 send (from Publish), got %d (echo was re-forwarded?)", n.sentCount())
	}
}
