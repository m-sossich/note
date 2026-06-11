package protocol

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/m-sossich/note/pkg/node"
)

// Handler implements gossip/1.0. Every received message is forwarded to all
// peers except the one who sent it. A seen-message set prevents loops: a
// message whose ID has already been processed is silently dropped.
type Handler struct {
	nodeID    string
	n         node.Node
	seen      map[string]struct{}
	onReceive func(msg GossipMessage, senderID string)
	mu        sync.Mutex
}

// NewHandler creates a Handler and registers gossip/1.0 on n.
func NewHandler(n node.Node, nodeID string) *Handler {
	h := &Handler{
		nodeID: nodeID,
		n:      n,
		seen:   make(map[string]struct{}),
	}
	n.Register(Protocol, h.Handle)
	return h
}

// SetOnReceive registers a callback invoked for each new message — both
// self-published (via Publish) and received from the network. Safe to call
// before the first message arrives.
func (h *Handler) SetOnReceive(fn func(msg GossipMessage, senderID string)) {
	h.mu.Lock()
	h.onReceive = fn
	h.mu.Unlock()
}

// Publish originates a new message from this node and sends it to all current
// peers. The onReceive callback fires immediately for the local node so callers
// can treat self-published messages the same as received ones.
func (h *Handler) Publish(text string) GossipMessage {
	msg := GossipMessage{
		ID:       uuid.New().String(),
		OriginID: h.nodeID,
		Text:     text,
		Hops:     0,
	}
	h.mu.Lock()
	h.seen[msg.ID] = struct{}{}
	cb := h.onReceive
	h.mu.Unlock()

	if cb != nil {
		cb(msg, h.nodeID)
	}
	h.forward(msg, "" /* no sender to exclude */)
	return msg
}

// Handle dispatches incoming gossip/1.0 messages.
func (h *Handler) Handle(senderID, msgType string, decode func(any) error) error {
	if msgType != MsgGossip {
		return fmt.Errorf("gossip: unknown message type %q", msgType)
	}
	var msg GossipMessage
	if err := decode(&msg); err != nil {
		return fmt.Errorf("gossip: decode: %w", err)
	}

	h.mu.Lock()
	_, already := h.seen[msg.ID]
	if !already {
		h.seen[msg.ID] = struct{}{}
	}
	cb := h.onReceive
	h.mu.Unlock()

	if already {
		return nil // duplicate — stop propagation
	}

	if cb != nil {
		cb(msg, senderID)
	}

	// Relay to all peers except the one who sent it, incrementing the hop count.
	relay := msg
	relay.Hops++
	h.forward(relay, senderID)
	return nil
}

// forward sends msg to every connected peer except excludeID.
// The library provides no built-in selective send — we iterate Peers() and
// call Send() individually, skipping the excluded peer.
func (h *Handler) forward(msg GossipMessage, excludeID string) {
	for _, id := range h.n.Peers() {
		if id == excludeID {
			continue
		}
		h.n.Send(id, Protocol, MsgGossip, msg) //nolint:errcheck
	}
}
