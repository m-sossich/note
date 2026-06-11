package protocol

import (
	"crypto/ed25519"
	"fmt"
	"log/slog"
	"sync"

	"github.com/m-sossich/note/pkg/node"
)

// Handler implements the chat/1.0 sub-protocol.
type Handler struct {
	room      string
	username  string
	privKey   ed25519.PrivateKey // nil in trusted mode
	getPubKey func(peerID string) []byte
	send      func(peerID, msgType string, payload any) error
	members   map[string]string               // peerID → username
	onMessage func(fromUsername, text string) // nil in normal usage; set in tests
	mu        sync.RWMutex
}

// NewHandler creates a Handler wired to n and registers the chat/1.0 protocol
// on it. privKey may be nil when running in trusted mode.
func NewHandler(n node.Node, room, username string, privKey ed25519.PrivateKey) *Handler {
	h := &Handler{
		room:     room,
		username: username,
		privKey:  privKey,
		getPubKey: func(peerID string) []byte {
			if info, ok := n.ConnectionInfo(peerID); ok {
				return info.PublicKey
			}
			return nil
		},
		send: func(peerID, msgType string, payload any) error {
			_, err := n.Send(peerID, Protocol, msgType, payload)
			return err
		},
		members: make(map[string]string),
	}
	n.Register(Protocol, h.Handle)
	return h
}

// OnConnect sends our ANNOUNCE to a newly connected peer. Wire this to
// note.WithPeerConnected so every joining peer learns our room and username.
func (h *Handler) OnConnect(peerID string) {
	var sig []byte
	if h.privKey != nil {
		sig = ed25519.Sign(h.privKey, announcePayload(h.room, h.username))
	}
	if err := h.send(peerID, MsgAnnounce, Announce{
		Room:      h.room,
		Username:  h.username,
		Signature: sig,
	}); err != nil {
		slog.Warn("send ANNOUNCE failed", "peer_id", peerID, "err", err)
	}
}

// OnDisconnect removes the peer from the room roster and prints a departure notice.
// Wire this to note.WithPeerDisconnected.
func (h *Handler) OnDisconnect(peerID string) {
	h.mu.Lock()
	name, inRoom := h.members[peerID]
	delete(h.members, peerID)
	h.mu.Unlock()
	if inRoom {
		fmt.Printf("--- %s left the room\n", name)
	}
}

// Handle dispatches incoming chat/1.0 messages.
func (h *Handler) Handle(peerID, msgType string, decode func(any) error) error {
	switch msgType {
	case MsgAnnounce:
		return h.handleAnnounce(peerID, decode)
	case MsgMessage:
		return h.handleMessage(peerID, decode)
	default:
		return fmt.Errorf("chat: unknown message type %q", msgType)
	}
}

// SetOnMessage registers a callback invoked for each MESSAGE received in the room.
// The first argument is the ANNOUNCE-verified display name; the second is the text.
// Intended for tests — leave nil in production.
func (h *Handler) SetOnMessage(fn func(fromUsername, text string)) {
	h.mu.Lock()
	h.onMessage = fn
	h.mu.Unlock()
}

// Members returns a snapshot of the current room roster (peerID → username).
func (h *Handler) Members() map[string]string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make(map[string]string, len(h.members))
	for k, v := range h.members {
		out[k] = v
	}
	return out
}

func (h *Handler) handleAnnounce(peerID string, decode func(any) error) error {
	var a Announce
	if err := decode(&a); err != nil {
		return fmt.Errorf("parse ANNOUNCE: %w", err)
	}
	if a.Room != h.room {
		return nil
	}
	if pub := h.getPubKey(peerID); len(pub) > 0 && len(a.Signature) > 0 {
		if !ed25519.Verify(pub, announcePayload(a.Room, a.Username), a.Signature) {
			slog.Warn("ANNOUNCE signature invalid — dropping", "peer_id", peerID, "claimed_username", a.Username)
			return fmt.Errorf("chat: ANNOUNCE from %s has invalid signature", peerID)
		}
	}
	h.mu.Lock()
	h.members[peerID] = a.Username
	h.mu.Unlock()
	fmt.Printf("--- %s joined the room\n", a.Username)
	return nil
}

func (h *Handler) handleMessage(peerID string, decode func(any) error) error {
	var msg ChatMessage
	if err := decode(&msg); err != nil {
		return fmt.Errorf("parse MESSAGE: %w", err)
	}
	if msg.Room != h.room {
		return nil
	}
	h.mu.RLock()
	name := h.members[peerID]
	cb := h.onMessage
	h.mu.RUnlock()
	if name == "" {
		name = truncateID(peerID)
	}
	fmt.Printf("[%s] %s\n", name, msg.Text)
	if cb != nil {
		cb(name, msg.Text)
	}
	return nil
}

// announcePayload is the canonical byte slice that the sender signs and the
// receiver verifies. Binding room+username together prevents a replay attack
// where a signature from one room is reused as a valid ANNOUNCE in another.
func announcePayload(room, username string) []byte {
	return []byte(room + ":" + username)
}

func truncateID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
