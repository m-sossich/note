// Package p2p defines the shared vocabulary of peer-to-peer communication.
// These types are used across the node, DHT, and handshake layers — any
// implementation of the protocol in any language must satisfy these contracts.
package p2p

import (
	"github.com/m-sossich/note/pkg/codec"
	"github.com/m-sossich/note/pkg/transport"
)

// Handler is called once per received sub-protocol message.
type Handler func(peerID string, msgType string, decode func(any) error) error

// ConnInfo holds observable state of a connected peer.
type ConnInfo struct {
	RemoteAddr   string // transport-layer address; ephemeral for inbound connections
	DeclaredAddr string // peer's announced listening address from discovery; empty if unknown
	PublicKey    []byte // nil in trusted mode
}

// HandshakeConfig carries local identity for use at connection time.
type HandshakeConfig struct {
	NodeID string
	Codec  codec.Codec
	PeerID string // known remote identity for outbound; empty for inbound
}

// HandshakeResult carries the outcome of a completed handshake.
type HandshakeResult struct {
	PeerID    string
	PublicKey []byte // nil in trusted mode
}

// Handshaker performs the identity exchange on a new connection.
// Both sides must complete before any APPLICATION frames are exchanged.
type Handshaker interface {
	Initiate(conn transport.Conn, cfg HandshakeConfig) (HandshakeResult, error)
	Accept(conn transport.Conn, cfg HandshakeConfig) (HandshakeResult, error)
}
