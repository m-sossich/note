// Package identify implements node.Handshaker.
//
// Trusted mode (Handshaker): the initiator sends a single IDENT frame carrying
// its NodeID.
//
// Verified mode (SecureHandshaker): identity comes from the TLS certificate CN.
// No IDENT frame is sent or expected in either direction.
package identify

import (
	"fmt"
	"time"

	"github.com/m-sossich/note/pkg/p2p"
	"github.com/m-sossich/note/pkg/transport"
	"github.com/m-sossich/note/pkg/wire"
)

type identMsg struct {
	NodeID string
}

type Config struct {
	Timeout time.Duration // default: 10s
}

// Handshaker is the trusted-mode handshaker. Sends one IDENT frame; reads one on accept.
type Handshaker struct {
	timeout time.Duration
}

func New(cfg Config) *Handshaker {
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	return &Handshaker{timeout: cfg.Timeout}
}

func (h *Handshaker) Initiate(conn transport.Conn, cfg p2p.HandshakeConfig) (p2p.HandshakeResult, error) {
	setDeadline(conn, time.Now().Add(h.timeout))
	defer setDeadline(conn, time.Time{})

	payload, err := cfg.Codec.Encode(identMsg{NodeID: cfg.NodeID})
	if err != nil {
		return p2p.HandshakeResult{}, fmt.Errorf("identify: encode: %w", err)
	}
	if _, err := conn.Send(wire.Encode(wire.Frame{Type: wire.TypeIdent, Payload: payload})); err != nil {
		return p2p.HandshakeResult{}, fmt.Errorf("identify: send: %w", err)
	}
	return p2p.HandshakeResult{PeerID: cfg.PeerID}, nil
}

func (h *Handshaker) Accept(conn transport.Conn, cfg p2p.HandshakeConfig) (p2p.HandshakeResult, error) {
	setDeadline(conn, time.Now().Add(h.timeout))
	defer setDeadline(conn, time.Time{})

	frame, err := receiveFrame(conn)
	if err != nil {
		return p2p.HandshakeResult{}, fmt.Errorf("identify: receive: %w", err)
	}
	if frame.Type != wire.TypeIdent {
		return p2p.HandshakeResult{}, fmt.Errorf("identify: unexpected frame 0x%02x", frame.Type)
	}

	var req identMsg
	if err := cfg.Codec.Decode(frame.Payload, &req); err != nil {
		return p2p.HandshakeResult{}, fmt.Errorf("identify: decode: %w", err)
	}
	return p2p.HandshakeResult{PeerID: req.NodeID}, nil
}

func receiveFrame(conn transport.Conn) (wire.Frame, error) {
	data, err := conn.Receive()
	if err != nil {
		return wire.Frame{}, err
	}
	return wire.Decode(data)
}

func setDeadline(conn transport.Conn, t time.Time) {
	if dc, ok := conn.(transport.DeadlineConn); ok {
		dc.SetDeadline(t)
	}
}
