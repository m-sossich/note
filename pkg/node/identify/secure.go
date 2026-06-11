package identify

import (
	"crypto/ed25519"
	"crypto/x509"
	"fmt"
	"time"

	"github.com/m-sossich/note/pkg/p2p"
	"github.com/m-sossich/note/pkg/transport"
)

// SecureHandshaker is the verified-mode handshaker. Identity comes from the TLS cert CN.
// No IDENT frame (or any wire frame) is sent or read in either direction.
type SecureHandshaker struct {
	timeout time.Duration
}

// NewSecure returns a SecureHandshaker. Use with pkg/transport/tls.
func NewSecure(cfg Config) *SecureHandshaker {
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	return &SecureHandshaker{timeout: cfg.Timeout}
}

func (h *SecureHandshaker) Initiate(conn transport.Conn, cfg p2p.HandshakeConfig) (p2p.HandshakeResult, error) {
	setDeadline(conn, time.Now().Add(h.timeout))
	defer setDeadline(conn, time.Time{})
	return peerIDFromCert(conn)
}

func (h *SecureHandshaker) Accept(conn transport.Conn, cfg p2p.HandshakeConfig) (p2p.HandshakeResult, error) {
	setDeadline(conn, time.Now().Add(h.timeout))
	defer setDeadline(conn, time.Time{})
	return peerIDFromCert(conn)
}

type certConn interface {
	PeerCertificate() *x509.Certificate
}

// tlsHandshaker is optionally implemented by connections that support explicit handshake completion.
type tlsHandshaker interface {
	Handshake() error
}

func peerIDFromCert(conn transport.Conn) (p2p.HandshakeResult, error) {
	if hs, ok := conn.(tlsHandshaker); ok {
		if err := hs.Handshake(); err != nil {
			return p2p.HandshakeResult{}, fmt.Errorf("identify-secure: tls handshake: %w", err)
		}
	}
	cc, ok := conn.(certConn)
	if !ok {
		return p2p.HandshakeResult{}, fmt.Errorf("identify-secure: connection does not expose a peer certificate — use with pkg/transport/tls")
	}
	cert := cc.PeerCertificate()
	if cert == nil {
		return p2p.HandshakeResult{}, fmt.Errorf("identify-secure: no peer certificate in TLS state")
	}
	if cert.Subject.CommonName == "" {
		return p2p.HandshakeResult{}, fmt.Errorf("identify-secure: peer certificate has no Common Name")
	}
	// Extract Ed25519 public key for routing table verification without inspecting TLS state directly.
	var pubKey []byte
	if ed, ok := cert.PublicKey.(ed25519.PublicKey); ok {
		pubKey = []byte(ed)
	}
	return p2p.HandshakeResult{PeerID: cert.Subject.CommonName, PublicKey: pubKey}, nil
}
