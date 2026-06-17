// Package tlstransport provides a TLS StreamTransport for verified-mode nodes.
// Both sides present self-signed Ed25519 certificates. The TLS layer proves
// key ownership; the certificate Common Name carries the peer's NodeID
// (SHA-256 of the public key) so it is readable after the handshake.
package tlstransport

import (
	"crypto/ed25519"
	gotls "crypto/tls"
	"crypto/x509"
	"fmt"
	"net"

	"github.com/m-sossich/note/pkg/identity"
	"github.com/m-sossich/note/pkg/transport"
)

const defaultMaxFrameSize uint32 = 64 * 1024 // matches tcp transport default

// Transport is a TLS StreamTransport with mutual auth and length-prefix framing.
type Transport struct {
	kp           *identity.Keypair
	maxFrameSize uint32
}

func New(kp *identity.Keypair, maxFrameSize uint32) *Transport {
	if maxFrameSize == 0 {
		maxFrameSize = defaultMaxFrameSize
	}
	return &Transport{kp: kp, maxFrameSize: maxFrameSize}
}

func (t *Transport) Dial(addr string) (transport.Conn, error) {
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("tls dial %s: %w", addr, err)
	}
	tlsConn := gotls.Client(raw, t.clientConfig())
	if err := tlsConn.Handshake(); err != nil {
		raw.Close()
		return nil, fmt.Errorf("tls handshake with %s: %w", addr, err)
	}
	return newConn(tlsConn, t.maxFrameSize), nil
}

func (t *Transport) Listen(addr string) (transport.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("tls listen %s: %w", addr, err)
	}
	return &listener{inner: ln, tr: t}, nil
}

func (t *Transport) Close() error { return nil }

func (t *Transport) clientConfig() *gotls.Config {
	return withP2PVerification(&gotls.Config{
		Certificates: []gotls.Certificate{t.kp.TLSCert},
	})
}

func (t *Transport) serverConfig() *gotls.Config {
	return withP2PVerification(&gotls.Config{
		Certificates: []gotls.Certificate{t.kp.TLSCert},
		ClientAuth:   gotls.RequireAnyClientCert,
	})
}

// withP2PVerification replaces CA chain validation with P2P identity verification:
// CN == SHA-256(Ed25519 pubkey). Peers use self-signed certs with no CA.
func withP2PVerification(cfg *gotls.Config) *gotls.Config {
	cfg.InsecureSkipVerify = true
	cfg.VerifyPeerCertificate = verifyP2PCert
	return cfg
}

type listener struct {
	inner net.Listener
	tr    *Transport
}

func (l *listener) Accept() (transport.Conn, error) {
	raw, err := l.inner.Accept()
	if err != nil {
		return nil, err
	}
	// Handshake is deferred (lazy on first Read/Write) so Accept stays non-blocking.
	return newConn(gotls.Server(raw, l.tr.serverConfig()), l.tr.maxFrameSize), nil
}

func (l *listener) Close() error   { return l.inner.Close() }
func (l *listener) Addr() net.Addr { return l.inner.Addr() }

// verifyP2PCert checks key is Ed25519 and CN == SHA-256(public_key).
// Key ownership and identity binding are verified atomically in the same handshake.
func verifyP2PCert(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	if len(rawCerts) == 0 {
		return fmt.Errorf("tls: no certificate presented")
	}
	cert, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return fmt.Errorf("tls: parse peer certificate: %w", err)
	}
	pub, ok := cert.PublicKey.(ed25519.PublicKey)
	if !ok {
		return fmt.Errorf("tls: peer certificate must use Ed25519, got %T", cert.PublicKey)
	}
	expected := identity.NodeIDFrom(pub)
	if cert.Subject.CommonName != expected {
		return fmt.Errorf("tls: peer cert CN %q does not match key hash %q", cert.Subject.CommonName, expected)
	}
	return nil
}
