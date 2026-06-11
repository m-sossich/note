package identify

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"strings"
	"testing"
	"time"

	"crypto/ed25519"
	"crypto/rand"

	"github.com/m-sossich/note/pkg/node"
	"github.com/m-sossich/note/pkg/transport"
)

// mockCertConn implements transport.Conn + certConn for testing SecureHandshaker.
type mockCertConn struct {
	nopConn
	cert *x509.Certificate
}

func (m *mockCertConn) PeerCertificate() *x509.Certificate { return m.cert }

// nopConn satisfies transport.Conn with no-op implementations.
type nopConn struct{}

func (nopConn) Send([]byte) (int, error) { return 0, nil }
func (nopConn) Receive() ([]byte, error) { return nil, nil }
func (nopConn) RemoteAddr() string       { return "test:0" }
func (nopConn) Close() error             { return nil }

// plainConn does not implement certConn — used to test the error path.
type plainConn struct{ nopConn }

var _ transport.Conn = (*plainConn)(nil)

func makeCert(t *testing.T, cn string) *x509.Certificate {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}
	return cert
}

func TestSecureHandshaker_ReadsNodeIDFromCert(t *testing.T) {
	h := NewSecure(Config{Timeout: time.Second})
	cert := makeCert(t, "expected-node-id")
	conn := &mockCertConn{cert: cert}
	cfg := node.HandshakeConfig{}

	res, err := h.Initiate(conn, cfg)
	if err != nil {
		t.Fatalf("Initiate: %v", err)
	}
	if res.PeerID != "expected-node-id" {
		t.Errorf("PeerID = %q, want %q", res.PeerID, "expected-node-id")
	}

	res, err = h.Accept(conn, cfg)
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if res.PeerID != "expected-node-id" {
		t.Errorf("PeerID = %q, want %q", res.PeerID, "expected-node-id")
	}
}

func TestSecureHandshaker_ErrorWhenNotTLSConn(t *testing.T) {
	h := NewSecure(Config{Timeout: time.Second})
	conn := &plainConn{}
	cfg := node.HandshakeConfig{}

	_, err := h.Initiate(conn, cfg)
	if err == nil {
		t.Fatal("expected error for non-TLS connection, got nil")
	}
	if !strings.Contains(err.Error(), "pkg/transport/tls") {
		t.Errorf("error should mention pkg/transport/tls, got: %v", err)
	}
}

func TestSecureHandshaker_ErrorWhenNilCert(t *testing.T) {
	h := NewSecure(Config{Timeout: time.Second})
	conn := &mockCertConn{cert: nil}

	_, err := h.Initiate(conn, node.HandshakeConfig{})
	if err == nil {
		t.Fatal("expected error for nil certificate, got nil")
	}
}

func TestSecureHandshaker_ErrorWhenEmptyCN(t *testing.T) {
	h := NewSecure(Config{Timeout: time.Second})
	cert := makeCert(t, "") // empty CN
	conn := &mockCertConn{cert: cert}

	_, err := h.Initiate(conn, node.HandshakeConfig{})
	if err == nil {
		t.Fatal("expected error for empty certificate CN, got nil")
	}
}
