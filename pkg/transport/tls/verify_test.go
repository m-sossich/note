package tlstransport

import gotls "crypto/tls"

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"

	"github.com/m-sossich/note/pkg/identity"
)

func buildCert(t *testing.T, pub ed25519.PublicKey, priv ed25519.PrivateKey, cn string) []byte {
	t.Helper()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	return der
}

// TestVerifyP2PCert_ValidCert verifies that a self-signed Ed25519 cert with
// CN == SHA-256(public_key) passes verification.
func TestVerifyP2PCert_ValidCert(t *testing.T) {
	kp, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	der := buildCert(t, kp.PublicKey, kp.PrivateKey, kp.NodeID)
	if err := verifyP2PCert([][]byte{der}, nil); err != nil {
		t.Errorf("valid cert rejected: %v", err)
	}
}

// TestVerifyP2PCert_MismatchedCN verifies that a cert whose CN does not match
// SHA-256(public_key) is rejected — the core protection against identity fraud.
func TestVerifyP2PCert_MismatchedCN(t *testing.T) {
	kp, _ := identity.Generate()
	// Claim a different node ID in the CN — mismatches SHA-256(kp.PublicKey).
	der := buildCert(t, kp.PublicKey, kp.PrivateKey, "fraudulent-node-id")
	if err := verifyP2PCert([][]byte{der}, nil); err == nil {
		t.Error("expected rejection for mismatched CN, got nil error")
	}
}

// TestVerifyP2PCert_NoCert verifies that an empty certificate list is rejected.
func TestVerifyP2PCert_NoCert(t *testing.T) {
	if err := verifyP2PCert(nil, nil); err == nil {
		t.Error("expected rejection for empty cert list, got nil error")
	}
	if err := verifyP2PCert([][]byte{}, nil); err == nil {
		t.Error("expected rejection for empty cert list, got nil error")
	}
}

// TestVerifyP2PCert_InvalidDER verifies that a malformed cert DER is rejected.
func TestVerifyP2PCert_InvalidDER(t *testing.T) {
	if err := verifyP2PCert([][]byte{[]byte("not-a-cert")}, nil); err == nil {
		t.Error("expected rejection for invalid DER, got nil error")
	}
}

// TestVerifyP2PCert_NonEd25519Key verifies that a cert using a non-Ed25519 key
// (RSA) is rejected — only Ed25519 keys are valid P2P identities.
func TestVerifyP2PCert_NonEd25519Key(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "some-node-id"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &rsaKey.PublicKey, rsaKey)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	if err := verifyP2PCert([][]byte{der}, nil); err == nil {
		t.Error("expected rejection for non-Ed25519 cert, got nil error")
	}
}

// TestWithP2PVerification verifies that withP2PVerification sets InsecureSkipVerify
// and VerifyPeerCertificate, leaving other fields untouched.
func TestWithP2PVerification(t *testing.T) {
	cfg := withP2PVerification(&gotls.Config{})
	if !cfg.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify to be true")
	}
	if cfg.VerifyPeerCertificate == nil {
		t.Error("expected VerifyPeerCertificate to be set")
	}
}
