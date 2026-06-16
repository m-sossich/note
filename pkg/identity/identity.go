// Package identity manages Ed25519 keypairs for verified-mode nodes.
// A node's identity is SHA-256(public_key) — the ID is derived from and bound
// to the keypair, making it cryptographically unforgeable without the private key.
package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"time"
)

// Keypair holds an Ed25519 identity. NodeID is SHA-256(PublicKey). TLSCert CN equals NodeID.
type Keypair struct {
	PublicKey  ed25519.PublicKey
	PrivateKey ed25519.PrivateKey
	NodeID     string
	TLSCert    tls.Certificate
}

func NodeIDFrom(pub ed25519.PublicKey) string {
	h := sha256.Sum256(pub)
	return hex.EncodeToString(h[:])
}

func (kp *Keypair) Sign(data []byte) []byte {
	return ed25519.Sign(kp.PrivateKey, data)
}

func VerifySignature(pub ed25519.PublicKey, data, sig []byte) bool {
	return ed25519.Verify(pub, data, sig)
}

// ValidateNodeEntry returns true when pubKey is a valid Ed25519 key and
// SHA-256(pubKey) == nodeID. Use as dht.Config.EntryValidator in verified mode
// to reject routing entries that lack cryptographic proof of identity.
func ValidateNodeEntry(nodeID string, pubKey []byte) bool {
	if len(pubKey) != ed25519.PublicKeySize {
		return false
	}
	return NodeIDFrom(ed25519.PublicKey(pubKey)) == nodeID
}

func Generate() (*Keypair, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("identity: generate key: %w", err)
	}
	return build(pub, priv)
}

// LoadOrGenerate reads a keypair from path, generating and persisting one if absent (mode 0600).
func LoadOrGenerate(path string) (*Keypair, error) {
	if data, err := os.ReadFile(path); err == nil {
		kp, err := fromPEM(data)
		if err == nil {
			return kp, nil
		}
	}
	kp, err := Generate()
	if err != nil {
		return nil, err
	}
	if err := save(path, kp.PrivateKey); err != nil {
		return nil, fmt.Errorf("identity: persist to %s: %w", path, err)
	}
	return kp, nil
}

func build(pub ed25519.PublicKey, priv ed25519.PrivateKey) (*Keypair, error) {
	nodeID := NodeIDFrom(pub)
	cert, err := selfSignedCert(pub, priv, nodeID)
	if err != nil {
		return nil, err
	}
	return &Keypair{
		PublicKey:  pub,
		PrivateKey: priv,
		NodeID:     nodeID,
		TLSCert:    cert,
	}, nil
}

func fromPEM(data []byte) (*Keypair, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("identity: no PEM block")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("identity: parse key: %w", err)
	}
	priv, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("identity: expected ed25519 key, got %T", key)
	}
	return build(priv.Public().(ed25519.PublicKey), priv)
}

func save(path string, priv ed25519.PrivateKey) error {
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return fmt.Errorf("marshal key: %w", err)
	}
	return os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0600)
}

// selfSignedCert produces a self-signed X.509 cert with CN = nodeID.
func selfSignedCert(pub ed25519.PublicKey, priv ed25519.PrivateKey, nodeID string) (tls.Certificate, error) {
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: nodeID},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(100 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("identity: create cert: %w", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("identity: marshal key for cert: %w", err)
	}
	privPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER})
	return tls.X509KeyPair(certPEM, privPEM)
}
