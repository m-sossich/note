package identity_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/m-sossich/note/pkg/identity"
)

func TestGenerate(t *testing.T) {
	kp, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if kp.NodeID == "" {
		t.Fatal("NodeID is empty")
	}
	if len(kp.PublicKey) == 0 {
		t.Fatal("PublicKey is empty")
	}
	if got := identity.NodeIDFrom(kp.PublicKey); got != kp.NodeID {
		t.Errorf("NodeIDFrom(PublicKey) = %q, want %q", got, kp.NodeID)
	}
}

func TestLoadOrGenerate_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.key")

	kp1, err := identity.LoadOrGenerate(path)
	if err != nil {
		t.Fatalf("first LoadOrGenerate: %v", err)
	}

	kp2, err := identity.LoadOrGenerate(path)
	if err != nil {
		t.Fatalf("second LoadOrGenerate: %v", err)
	}

	if kp1.NodeID != kp2.NodeID {
		t.Errorf("NodeID changed across loads: %q != %q", kp1.NodeID, kp2.NodeID)
	}
}

func TestLoadOrGenerate_FilePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.key")
	if _, err := identity.LoadOrGenerate(path); err != nil {
		t.Fatalf("LoadOrGenerate: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("key file permissions = %o, want 0600", perm)
	}
}

func TestNodeIDIsLowercaseHex(t *testing.T) {
	kp, _ := identity.Generate()
	for i, c := range kp.NodeID {
		if c >= 'A' && c <= 'F' {
			t.Errorf("NodeID[%d] = %c: must be lowercase hex", i, c)
		}
	}
	if len(kp.NodeID) != 64 {
		t.Errorf("NodeID length = %d, want 64 (32 bytes hex)", len(kp.NodeID))
	}
}

func TestSign_RoundTrip(t *testing.T) {
	kp, _ := identity.Generate()
	data := []byte("nodeID|10.0.0.1:9000")
	sig := kp.Sign(data)
	if len(sig) == 0 {
		t.Fatal("Sign returned empty signature")
	}
	if !identity.VerifySignature(kp.PublicKey, data, sig) {
		t.Error("VerifySignature returned false for a valid signature")
	}
}

func TestVerifySignature_RejectsWrongData(t *testing.T) {
	kp, _ := identity.Generate()
	sig := kp.Sign([]byte("original data"))
	if identity.VerifySignature(kp.PublicKey, []byte("tampered data"), sig) {
		t.Error("VerifySignature should return false for mismatched data")
	}
}

func TestVerifySignature_RejectsWrongKey(t *testing.T) {
	kpA, _ := identity.Generate()
	kpB, _ := identity.Generate()
	sig := kpA.Sign([]byte("some data"))
	if identity.VerifySignature(kpB.PublicKey, []byte("some data"), sig) {
		t.Error("VerifySignature should return false when key doesn't match signer")
	}
}
