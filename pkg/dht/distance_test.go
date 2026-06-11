package dht

import (
	"bytes"
	"testing"
)

func TestKeyFromString_Determinism(t *testing.T) {
	k1 := KeyFromString("hello")
	k2 := KeyFromString("hello")
	if k1 != k2 {
		t.Errorf("KeyFromString not deterministic: %x != %x", k1, k2)
	}
}

func TestKeyFromString_DifferentInputs(t *testing.T) {
	k1 := KeyFromString("hello")
	k2 := KeyFromString("world")
	if k1 == k2 {
		t.Error("different inputs produced the same key")
	}
}

func TestKeyFromBytes_Determinism(t *testing.T) {
	k1 := KeyFromBytes([]byte("hello"))
	k2 := KeyFromBytes([]byte("hello"))
	if k1 != k2 {
		t.Errorf("KeyFromBytes not deterministic: %x != %x", k1, k2)
	}
}

func TestKeyFromString_MatchesKeyFromBytes(t *testing.T) {
	k1 := KeyFromString("hello")
	k2 := KeyFromBytes([]byte("hello"))
	if k1 != k2 {
		t.Errorf("KeyFromString and KeyFromBytes disagree: %x vs %x", k1, k2)
	}
}

func TestXOR_Commutativity(t *testing.T) {
	a := KeyFromString("alpha")
	b := KeyFromString("beta")
	if XOR(a, b) != XOR(b, a) {
		t.Error("XOR is not commutative")
	}
}

func TestXOR_SelfIsZero(t *testing.T) {
	a := KeyFromString("alpha")
	result := XOR(a, a)
	var zero DHTKey
	if result != zero {
		t.Errorf("XOR(a, a) = %x, want zero", result)
	}
}

func TestXOR_KnownValue(t *testing.T) {
	var a, b DHTKey
	a[0] = 0xFF
	b[0] = 0x0F
	result := XOR(a, b)
	if result[0] != 0xF0 {
		t.Errorf("XOR byte 0 = %02x, want 0xF0", result[0])
	}
	// Remaining bytes should all be zero.
	for i := 1; i < 32; i++ {
		if result[i] != 0 {
			t.Errorf("XOR byte %d = %02x, want 0x00", i, result[i])
		}
	}
}

func TestCommonPrefixLen_AllDifferent(t *testing.T) {
	var a, b DHTKey
	// First bit differs: a[0] = 0x00, b[0] = 0x80 → CPL = 0.
	a[0] = 0x00
	b[0] = 0x80
	if got := CommonPrefixLen(a, b); got != 0 {
		t.Errorf("CommonPrefixLen = %d, want 0", got)
	}
}

func TestCommonPrefixLen_SameKeys(t *testing.T) {
	k := KeyFromString("equal")
	if got := CommonPrefixLen(k, k); got != 256 {
		t.Errorf("CommonPrefixLen(k,k) = %d, want 256", got)
	}
}

func TestCommonPrefixLen_PartialMatch(t *testing.T) {
	var a, b DHTKey
	// First byte identical, second byte differs in bit 3 from the top:
	// a[1] = 0b00010000, b[1] = 0b00001000 → differ at bit index 3 within byte 1
	// so CPL = 8 + 3 = 11.
	a[1] = 0b00010000
	b[1] = 0b00001000
	if got := CommonPrefixLen(a, b); got != 11 {
		t.Errorf("CommonPrefixLen = %d, want 11", got)
	}
}

func TestLess_Ordering(t *testing.T) {
	var small, large DHTKey
	small[0] = 0x00
	large[0] = 0xFF
	if !Less(small, large) {
		t.Error("Less(small, large) = false, want true")
	}
	if Less(large, small) {
		t.Error("Less(large, small) = true, want false")
	}
}

func TestLess_Equal(t *testing.T) {
	k := KeyFromString("same")
	if Less(k, k) {
		t.Error("Less(k, k) = true, want false")
	}
}

func TestLess_MultiByteOrdering(t *testing.T) {
	var a, b DHTKey
	// a < b: first bytes equal, second byte of a < second byte of b.
	a[0] = 0x01
	b[0] = 0x01
	a[1] = 0x00
	b[1] = 0x01
	if !Less(a, b) {
		t.Errorf("expected a < b")
	}
	if Less(b, a) {
		t.Errorf("expected !(b < a)")
	}
}

func TestXOR_ResultBytes(t *testing.T) {
	a := KeyFromString("nodeA")
	b := KeyFromString("nodeB")
	dist := XOR(a, b)
	// Verify that the XOR matches manual byte-by-byte computation.
	for i := range a {
		want := a[i] ^ b[i]
		if dist[i] != want {
			t.Errorf("XOR byte %d: got %02x, want %02x", i, dist[i], want)
		}
	}
	// Also verify bytes are accessible as a slice.
	_ = bytes.Equal(dist[:], dist[:])
}
