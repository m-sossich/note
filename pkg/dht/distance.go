package dht

import (
	"crypto/sha256"
	"math/bits"
)

// DHTKey is a 256-bit Kademlia node identifier.
type DHTKey [32]byte

const (
	keySpaceBits = len(DHTKey{}) * 8
	bitsPerByte  = 8
)

func KeyFromString(s string) DHTKey { return sha256.Sum256([]byte(s)) }
func KeyFromBytes(b []byte) DHTKey  { return sha256.Sum256(b) }

// XOR returns the Kademlia distance between a and b.
func XOR(a, b DHTKey) DHTKey {
	var result DHTKey
	for i := range a {
		result[i] = a[i] ^ b[i]
	}
	return result
}

// CommonPrefixLen returns shared leading bits; determines which k-bucket a key maps to.
func CommonPrefixLen(a, b DHTKey) int {
	for i := range a {
		diff := a[i] ^ b[i]
		if diff != 0 {
			return i*bitsPerByte + bits.LeadingZeros8(diff)
		}
	}
	return keySpaceBits
}

// Less reports a < b as big-endian unsigned 256-bit integers.
func Less(a, b DHTKey) bool {
	for i := range a {
		if a[i] < b[i] {
			return true
		}
		if a[i] > b[i] {
			return false
		}
	}
	return false
}
