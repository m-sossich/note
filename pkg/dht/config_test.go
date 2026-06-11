package dht

import (
	"testing"
	"time"
)

func TestConfig_SetDefaults_AllZero(t *testing.T) {
	cfg := Config{}
	cfg.setDefaults()

	if cfg.BucketSize != defaultBucketSize {
		t.Errorf("BucketSize = %d, want %d", cfg.BucketSize, defaultBucketSize)
	}
	if cfg.Alpha != defaultAlpha {
		t.Errorf("Alpha = %d, want %d", cfg.Alpha, defaultAlpha)
	}
	if cfg.RequestTimeout != defaultRequestTimeout {
		t.Errorf("RequestTimeout = %v, want %v", cfg.RequestTimeout, defaultRequestTimeout)
	}
}

func TestConfig_SetDefaults_PreservesNonZero(t *testing.T) {
	cfg := Config{BucketSize: 20, Alpha: 5, RequestTimeout: 30 * time.Second}
	cfg.setDefaults()

	if cfg.BucketSize != 20 {
		t.Errorf("BucketSize overwritten: got %d, want 20", cfg.BucketSize)
	}
	if cfg.Alpha != 5 {
		t.Errorf("Alpha overwritten: got %d, want 5", cfg.Alpha)
	}
	if cfg.RequestTimeout != 30*time.Second {
		t.Errorf("RequestTimeout overwritten: got %v, want 30s", cfg.RequestTimeout)
	}
}

func TestConfig_SetDefaults_PartialZero(t *testing.T) {
	cfg := Config{BucketSize: 16}
	cfg.setDefaults()

	if cfg.BucketSize != 16 {
		t.Errorf("BucketSize overwritten: got %d", cfg.BucketSize)
	}
	if cfg.Alpha != defaultAlpha {
		t.Errorf("Alpha not filled: got %d, want %d", cfg.Alpha, defaultAlpha)
	}
	if cfg.RequestTimeout != defaultRequestTimeout {
		t.Errorf("RequestTimeout not filled: got %v", cfg.RequestTimeout)
	}
}

// TestNew_Config_BucketSize verifies that a custom BucketSize is propagated
// to the routing table, limiting how many nodes each k-bucket holds.
func TestNew_Config_BucketSize(t *testing.T) {
	cfg := Config{BucketSize: 2}
	cfg.setDefaults()

	rt := newRoutingTable(KeyFromString("local"), cfg.BucketSize)
	if rt.bucketSize != 2 {
		t.Fatalf("routingTable.bucketSize = %d, want 2", rt.bucketSize)
	}

	// Add three nodes that all hash into the same CPL=0 bucket.
	// Force them to land in bucket 0 by flipping the MSB relative to local.
	local := KeyFromString("local")
	n1 := local
	n2 := local
	n3 := local
	// Flip the high bit to put all three in CPL=0 (first bit differs from local).
	flipMSB := func(k DHTKey) DHTKey {
		k[0] ^= 0x80
		return k
	}
	n1 = flipMSB(n1)
	n1[1] = 0x01
	n2 = flipMSB(n2)
	n2[1] = 0x02
	n3 = flipMSB(n3)
	n3[1] = 0x03

	rt.Update(NodeInfo{NodeID: "n1", Key: n1}, nopPing)
	rt.Update(NodeInfo{NodeID: "n2", Key: n2}, nopPing)
	// Third node: bucket is full (size=2), ping returns false → n1 evicted.
	rt.Update(NodeInfo{NodeID: "n3", Key: n3}, nopPing)

	all := rt.FindClosest(n1, 10)
	if len(all) > 2 {
		t.Errorf("bucket size 2 not enforced: got %d nodes in table", len(all))
	}
}
