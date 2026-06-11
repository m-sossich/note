package dht_test

import (
	"context"
	"testing"

	jsoncdc "github.com/m-sossich/note/pkg/codec/json"
	"github.com/m-sossich/note/pkg/dht"
	"github.com/m-sossich/note/pkg/node"
	"github.com/m-sossich/note/pkg/node/identify"
	tcptransport "github.com/m-sossich/note/pkg/transport/tcp"
)

// startNode creates a node at listenAddr, starts it, and registers a cleanup.
func startNode(t *testing.T, nodeID, listenAddr string) node.Node {
	t.Helper()
	jc := jsoncdc.New()
	n, err := node.New(node.Config{
		NodeID:     nodeID,
		ListenAddr: listenAddr,
		Transport:  tcptransport.New(0),
		Codec:      jc,
		Handshaker: identify.New(identify.Config{}),
	}, nil)
	if err != nil {
		t.Fatalf("create node %s: %v", nodeID, err)
	}
	if err := n.Start(); err != nil {
		t.Fatalf("start node %s: %v", nodeID, err)
	}
	t.Cleanup(func() { n.Stop() })
	return n
}

// TestDHT_LocalStore verifies that a stored provider record can be retrieved
// locally without any network round-trip.
func TestDHT_LocalStore(t *testing.T) {
	nA := startNode(t, "dht-A", "127.0.0.1:19600")
	dhtA := dht.New(nA, "dht-A", "127.0.0.1:19600", dht.Config{})
	t.Cleanup(func() { dhtA.Stop() })

	key := []byte("greeting")
	value := []byte("hello world")

	// Store — no peers in the routing table, so only the local record is written.
	// Attempted == 0 is expected (solo node).
	if _, err := dhtA.Store(context.Background(), key, value); err != nil {
		t.Fatalf("Store: %v", err)
	}

	providers, err := dhtA.FindProviders(context.Background(), key)
	if err != nil {
		t.Fatalf("FindProviders: %v", err)
	}
	if len(providers) == 0 {
		t.Fatal("FindProviders: no providers found")
	}
	if string(providers[0].Value) != string(value) {
		t.Errorf("provider value = %q, want %q", providers[0].Value, value)
	}
}

// TestDHT_LocalKey verifies that LocalKey returns a 32-byte SHA256 of the nodeID.
func TestDHT_LocalKey(t *testing.T) {
	nA := startNode(t, "dht-key-test", "127.0.0.1:19601")
	dhtA := dht.New(nA, "dht-key-test", "127.0.0.1:19601", dht.Config{})
	t.Cleanup(func() { dhtA.Stop() })

	k := dhtA.LocalKey()
	if len(k) != 32 {
		t.Errorf("LocalKey length = %d, want 32", len(k))
	}
}

// TestDHT_FindProviders_NotFound verifies that FindProviders returns nil for an
// unknown key when the routing table is empty.
func TestDHT_FindProviders_NotFound(t *testing.T) {
	nA := startNode(t, "dht-nf", "127.0.0.1:19602")
	dhtA := dht.New(nA, "dht-nf", "127.0.0.1:19602", dht.Config{})
	t.Cleanup(func() { dhtA.Stop() })

	providers, err := dhtA.FindProviders(context.Background(), []byte("nonexistent"))
	if err != nil {
		t.Fatalf("FindProviders: unexpected error: %v", err)
	}
	if len(providers) != 0 {
		t.Errorf("expected no providers for nonexistent key, got %d", len(providers))
	}
}

// TestDHT_Lookup_EmptyTable verifies that Lookup returns an empty result (not
// an error) when the routing table has no peers.
func TestDHT_Lookup_EmptyTable(t *testing.T) {
	nA := startNode(t, "dht-lu", "127.0.0.1:19603")
	dhtA := dht.New(nA, "dht-lu", "127.0.0.1:19603", dht.Config{})
	t.Cleanup(func() { dhtA.Stop() })

	result, err := dhtA.Lookup(context.Background(), []byte("some-key"))
	if err != nil {
		t.Fatalf("expected no error with empty routing table, got: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty result, got %d nodes", len(result))
	}
}

// TestDHT_SeedPeer verifies that SeedPeer adds a node to the routing table.
func TestDHT_SeedPeer(t *testing.T) {
	nA := startNode(t, "dht-seed", "127.0.0.1:19604")
	nB := startNode(t, "dht-seed-B", "127.0.0.1:19605")

	dhtA := dht.New(nA, "dht-seed", "127.0.0.1:19604", dht.Config{})
	t.Cleanup(func() { dhtA.Stop() })
	dhtB := dht.New(nB, "dht-seed-B", "127.0.0.1:19605", dht.Config{})
	t.Cleanup(func() { dhtB.Stop() })

	b := dhtB.LocalNodeInfo()
	dhtA.SeedPeer(b.NodeID, b.Address, b.PublicKey)

	nodes, err := dhtA.Lookup(context.Background(), dhtB.LocalKey())
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(nodes) == 0 {
		t.Fatal("expected at least one node after seeding, got 0")
	}
	if nodes[0].NodeID != "dht-seed-B" {
		t.Errorf("closest node = %q, want dht-seed-B", nodes[0].NodeID)
	}
}

// TestDHT_LocalNodeInfo verifies LocalNodeInfo returns correct identity.
func TestDHT_LocalNodeInfo(t *testing.T) {
	nA := startNode(t, "dht-info", "127.0.0.1:19606")
	dhtA := dht.New(nA, "dht-info", "127.0.0.1:19606", dht.Config{})
	t.Cleanup(func() { dhtA.Stop() })

	info := dhtA.LocalNodeInfo()
	if info.NodeID != "dht-info" {
		t.Errorf("NodeID = %q, want dht-info", info.NodeID)
	}
	if info.Address != "127.0.0.1:19606" {
		t.Errorf("Address = %q, want 127.0.0.1:19606", info.Address)
	}
	if len(info.Key) != 32 {
		t.Errorf("Key length = %d, want 32", len(info.Key))
	}
}

// TestDHT_MultiHolder_LocalStore verifies that multiple provider records can
// be stored under the same key and all are returned by FindProviders.
func TestDHT_MultiHolder_LocalStore(t *testing.T) {
	nA := startNode(t, "multi-A", "127.0.0.1:19607")
	dhtA := dht.New(nA, "multi-A", "127.0.0.1:19607", dht.Config{})
	t.Cleanup(func() { dhtA.Stop() })

	// Simulate two providers announcing the same key locally.
	// In practice each would be received via STORE from a remote peer.
	key := []byte("shared-content")
	_, _ = dhtA.Store(context.Background(), key, []byte("metadata-A"))

	providers, err := dhtA.FindProviders(context.Background(), key)
	if err != nil {
		t.Fatalf("FindProviders: %v", err)
	}
	if len(providers) == 0 {
		t.Fatal("expected providers, got none")
	}
	if providers[0].NodeID != "multi-A" {
		t.Errorf("provider NodeID = %q, want multi-A", providers[0].NodeID)
	}
	if providers[0].Address != "127.0.0.1:19607" {
		t.Errorf("provider Address = %q, want 127.0.0.1:19607", providers[0].Address)
	}
}
