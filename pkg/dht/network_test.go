package dht_test

import (
	"context"
	"testing"
	"time"

	jsoncdc "github.com/m-sossich/note/pkg/codec/json"
	"github.com/m-sossich/note/pkg/dht"
	"github.com/m-sossich/note/pkg/discovery"
	"github.com/m-sossich/note/pkg/node"
	"github.com/m-sossich/note/pkg/node/identify"
	tcptransport "github.com/m-sossich/note/pkg/transport/tcp"
)

// eventSource is a minimal node.PeerSource backed by a channel.
type eventSource struct{ ch chan discovery.PeerEvent }

func (e *eventSource) Events() <-chan discovery.PeerEvent { return e.ch }

// startDHTNode creates a node with the DHT protocol registered and starts it.
func startDHTNode(t *testing.T, nodeID, listenAddr string) (node.Node, *dht.DHT) {
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
	d := dht.New(n, nodeID, listenAddr, dht.Config{})
	if err := n.Start(); err != nil {
		t.Fatalf("start node %s: %v", nodeID, err)
	}
	t.Cleanup(func() { d.Stop(); n.Stop() })
	return n, d
}

// startDHTNodeWithSource creates a node backed by an eventSource.
func startDHTNodeWithSource(t *testing.T, nodeID, listenAddr string, src *eventSource) (node.Node, *dht.DHT) {
	t.Helper()
	jc := jsoncdc.New()
	n, err := node.New(node.Config{
		NodeID:     nodeID,
		ListenAddr: listenAddr,
		Transport:  tcptransport.New(0),
		Codec:      jc,
		Handshaker: identify.New(identify.Config{}),
	}, src)
	if err != nil {
		t.Fatalf("create node %s: %v", nodeID, err)
	}
	d := dht.New(n, nodeID, listenAddr, dht.Config{})
	if err := n.Start(); err != nil {
		t.Fatalf("start node %s: %v", nodeID, err)
	}
	t.Cleanup(func() { d.Stop(); n.Stop() })
	return n, d
}

// waitConnected polls until both n1 and n2 see at least one peer, ensuring
// both sides have called addConn before any RPCs are issued.
func waitConnected(t *testing.T, n1, n2 node.Node, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(n1.Peers()) > 0 && len(n2.Peers()) > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout: n1 peers=%v n2 peers=%v", n1.Peers(), n2.Peers())
}

// TestDHT_PeersInTable verifies that PeersInTable reflects seeded routing table entries.
func TestDHT_PeersInTable(t *testing.T) {
	_, dhtA := startDHTNode(t, "pit-A", "127.0.0.1:19610")
	_, dhtB := startDHTNode(t, "pit-B", "127.0.0.1:19611")

	if got := dhtA.PeersInTable(); len(got) != 0 {
		t.Fatalf("expected empty table before seeding, got %d entries", len(got))
	}

	b := dhtB.LocalNodeInfo()
	dhtA.SeedPeer(b.NodeID, b.Address, b.PublicKey)

	got := dhtA.PeersInTable()
	if len(got) != 1 || got[0].NodeID != "pit-B" {
		t.Fatalf("expected [pit-B] in table, got %+v", got)
	}
}

// TestDHT_Network_StoreReplicatesToPeer verifies that Store replicates to a
// connected peer, and that peer can return the provider record via FindProviders.
func TestDHT_Network_StoreReplicatesToPeer(t *testing.T) {
	jc := jsoncdc.New()

	nA, err := node.New(node.Config{
		NodeID:     "net-store-A",
		ListenAddr: "127.0.0.1:19612",
		Transport:  tcptransport.New(0),
		Codec:      jc,
		Handshaker: identify.New(identify.Config{}),
	}, nil)
	if err != nil {
		t.Fatalf("create nA: %v", err)
	}
	dhtA := dht.New(nA, "net-store-A", "127.0.0.1:19612", dht.Config{})
	if err := nA.Start(); err != nil {
		t.Fatalf("start nA: %v", err)
	}
	t.Cleanup(func() { dhtA.Stop(); nA.Stop() })

	src := &eventSource{ch: make(chan discovery.PeerEvent, 1)}
	nB, dhtB := startDHTNodeWithSource(t, "net-store-B", "127.0.0.1:19613", src)

	// Seed routing tables so iterativeLookup knows about the other node.
	b := dhtB.LocalNodeInfo()
	dhtA.SeedPeer(b.NodeID, b.Address, b.PublicKey)
	a := dhtA.LocalNodeInfo()
	dhtB.SeedPeer(a.NodeID, a.Address, a.PublicKey)

	// Trigger TCP connection (B dials A).
	src.ch <- discovery.PeerEvent{
		Type:    discovery.PeerFound,
		PeerID:  "net-store-A",
		Address: "127.0.0.1:19612",
	}
	waitConnected(t, nA, nB, 3*time.Second)

	// A stores the value — iterativeLookup will find B and send a STORE RPC.
	key := []byte("replicated-key")
	value := []byte("replicated-value")
	res, err := dhtA.Store(context.Background(), key, value)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if res.Replicated == 0 {
		t.Fatalf("Store: expected replication to at least one peer, got 0 (attempted %d)", res.Attempted)
	}

	// B should now hold the provider record (received via STORE RPC).
	var providers []dht.ProviderRecord
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		providers, err = dhtB.FindProviders(context.Background(), key)
		if err != nil {
			t.Fatalf("B.FindProviders: %v", err)
		}
		if len(providers) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(providers) == 0 {
		t.Fatal("STORE replication: B does not have the provider record after Store()")
	}
	if string(providers[0].Value) != string(value) {
		t.Errorf("B.FindProviders value = %q, want %q", providers[0].Value, value)
	}
}

// TestDHT_Network_FindProvidersFromPeer verifies that iterativeFindProviders
// issues FIND_PROVIDERS RPCs and retrieves records held exclusively by a remote peer.
func TestDHT_Network_FindProvidersFromPeer(t *testing.T) {
	jc := jsoncdc.New()

	nA, err := node.New(node.Config{
		NodeID:     "fv-A",
		ListenAddr: "127.0.0.1:19614",
		Transport:  tcptransport.New(0),
		Codec:      jc,
		Handshaker: identify.New(identify.Config{}),
	}, nil)
	if err != nil {
		t.Fatalf("create nA: %v", err)
	}
	dhtA := dht.New(nA, "fv-A", "127.0.0.1:19614", dht.Config{})
	if err := nA.Start(); err != nil {
		t.Fatalf("start nA: %v", err)
	}
	t.Cleanup(func() { dhtA.Stop(); nA.Stop() })

	src := &eventSource{ch: make(chan discovery.PeerEvent, 1)}
	nB, dhtB := startDHTNodeWithSource(t, "fv-B", "127.0.0.1:19615", src)

	// Store the value on B BEFORE seeding routing tables so Store does not
	// replicate to A — A must retrieve it via FIND_VALUE RPC later.
	key := []byte("remote-key")
	value := []byte("remote-value")
	// B has no peers yet so Attempted == 0 (local-only store is expected here).
	if _, err := dhtB.Store(context.Background(), key, value); err != nil {
		t.Fatalf("dhtB.Store: %v", err)
	}

	// Now seed routing tables and connect.
	b := dhtB.LocalNodeInfo()
	dhtA.SeedPeer(b.NodeID, b.Address, b.PublicKey)
	a := dhtA.LocalNodeInfo()
	dhtB.SeedPeer(a.NodeID, a.Address, a.PublicKey)

	src.ch <- discovery.PeerEvent{
		Type:    discovery.PeerFound,
		PeerID:  "fv-A",
		Address: "127.0.0.1:19614",
	}
	waitConnected(t, nA, nB, 3*time.Second)

	// A does not have the provider record locally — must retrieve from B via FIND_PROVIDERS.
	var providers []dht.ProviderRecord
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		providers, err = dhtA.FindProviders(context.Background(), key)
		if err != nil {
			t.Fatalf("dhtA.FindProviders: %v", err)
		}
		if len(providers) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(providers) == 0 {
		t.Fatal("dhtA.FindProviders: did not retrieve provider record from B via FIND_PROVIDERS RPC")
	}
	if string(providers[0].Value) != string(value) {
		t.Errorf("dhtA.FindProviders value = %q, want %q", providers[0].Value, value)
	}
}
