package main

// TestGossip_E2E verifies end-to-end convergence in a fully-connected 5-node
// network. All messages arrive in a single hop (hops=0).
//
// TestGossip_E2E_Relay verifies multi-hop gossip routing using a two-partition
// topology. Two independent bootstraps create isolated discovery domains; hub
// announces to both and bridges them. Group-A leaves only know hub and each
// other; group-B leaves likewise. Cross-partition messages must relay through
// hub — no direct A↔B connections form because the bootstraps never exchange
// peer lists with each other.
//
//   bsA ←── leaf1, leaf2  (group A)
//   hub ←── bsA + bsB     (bridge)
//   bsB ←── leaf3, leaf4  (group B)
//
// This is deterministic: no MaxPeers races, no timing tricks. Relay is
// structurally guaranteed by the partition.
//
// Ports 19950–19962.

import (
	"fmt"
	"sync"
	"testing"
	"time"

	note "github.com/m-sossich/note"
	"github.com/m-sossich/note/demo/gossip/protocol"
	"github.com/m-sossich/note/pkg/identity"
	"github.com/m-sossich/note/pkg/node"
)

// gossipTestNode is a running gossip node wired for test observation.
type gossipTestNode struct {
	p        *note.Peer
	h        *protocol.Handler
	received map[string]protocol.GossipMessage
	mu       sync.Mutex
}

func newGossipNode(t *testing.T, addr, bootstrapAddr string) *gossipTestNode {
	t.Helper()
	kp, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	gn := &gossipTestNode{received: make(map[string]protocol.GossipMessage)}
	var h *protocol.Handler
	p, err := note.NewVerifiedPeer(kp, addr,
		note.WithBootstrap(bootstrapAddr),
		note.WithHandlerFactory(func(n node.Node) {
			h = protocol.NewHandler(n, kp.NodeID)
		}),
	)
	if err != nil {
		t.Fatalf("newGossipNode %s: %v", addr, err)
	}
	t.Cleanup(func() { p.Close() })
	h.SetOnReceive(func(msg protocol.GossipMessage, _ string) {
		gn.mu.Lock()
		gn.received[msg.ID] = msg
		gn.mu.Unlock()
	})
	gn.p = p
	gn.h = h
	return gn
}

// ---------------------------------------------------------------------------
// TestGossip_E2E — full mesh
// ---------------------------------------------------------------------------

func TestGossip_E2E(t *testing.T) {
	const bootstrapAddr = "127.0.0.1:19950"
	addrs := []string{
		"127.0.0.1:19951", "127.0.0.1:19952",
		"127.0.0.1:19953", "127.0.0.1:19954", "127.0.0.1:19955",
	}

	bsKP, _ := identity.Generate()
	bs, err := note.NewVerifiedPeer(bsKP, bootstrapAddr)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	t.Cleanup(func() { bs.Close() })

	nodes := make([]*gossipTestNode, len(addrs))
	for i, addr := range addrs {
		nodes[i] = newGossipNode(t, addr, bootstrapAddr)
	}
	nodeIDs := make([]string, len(nodes))
	for i, n := range nodes {
		nodeIDs[i] = n.p.ID()
	}

	t.Log("phase 1: mesh formation")
	waitUntilGossip(t, 30*time.Second, 200*time.Millisecond, func() (bool, string) {
		for i, n := range nodes {
			peerSet := make(map[string]struct{})
			for _, id := range n.p.Peers() {
				peerSet[id] = struct{}{}
			}
			for j, id := range nodeIDs {
				if j == i {
					continue
				}
				if _, ok := peerSet[id]; !ok {
					return false, fmt.Sprintf("node %d missing peer %d", i, j)
				}
			}
		}
		return true, ""
	})

	published := make([]protocol.GossipMessage, len(nodes))
	for i, n := range nodes {
		published[i] = n.h.Publish(fmt.Sprintf("msg from node %d", i))
	}

	waitUntilGossip(t, 15*time.Second, 100*time.Millisecond, func() (bool, string) {
		for i, n := range nodes {
			n.mu.Lock()
			c := len(n.received)
			n.mu.Unlock()
			if c < len(nodes) {
				return false, fmt.Sprintf("node %d has %d/%d", i, c, len(nodes))
			}
		}
		return true, ""
	})

	for i, n := range nodes {
		n.mu.Lock()
		recv := make(map[string]protocol.GossipMessage, len(n.received))
		for k, v := range n.received {
			recv[k] = v
		}
		n.mu.Unlock()
		for _, pub := range published {
			if m, ok := recv[pub.ID]; !ok {
				t.Errorf("node %d missing message from %s", i, pub.OriginID[:8])
			} else if m.Text != pub.Text {
				t.Errorf("node %d wrong text: got %q want %q", i, m.Text, pub.Text)
			}
		}
		if got := len(recv); got != len(published) {
			t.Errorf("node %d: %d messages, expected %d (duplicate?)", i, got, len(published))
		}
	}
	t.Log("TestGossip_E2E passed")
}

// ---------------------------------------------------------------------------
// TestGossip_E2E_Relay — two-partition topology, guaranteed relay
// ---------------------------------------------------------------------------

func TestGossip_E2E_Relay(t *testing.T) {
	const (
		bsAAddr   = "127.0.0.1:19956"
		bsBAddr   = "127.0.0.1:19957"
		hubAddr   = "127.0.0.1:19958"
		leaf1Addr = "127.0.0.1:19959"
		leaf2Addr = "127.0.0.1:19960"
		leaf3Addr = "127.0.0.1:19961"
		leaf4Addr = "127.0.0.1:19962"
	)

	// Two independent bootstraps — they never exchange peer lists.
	for _, addr := range []string{bsAAddr, bsBAddr} {
		kp, _ := identity.Generate()
		p, err := note.NewVerifiedPeer(kp, addr)
		if err != nil {
			t.Fatalf("bootstrap %s: %v", addr, err)
		}
		t.Cleanup(func() { p.Close() })
	}

	// Hub announces to BOTH bootstraps. Its address will appear in PEERS
	// responses from bsA (for group-A leaves) and bsB (for group-B leaves).
	hubKP, _ := identity.Generate()
	hub := &gossipTestNode{received: make(map[string]protocol.GossipMessage)}
	var hubH *protocol.Handler
	hubPeer, err := note.NewVerifiedPeer(hubKP, hubAddr,
		note.WithBootstrap(bsAAddr, bsBAddr),
		note.WithHandlerFactory(func(n node.Node) {
			hubH = protocol.NewHandler(n, hubKP.NodeID)
		}),
	)
	if err != nil {
		t.Fatalf("hub: %v", err)
	}
	t.Cleanup(func() { hubPeer.Close() })
	hubH.SetOnReceive(func(msg protocol.GossipMessage, _ string) {
		hub.mu.Lock()
		hub.received[msg.ID] = msg
		hub.mu.Unlock()
	})
	hub.p = hubPeer
	hub.h = hubH

	mkLeaf := func(addr, bsAddr string) *gossipTestNode {
		kp, _ := identity.Generate()
		gn := &gossipTestNode{received: make(map[string]protocol.GossipMessage)}
		var h *protocol.Handler
		p, err := note.NewVerifiedPeer(kp, addr,
			note.WithBootstrap(bsAddr),
			note.WithHandlerFactory(func(n node.Node) {
				h = protocol.NewHandler(n, kp.NodeID)
			}),
		)
		if err != nil {
			t.Fatalf("leaf %s: %v", addr, err)
		}
		t.Cleanup(func() { p.Close() })
		h.SetOnReceive(func(msg protocol.GossipMessage, _ string) {
			gn.mu.Lock()
			gn.received[msg.ID] = msg
			gn.mu.Unlock()
		})
		gn.p = p
		gn.h = h
		return gn
	}

	// Group A — bootstrap bsA; will discover hub + each other, never group B.
	leaf1 := mkLeaf(leaf1Addr, bsAAddr)
	leaf2 := mkLeaf(leaf2Addr, bsAAddr)

	// Group B — bootstrap bsB; will discover hub + each other, never group A.
	leaf3 := mkLeaf(leaf3Addr, bsBAddr)
	leaf4 := mkLeaf(leaf4Addr, bsBAddr)

	all := []*gossipTestNode{hub, leaf1, leaf2, leaf3, leaf4}

	// Phase 1: hub must be connected to at least one node from each partition.
	// Once hub bridges both groups, messages can cross via relay.
	t.Log("phase 1: waiting for hub to bridge both partitions")
	waitUntilGossip(t, 30*time.Second, 200*time.Millisecond, func() (bool, string) {
		hubPeers := make(map[string]struct{})
		for _, id := range hub.p.Peers() {
			hubPeers[id] = struct{}{}
		}
		hasA := func() bool {
			for _, l := range []*gossipTestNode{leaf1, leaf2} {
				if _, ok := hubPeers[l.p.ID()]; ok {
					return true
				}
			}
			return false
		}
		hasB := func() bool {
			for _, l := range []*gossipTestNode{leaf3, leaf4} {
				if _, ok := hubPeers[l.p.ID()]; ok {
					return true
				}
			}
			return false
		}
		if !hasA() {
			return false, "hub not yet connected to any group-A leaf"
		}
		if !hasB() {
			return false, "hub not yet connected to any group-B leaf"
		}
		return true, ""
	})
	t.Logf("hub: %d peers | leaf1: %d leaf2: %d leaf3: %d leaf4: %d",
		len(hub.p.Peers()), len(leaf1.p.Peers()), len(leaf2.p.Peers()),
		len(leaf3.p.Peers()), len(leaf4.p.Peers()))
	t.Log("phase 1 complete: hub bridges both partitions")

	// Phase 2: publish one message from each partition.
	msgA := leaf1.h.Publish("from group-A (leaf1)")
	msgB := leaf3.h.Publish("from group-B (leaf3)")

	// Phase 3: cross-partition delivery.
	// leaf4 (group B) must receive msgA (from group A) via hub.
	// leaf2 (group A) must receive msgB (from group B) via hub.
	t.Log("phase 3: waiting for cross-partition delivery")
	waitUntilGossip(t, 15*time.Second, 100*time.Millisecond, func() (bool, string) {
		leaf4.mu.Lock()
		gotA := leaf4.received[msgA.ID].ID != ""
		leaf4.mu.Unlock()
		if !gotA {
			return false, "leaf4 (group-B) missing leaf1's message (group-A)"
		}
		leaf2.mu.Lock()
		gotB := leaf2.received[msgB.ID].ID != ""
		leaf2.mu.Unlock()
		if !gotB {
			return false, "leaf2 (group-A) missing leaf3's message (group-B)"
		}
		return true, ""
	})
	t.Log("phase 3 complete: cross-partition delivery confirmed")

	// Phase 4: verify relay. Messages that crossed the partition boundary
	// must have arrived with hops > 0 — no direct A↔B connections exist.
	leaf4.mu.Lock()
	hopsA := leaf4.received[msgA.ID].Hops
	leaf4.mu.Unlock()

	leaf2.mu.Lock()
	hopsB := leaf2.received[msgB.ID].Hops
	leaf2.mu.Unlock()

	if hopsA == 0 {
		t.Errorf("leaf4 received leaf1's message with hops=0: direct A↔B connection formed, partition broken")
	}
	if hopsB == 0 {
		t.Errorf("leaf2 received leaf3's message with hops=0: direct A↔B connection formed, partition broken")
	}
	t.Logf("phase 4: relay confirmed — leaf1→leaf4 hops=%d, leaf3→leaf2 hops=%d", hopsA, hopsB)

	// Phase 5: full convergence — all nodes receive both messages.
	waitUntilGossip(t, 10*time.Second, 100*time.Millisecond, func() (bool, string) {
		for i, n := range all {
			for _, msg := range []protocol.GossipMessage{msgA, msgB} {
				n.mu.Lock()
				_, ok := n.received[msg.ID]
				n.mu.Unlock()
				if !ok {
					return false, fmt.Sprintf("node %d missing message %s", i, msg.ID[:8])
				}
			}
		}
		return true, ""
	})
	t.Log("phase 5 complete: all nodes have all messages")
}

func waitUntilGossip(t *testing.T, timeout, poll time.Duration, cond func() (bool, string)) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if ok, _ := cond(); ok {
			return
		}
		select {
		case <-deadline:
			_, last := cond()
			t.Fatalf("timed out after %s: %s", timeout, last)
		case <-time.After(poll):
		}
	}
}
