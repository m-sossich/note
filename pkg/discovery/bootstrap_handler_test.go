package discovery

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/m-sossich/note/pkg/identity"
)

// newBootstrapDiscovery returns a minimal Discovery for unit tests.
func newBootstrapDiscovery(nodeID string, tr *captureSendTransport) *Discovery {
	return &Discovery{
		cfg: Config{
			NodeID: nodeID,
			Codec:  testCodec(),
		},
		tr:            tr,
		table:         newPeerTable(0),
		events:        make(chan PeerEvent, 64),
		stopCh:        make(chan struct{}),
		pending:       make(map[string]string),
		pendingByPeer: make(map[string]string),
	}
}

// signedAnnounce returns a properly signed announceMsg for use in tests.
func signedAnnounce(kp *identity.Keypair, protocols []string) announceMsg {
	nodeID := kp.NodeID
	address := "10.0.0.1:9000"
	data := announceSignData(nodeID, address, protocols)
	return announceMsg{
		Type:      msgAnnounce,
		NodeID:    nodeID,
		Address:   address,
		PublicKey: base64.StdEncoding.EncodeToString(kp.PublicKey),
		Signature: base64.StdEncoding.EncodeToString(kp.Sign(data)),
		Protocols: protocols,
	}
}

// TestVerifyAnnounce_ValidSignature verifies that a correctly signed ANNOUNCE passes.
func TestVerifyAnnounce_ValidSignature(t *testing.T) {
	kp, _ := identity.Generate()
	msg := signedAnnounce(kp, []string{"chat/1.0", "dht/1.0"})
	if !verifyAnnounce(msg) {
		t.Error("expected valid signed ANNOUNCE to pass verification")
	}
}

// TestVerifyAnnounce_MismatchedNodeID verifies that a public key not matching
// the claimed NodeID is rejected.
func TestVerifyAnnounce_MismatchedNodeID(t *testing.T) {
	kpA, _ := identity.Generate()
	kpB, _ := identity.Generate()
	msg := signedAnnounce(kpA, nil)
	msg.NodeID = kpB.NodeID // claim B's ID but sign with A's key
	if verifyAnnounce(msg) {
		t.Error("expected mismatched NodeID to fail verification")
	}
}

// TestVerifyAnnounce_InvalidSignature verifies that a valid public key with a
// wrong signature is rejected.
func TestVerifyAnnounce_InvalidSignature(t *testing.T) {
	kpA, _ := identity.Generate()
	kpB, _ := identity.Generate()
	msg := signedAnnounce(kpA, nil)
	// Replace the signature with one signed by a different key.
	msg.Signature = base64.StdEncoding.EncodeToString(kpB.Sign(announceSignData(msg.NodeID, msg.Address, msg.Protocols)))
	if verifyAnnounce(msg) {
		t.Error("expected wrong signature to fail verification")
	}
}

// TestVerifyAnnounce_MalformedPublicKey verifies that an unparsable public key
// is rejected.
func TestVerifyAnnounce_MalformedPublicKey(t *testing.T) {
	kp, _ := identity.Generate()
	msg := signedAnnounce(kp, nil)
	msg.PublicKey = "not-base64!!!"
	if verifyAnnounce(msg) {
		t.Error("expected malformed public key to fail verification")
	}
}

// TestVerifyAnnounce_MalformedSignature verifies that an unparsable signature
// string is rejected before Ed25519 verification is attempted.
func TestVerifyAnnounce_MalformedSignature(t *testing.T) {
	kp, _ := identity.Generate()
	msg := signedAnnounce(kp, nil)
	msg.Signature = "not-base64!!!"
	if verifyAnnounce(msg) {
		t.Error("expected malformed signature to fail verification")
	}
}

// TestVerifyAnnounce_TamperedProtocols verifies that modifying the Protocols
// field after signing invalidates the signature — closing the MITM vector
// where an attacker strips or replaces the protocol list in transit.
func TestVerifyAnnounce_TamperedProtocols(t *testing.T) {
	kp, _ := identity.Generate()
	msg := signedAnnounce(kp, []string{"chat/1.0", "dht/1.0"})
	msg.Protocols = []string{} // MITM strips the protocol list
	if verifyAnnounce(msg) {
		t.Error("expected tampered Protocols to fail verification")
	}
}

// TestHandleAnnounce_UnsignedAccepted verifies that an ANNOUNCE without a
// signature (trusted mode) is accepted by any node.
func TestHandleAnnounce_UnsignedAccepted(t *testing.T) {
	tr := newCaptureTransport()
	d := newBootstrapDiscovery("bootstrap", tr)
	d.handleAnnounce("10.0.0.1:9000", announceMsg{
		Type:    msgAnnounce,
		NodeID:  "trusted-peer",
		Address: "10.0.0.1:9000",
	})
	if _, ok := d.table.Get("trusted-peer"); !ok {
		t.Error("unsigned ANNOUNCE should be accepted in trusted mode")
	}
}

// TestHandleAnnounce_InvalidSignatureDropped verifies that a signed ANNOUNCE
// with an invalid signature is dropped and not added to the peer table.
func TestHandleAnnounce_InvalidSignatureDropped(t *testing.T) {
	kpA, _ := identity.Generate()
	kpB, _ := identity.Generate()
	tr := newCaptureTransport()
	d := newBootstrapDiscovery("bootstrap", tr)

	msg := signedAnnounce(kpA, nil)
	// Replace signature with one from a different keypair — binding fails.
	msg.Signature = base64.StdEncoding.EncodeToString(kpB.Sign(announceSignData(msg.NodeID, msg.Address, msg.Protocols)))

	d.handleAnnounce("10.0.0.1:9000", msg)
	if _, ok := d.table.Get(msg.NodeID); ok {
		t.Error("ANNOUNCE with invalid signature should be dropped")
	}
}

// TestHandleAnnounce_ValidSignatureAdded verifies that a correctly signed
// ANNOUNCE is added to the peer table.
func TestHandleAnnounce_ValidSignatureAdded(t *testing.T) {
	kp, _ := identity.Generate()
	tr := newCaptureTransport()
	d := newBootstrapDiscovery("bootstrap", tr)

	d.handleAnnounce("10.0.0.1:9000", signedAnnounce(kp, []string{"chat/1.0"}))
	if _, ok := d.table.Get(kp.NodeID); !ok {
		t.Error("valid signed ANNOUNCE should be added to peer table")
	}
}

// TestHandleAnnounce_EmitsPeerLostOnEviction verifies that when the peer table
// is full and a new ANNOUNCE arrives, a peer-lost event is emitted for the
// evicted peer alongside the peer-found event for the new one.
func TestHandleAnnounce_EmitsPeerLostOnEviction(t *testing.T) {
	tr := newCaptureTransport()
	d := &Discovery{
		cfg:           Config{NodeID: "bootstrap", Codec: testCodec(), MaxPeers: 1},
		tr:            tr,
		table:         newPeerTable(1),
		events:        make(chan PeerEvent, 8),
		stopCh:        make(chan struct{}),
		pending:       make(map[string]string),
		pendingByPeer: make(map[string]string),
	}

	// Fill the table.
	d.handleAnnounce("10.0.0.1:9000", announceMsg{
		Type: msgAnnounce, NodeID: "peer-A", Address: "10.0.0.1:9000",
	})
	<-d.events // drain peer-found for peer-A

	// Second ANNOUNCE must evict peer-A and emit peer-lost + peer-found.
	d.handleAnnounce("10.0.0.2:9000", announceMsg{
		Type: msgAnnounce, NodeID: "peer-B", Address: "10.0.0.2:9000",
	})

	var gotLost, gotFound bool
	for len(d.events) > 0 {
		ev := <-d.events
		if ev.Type == PeerLost && ev.PeerID == "peer-A" {
			gotLost = true
		}
		if ev.Type == PeerFound && ev.PeerID == "peer-B" {
			gotFound = true
		}
	}
	if !gotLost {
		t.Error("expected peer-lost event for evicted peer-A")
	}
	if !gotFound {
		t.Error("expected peer-found event for new peer-B")
	}
}

// TestHandlePeers_EmitsPeerLostOnEviction verifies that a PEERS response also
// emits peer-lost when the table is full and eviction occurs.
func TestHandlePeers_EmitsPeerLostOnEviction(t *testing.T) {
	tr := newCaptureTransport()
	d := &Discovery{
		cfg:           Config{NodeID: "node", Codec: testCodec(), MaxPeers: 1},
		tr:            tr,
		table:         newPeerTable(1),
		events:        make(chan PeerEvent, 8),
		stopCh:        make(chan struct{}),
		pending:       make(map[string]string),
		pendingByPeer: make(map[string]string),
	}

	d.handlePeers(peersMsg{
		Type:  msgPeers,
		Peers: []peerEntry{{NodeID: "peer-A", Address: "10.0.0.1:9000"}},
	})
	<-d.events // drain peer-found for peer-A

	d.handlePeers(peersMsg{
		Type:  msgPeers,
		Peers: []peerEntry{{NodeID: "peer-B", Address: "10.0.0.2:9000"}},
	})

	var gotLost bool
	for len(d.events) > 0 {
		ev := <-d.events
		if ev.Type == PeerLost && ev.PeerID == "peer-A" {
			gotLost = true
		}
	}
	if !gotLost {
		t.Error("expected peer-lost event for evicted peer-A from PEERS handler")
	}
}

// TestSendPeerList_CapsAt100 verifies DISC-13: a PEERS response never exceeds
// 100 entries regardless of how many peers are in the table.
func TestSendPeerList_CapsAt100(t *testing.T) {
	tr := newCaptureTransport()
	d := newBootstrapDiscovery("bootstrap", tr)

	// Add 150 peers to the table.
	for i := 0; i < 150; i++ {
		d.table.Add(fmt.Sprintf("peer-%d", i), fmt.Sprintf("10.0.0.%d:9000", i%256), nil)
	}

	d.sendPeerList("requester-addr:9000", "some-excluded-node")

	pkt, ok := tr.lastSent()
	data := pkt.data
	if !ok {
		t.Fatal("sendPeerList did not send any packet to the requester")
	}

	var msg peersMsg
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("unmarshal PEERS response: %v", err)
	}
	// maxPeersPerResponse includes the self-entry: (maxPeersPerResponse-1) other peers + 1 self.
	if len(msg.Peers) > maxPeersPerResponse {
		t.Errorf("PEERS response contains %d entries, want ≤ %d (DISC-13)", len(msg.Peers), maxPeersPerResponse)
	}
}

// TestSendPeerList_ExcludesSender verifies that the requesting node's own ID
// is never included in the response — regardless of table size.
func TestSendPeerList_ExcludesSender(t *testing.T) {
	tr := newCaptureTransport()
	const requester = "the-requester"
	d := newBootstrapDiscovery("bootstrap", tr)
	d.table.Add(requester, "10.0.0.1:9000", nil)
	d.table.Add("other-peer", "10.0.0.2:9000", nil)

	d.sendPeerList("10.0.0.1:9000", requester)

	pkt, ok := tr.lastSent()
	data := pkt.data
	if !ok {
		t.Fatal("sendPeerList sent no packet")
	}
	var msg peersMsg
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, p := range msg.Peers {
		if p.NodeID == requester {
			t.Errorf("sender %q should be excluded from PEERS response", requester)
		}
	}
}

// TestSendPeerList_UnderLimit verifies that when the table has fewer than 100
// entries, all of them are returned (no spurious truncation).
func TestSendPeerList_UnderLimit(t *testing.T) {
	tr := newCaptureTransport()
	d := newBootstrapDiscovery("bootstrap", tr)

	for i := 0; i < 10; i++ {
		d.table.Add(fmt.Sprintf("peer-%d", i), fmt.Sprintf("10.0.0.%d:9000", i), nil)
	}

	d.sendPeerList("requester:9000", "excluded")

	pkt, ok := tr.lastSent()
	data := pkt.data
	if !ok {
		t.Fatal("sendPeerList sent no packet")
	}
	var msg peersMsg
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// sendPeerList always appends the local node (self-entry), so a 10-peer
	// table returns 11 entries (10 known peers + the sender itself).
	if len(msg.Peers) != 11 {
		t.Errorf("expected 11 entries (10 peers + self), got %d", len(msg.Peers))
	}
}
