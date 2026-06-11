package discovery

import (
	"testing"
	"time"

	jsoncdc "github.com/m-sossich/note/pkg/codec/json"
)

// testCodec returns the default JSON codec for use in tests.
func testCodec() *jsoncdc.Codec { return jsoncdc.New() }

// TestDispatch_MissingTypeField verifies that dispatch drops packets that decode
// successfully but carry no Type discriminator.
func TestDispatch_MissingTypeField(t *testing.T) {
	tr := newCaptureTransport()
	d := newDiscovery(Config{NodeID: "node"}, tr)
	data, _ := marshalMsg(struct{ NodeID string }{NodeID: "x"}, testCodec())
	d.dispatch("10.0.0.1:9001", data)
	if _, ok := tr.lastSent(); ok {
		t.Error("packet with missing Type should be silently dropped")
	}
}

// TestConfig_AdvertiseAddr_Fallback verifies that advertiseAddr returns BindAddr
// when AdvertiseAddr is empty.
func TestConfig_AdvertiseAddr_Fallback(t *testing.T) {
	cfg := Config{BindAddr: "127.0.0.1:9000"}
	got := cfg.advertiseAddr()
	if got != "127.0.0.1:9000" {
		t.Errorf("advertiseAddr fallback = %q, want %q", got, "127.0.0.1:9000")
	}
}

// TestConfig_AdvertiseAddr_Override verifies that advertiseAddr returns the
// explicit AdvertiseAddr when set.
func TestConfig_AdvertiseAddr_Override(t *testing.T) {
	cfg := Config{
		BindAddr:      "0.0.0.0:9000",
		AdvertiseAddr: "10.0.0.1:9000",
	}
	got := cfg.advertiseAddr()
	if got != "10.0.0.1:9000" {
		t.Errorf("advertiseAddr override = %q, want %q", got, "10.0.0.1:9000")
	}
}

// TestLivenessTick_EmptyTable verifies that livenessTick does not panic and
// emits no events when the peer table is empty.
func TestLivenessTick_EmptyTable(t *testing.T) {
	ft := newFakeTransport(0)
	d := &Discovery{
		cfg:     Config{NodeID: "test-node", PingInterval: time.Hour, PingMaxMissed: 3},
		tr:      ft,
		table:   newPeerTable(),
		events:  make(chan PeerEvent, 8),
		stopCh:  make(chan struct{}),
		pending: make(map[string]string),
	}

	// Must not panic.
	d.livenessTick()

	select {
	case ev := <-d.events:
		t.Errorf("expected no events on empty table, got: %+v", ev)
	default:
	}
}

// TestHandlePong_UnknownNonce verifies that handlePong with an unknown nonce
// does not panic and does not change any peer state.
func TestHandlePong_UnknownNonce(t *testing.T) {
	ft := newFakeTransport(0)
	d := &Discovery{
		cfg:     Config{NodeID: "test-node", PingInterval: time.Hour, PingMaxMissed: 3},
		tr:      ft,
		table:   newPeerTable(),
		events:  make(chan PeerEvent, 8),
		stopCh:  make(chan struct{}),
		pending: make(map[string]string),
	}
	d.table.Add("peer-1", "127.0.0.1:9001", nil)
	d.table.MarkPingSent("peer-1")

	// handlePong with a nonce that is not in d.pending.
	d.handlePong(pongMsg{Type: msgPong, NodeID: "peer-1", Nonce: "no-such-nonce"})

	// Peer should still be in the table.
	if len(d.table.List()) != 1 {
		t.Error("peer should remain in table after unknown nonce pong")
	}
}

// TestHandlePeers_FiltersSelf verifies that handlePeers never adds the local
// node to the peer table.
func TestHandlePeers_FiltersSelf(t *testing.T) {
	ft := newFakeTransport(0)
	const localID = "local-node"
	d := &Discovery{
		cfg:     Config{NodeID: localID, PingInterval: time.Hour, PingMaxMissed: 3},
		tr:      ft,
		table:   newPeerTable(),
		events:  make(chan PeerEvent, 8),
		stopCh:  make(chan struct{}),
		pending: make(map[string]string),
	}

	msg := peersMsg{
		Type: msgPeers,
		Peers: []peerEntry{
			{NodeID: localID, Address: "127.0.0.1:9000"},  // self — must be filtered
			{NodeID: "peer-1", Address: "127.0.0.1:9001"}, // remote — must be added
		},
	}
	d.handlePeers(msg)

	peers := d.table.List()
	for _, p := range peers {
		if p.NodeID == localID {
			t.Errorf("self (%s) should not be added to peer table", localID)
		}
	}
	if len(peers) != 1 || peers[0].NodeID != "peer-1" {
		t.Errorf("expected [peer-1] in table, got %+v", peers)
	}
}

// TestDispatch_PingPath verifies that a PING message triggers a PONG reply to the sender.
func TestDispatch_PingPath(t *testing.T) {
	tr := newCaptureTransport()
	d := newDiscovery(Config{NodeID: "pong-node"}, tr)

	ping, err := marshalMsg(newPingMsg("remote-peer", "test-nonce-123"), testCodec())
	if err != nil {
		t.Fatalf("marshal ping: %v", err)
	}

	d.dispatch("10.0.0.1:9001", ping)

	pkt, ok := tr.lastSent()
	if !ok {
		t.Fatal("expected a PONG to be sent, got nothing")
	}
	if pkt.addr != "10.0.0.1:9001" {
		t.Errorf("PONG sent to %q, want 10.0.0.1:9001", pkt.addr)
	}
	var msg pongMsg
	if err := testCodec().Decode(pkt.data, &msg); err != nil {
		t.Fatalf("unmarshal PONG: %v", err)
	}
	if msg.Type != msgPong {
		t.Errorf("message type = %q, want %q", msg.Type, msgPong)
	}
	if msg.Nonce != "test-nonce-123" {
		t.Errorf("PONG nonce = %q, want test-nonce-123", msg.Nonce)
	}
}

// TestDispatch_AnyNode_HandlesAnnounce verifies that every node (not just bootstrap)
// handles ANNOUNCE messages by adding the peer and sending a PEERS reply.
func TestDispatch_AnyNode_HandlesAnnounce(t *testing.T) {
	tr := newCaptureTransport()
	d := newDiscovery(Config{NodeID: "regular-node"}, tr)

	ann, _ := marshalMsg(newAnnounceMsg("peer-1", "10.0.0.2:9000", "", "", nil), testCodec())
	d.dispatch("10.0.0.2:9000", ann)

	// Node must reply with PEERS (even if the list is empty).
	if _, ok := tr.lastSent(); !ok {
		t.Error("every node should reply to ANNOUNCE with PEERS")
	}
	// Peer must be added to the table.
	if _, ok := d.table.Get("peer-1"); !ok {
		t.Error("ANNOUNCE should add peer to the table")
	}
}

// TestDispatch_AnyNode_HandlesFindPeers verifies that every node replies to
// FIND_PEERS messages — the bootstrap role is no longer architectural.
func TestDispatch_AnyNode_HandlesFindPeers(t *testing.T) {
	tr := newCaptureTransport()
	d := newDiscovery(Config{NodeID: "regular-node"}, tr)

	fp, _ := marshalMsg(newFindPeersMsg("peer-1"), testCodec())
	d.dispatch("10.0.0.2:9000", fp)

	if _, ok := tr.lastSent(); !ok {
		t.Error("every node should reply to FIND_PEERS")
	}
}

// TestDispatch_MalformedPacket verifies that dispatch drops packets with invalid JSON.
func TestDispatch_MalformedPacket(t *testing.T) {
	tr := newCaptureTransport()
	d := newDiscovery(Config{NodeID: "node"}, tr)

	d.dispatch("10.0.0.1:9001", []byte("not-json!!!"))

	if _, ok := tr.lastSent(); ok {
		t.Error("malformed packet should be dropped without any reply")
	}
}

// TestEmitEvent_ChannelFull verifies that emitEvent does not block when the
// events channel is full and drops the event instead.
func TestEmitEvent_ChannelFull(t *testing.T) {
	tr := newCaptureTransport()
	d := newDiscovery(Config{NodeID: "node"}, tr)
	// Fill the channel.
	for i := 0; i < cap(d.events); i++ {
		d.events <- PeerEvent{Type: PeerFound, PeerID: "dummy"}
	}

	// Must not block.
	done := make(chan struct{})
	go func() {
		d.emitEvent(PeerEvent{Type: PeerFound, PeerID: "overflow"})
		close(done)
	}()

	select {
	case <-done:
	case <-make(chan struct{}): // never fires — just for select syntax
	}
	// If we reach here, emitEvent returned without blocking.
	select {
	case <-done:
	default:
		t.Fatal("emitEvent blocked on full channel")
	}
}

// TestConfig_SetDefaults_AllZero verifies both fields are filled when zero.
func TestConfig_SetDefaults_AllZero(t *testing.T) {
	cfg := Config{}
	cfg.setDefaults()
	if cfg.PingInterval == 0 {
		t.Error("PingInterval should be non-zero after setDefaults")
	}
	if cfg.PingMaxMissed == 0 {
		t.Error("PingMaxMissed should be non-zero after setDefaults")
	}
}

// TestConfig_SetDefaults_PreservesExisting verifies non-zero fields are not overwritten.
func TestConfig_SetDefaults_PreservesExisting(t *testing.T) {
	const customMissed = 99
	cfg := Config{PingMaxMissed: customMissed}
	cfg.setDefaults()
	if cfg.PingMaxMissed != customMissed {
		t.Errorf("setDefaults overwrote PingMaxMissed: got %d, want %d", cfg.PingMaxMissed, customMissed)
	}
}

// TestHandleAnnounce_IgnoresSelf verifies that handleAnnounce does not add the
// local node to the peer table.
func TestHandleAnnounce_IgnoresSelf(t *testing.T) {
	tr := newCaptureTransport()
	d := newDiscovery(Config{NodeID: "self-node"}, tr)

	d.handleAnnounce("127.0.0.1:9000", announceMsg{
		Type:    msgAnnounce,
		NodeID:  "self-node", // same as local NodeID
		Address: "127.0.0.1:9000",
	})

	if len(d.table.List()) != 0 {
		t.Error("self announce should not be added to the peer table")
	}
}
