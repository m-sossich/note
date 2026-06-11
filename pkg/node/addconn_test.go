package node

import (
	"testing"
)

// TestAddConn_NoCollision verifies the happy path: first connection registers
// cleanly with won=true and no loser.
func TestAddConn_NoCollision(t *testing.T) {
	n := &nodeImpl{
		cfg:   Config{NodeID: "local"},
		conns: make(map[string]*connection),
	}
	conn := &connection{peerID: "remote"}
	won, loser := n.addConn(conn)
	if !won {
		t.Error("expected won=true for first connection to peer")
	}
	if loser != nil {
		t.Errorf("expected no loser, got %v", loser)
	}
	if n.conns["remote"] != conn {
		t.Error("connection not registered in map")
	}
}

// TestAddConn_NewWins verifies NOD-15: when local ID < remote ID, the new
// connection wins, the existing connection is the loser.
func TestAddConn_NewWins(t *testing.T) {
	n := &nodeImpl{
		cfg:   Config{NodeID: "aaa"}, // "aaa" < "zzz"
		conns: make(map[string]*connection),
	}
	existing := &connection{peerID: "zzz"}
	n.conns["zzz"] = existing

	newcomer := &connection{peerID: "zzz"}
	won, loser := n.addConn(newcomer)

	if !won {
		t.Error("expected won=true when local ID < remote ID")
	}
	if loser != existing {
		t.Error("expected existing connection to be the loser")
	}
	if n.conns["zzz"] != newcomer {
		t.Error("expected newcomer to be registered in map")
	}
}

// TestAddConn_NewLoses verifies NOD-15: when local ID > remote ID, the new
// connection loses and the existing connection stays in the map.
func TestAddConn_NewLoses(t *testing.T) {
	n := &nodeImpl{
		cfg:   Config{NodeID: "zzz"}, // "zzz" > "aaa"
		conns: make(map[string]*connection),
	}
	existing := &connection{peerID: "aaa"}
	n.conns["aaa"] = existing

	newcomer := &connection{peerID: "aaa"}
	won, loser := n.addConn(newcomer)

	if won {
		t.Error("expected won=false when local ID > remote ID")
	}
	if loser != nil {
		t.Error("expected loser=nil when new connection itself loses")
	}
	if n.conns["aaa"] != existing {
		t.Error("expected existing connection to remain in map")
	}
}

// TestRemoveConnIfCurrent_MatchRemoves verifies that a goroutine removes its
// own connection and returns true.
func TestRemoveConnIfCurrent_MatchRemoves(t *testing.T) {
	n := &nodeImpl{conns: make(map[string]*connection)}
	conn := &connection{peerID: "peer"}
	n.conns["peer"] = conn

	if !n.removeConnIfCurrent("peer", conn) {
		t.Error("expected true when removing current connection")
	}
	if _, ok := n.conns["peer"]; ok {
		t.Error("connection should be removed from map")
	}
}

// TestRemoveConnIfCurrent_MismatchIsNoop verifies that a losing tie-break
// goroutine does not evict the winner from the map.
func TestRemoveConnIfCurrent_MismatchIsNoop(t *testing.T) {
	n := &nodeImpl{conns: make(map[string]*connection)}
	winner := &connection{peerID: "peer"}
	loser := &connection{peerID: "peer"}
	n.conns["peer"] = winner

	if n.removeConnIfCurrent("peer", loser) {
		t.Error("expected false when conn does not match current entry")
	}
	if n.conns["peer"] != winner {
		t.Error("winner should remain in map after loser's remove attempt")
	}
}
