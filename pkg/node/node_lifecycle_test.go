package node

import (
	"strings"
	"testing"

	"github.com/m-sossich/note/pkg/codec"
	jsoncdc "github.com/m-sossich/note/pkg/codec/json"
	tcptransport "github.com/m-sossich/note/pkg/transport/tcp"
)

func jc() codec.Codec { return jsoncdc.New() }

// TestNew_MissingNodeID verifies that New returns an error when NodeID is empty.
func TestNew_MissingNodeID(t *testing.T) {
	_, err := New(Config{Codec: jc()}, nil)
	if err == nil {
		t.Fatal("expected error for missing NodeID, got nil")
	}
	if !strings.Contains(err.Error(), "NodeID") {
		t.Errorf("error should mention 'NodeID', got: %v", err)
	}
}

// TestNode_Start_DoubleStart verifies that calling Start twice returns an error.
func TestNode_Start_DoubleStart(t *testing.T) {
	n, err := New(Config{
		NodeID:     "node-ds",
		ListenAddr: "127.0.0.1:0",
		Transport:  tcptransport.New(0),
		Handshaker: &minHandshaker{},
		Codec:      jc(),
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := n.Start(); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	t.Cleanup(func() { n.Stop() })

	if err := n.Start(); err == nil {
		t.Fatal("expected error on second Start, got nil")
	}
}

// TestNode_Stop_DoubleStop verifies that Stop is idempotent (sync.Once guard).
func TestNode_Stop_DoubleStop(t *testing.T) {
	n, err := New(Config{
		NodeID:     "node-dstop",
		ListenAddr: "127.0.0.1:0",
		Transport:  tcptransport.New(0),
		Handshaker: &minHandshaker{},
		Codec:      jc(),
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := n.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := n.Stop(); err != nil {
		t.Errorf("first Stop: %v", err)
	}
	// Must not panic or deadlock.
	if err := n.Stop(); err != nil {
		t.Errorf("second Stop: %v", err)
	}
}

// TestNode_Stop_BeforeStart verifies that Stop on a never-started node does not panic.
func TestNode_Stop_BeforeStart(t *testing.T) {
	n, err := New(Config{
		NodeID:     "node-sbefore",
		ListenAddr: "127.0.0.1:0",
		Transport:  tcptransport.New(0),
		Handshaker: &minHandshaker{},
		Codec:      jc(),
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Must not panic.
	_ = n.Stop()
}

// TestConnectionState_String_Unknown verifies that an unrecognised state value
// returns "UNKNOWN" rather than panicking.
func TestConnectionState_String_Unknown(t *testing.T) {
	got := connectionState(99).String()
	if got != "UNKNOWN" {
		t.Errorf("connectionState(99).String() = %q, want %q", got, "UNKNOWN")
	}
}
