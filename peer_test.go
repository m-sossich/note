package note_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	note "github.com/m-sossich/note"
	"github.com/m-sossich/note/pkg/identity"
)

// freeAddr returns a 127.0.0.1:PORT address where PORT is currently unbound.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeAddr: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// peer starts a Peer on a fresh OS-assigned address and registers cleanup.
func newTestPeer(t *testing.T, opts ...note.Option) *note.Peer {
	t.Helper()
	opts = append([]note.Option{
		note.WithIdentityPath(filepath.Join(t.TempDir(), "node.id")),
	}, opts...)
	p, err := note.NewPeer(freeAddr(t), opts...)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

var _ = fmt.Sprintf // keep import

func TestNewPeer_RunsOnCreation(t *testing.T) {
	p, err := note.NewPeer("127.0.0.1:29900",
		note.WithIdentityPath(filepath.Join(t.TempDir(), "node.id")),
	)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	defer p.Close()

	if p.ID() == "" {
		t.Fatal("ID() should be non-empty")
	}
}

func TestNewPeer_IdentityPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.id")

	id1, err := note.LoadOrGenerateID(path)
	if err != nil {
		t.Fatalf("first LoadOrGenerateID: %v", err)
	}
	id2, err := note.LoadOrGenerateID(path)
	if err != nil {
		t.Fatalf("second LoadOrGenerateID: %v", err)
	}
	if id1 != id2 {
		t.Errorf("identity should persist: %q != %q", id1, id2)
	}
}

func TestNewPeer_WithHandler_Registered(t *testing.T) {
	called := make(chan string, 1)

	p, err := note.NewPeer("127.0.0.1:29901",
		note.WithIdentityPath(filepath.Join(t.TempDir(), "node.id")),
		note.WithHandler("echo/1.0", func(peerID, msgType string, decode func(any) error) error {
			called <- msgType
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	defer p.Close()

	if len(called) != 0 {
		t.Error("handler should not have fired without a message")
	}
}

func TestNewPeer_WithDHT_StoreAndFind(t *testing.T) {
	p, err := note.NewPeer("127.0.0.1:29902",
		note.WithIdentityPath(filepath.Join(t.TempDir(), "node.id")),
		note.WithDHT(),
	)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	defer p.Close()

	ctx := context.Background()

	if _, err := p.Lookup(ctx, []byte("key")); err != nil {
		t.Fatalf("Lookup with no peers should not error: %v", err)
	}
	if _, err := p.Announce(ctx, []byte("key"), []byte("value")); err != nil {
		t.Fatalf("Announce: %v", err)
	}
	providers, err := p.FindProviders(ctx, []byte("key"))
	if err != nil {
		t.Fatalf("FindProviders: %v", err)
	}
	if len(providers) == 0 {
		t.Fatal("expected at least one provider after Announce")
	}
	if string(providers[0].Value) != "value" {
		t.Errorf("provider value = %q, want %q", providers[0].Value, "value")
	}
}

func TestNewPeer_NoDHT_ReturnsErrorOnFindProviders(t *testing.T) {
	p, err := note.NewPeer("127.0.0.1:29903",
		note.WithIdentityPath(filepath.Join(t.TempDir(), "node.id")),
	)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	defer p.Close()

	_, err = p.FindProviders(context.Background(), []byte("key"))
	if err == nil {
		t.Error("expected error when calling FindProviders without WithDHT")
	}
	if _, err2 := p.Announce(context.Background(), []byte("key"), nil); err2 == nil {
		t.Error("expected error when calling Announce without WithDHT")
	}
}

func TestNewPeer_StartsAndRespondsToDiscovery(t *testing.T) {
	p, err := note.NewPeer("127.0.0.1:29910",
		note.WithIdentityPath(filepath.Join(t.TempDir(), "bs.id")),
	)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	defer p.Close()
}

// TestTwoNodes_SendReceive exercises the full public API path:
// NewPeer → WithHandler → peer discovery → Send → handler fires.
// No internal packages are imported — this is the contract test for the note API.
func TestTwoNodes_SendReceive(t *testing.T) {
	type msg struct{ Text string }

	received := make(chan msg, 1)
	receiver := newTestPeer(t,
		note.WithHandler("test/1.0", func(_, _ string, decode func(any) error) error {
			var m msg
			if err := decode(&m); err != nil {
				return err
			}
			received <- m
			return nil
		}),
	)

	connected := make(chan string, 1)
	sender := newTestPeer(t,
		note.WithBootstrap(receiver.Addr()),
		note.WithPeerConnected(func(peerID string) {
			select {
			case connected <- peerID:
			default:
			}
		}),
	)

	select {
	case <-connected:
	case <-time.After(10 * time.Second):
		t.Fatal("sender did not connect to receiver within 10s")
	}

	if _, err := sender.Send(receiver.ID(), note.Msg("test/1.0", "TEXT", msg{Text: "hello"})); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case got := <-received:
		if got.Text != "hello" {
			t.Errorf("received %q, want %q", got.Text, "hello")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not fire within 5s")
	}
}

// TestTwoNodes_Broadcast exercises Broadcast and confirms return count.
func TestTwoNodes_Broadcast(t *testing.T) {
	received := make(chan struct{}, 1)
	receiver := newTestPeer(t,
		note.WithHandler("test/1.0", func(_, _ string, _ func(any) error) error {
			select {
			case received <- struct{}{}:
			default:
			}
			return nil
		}),
	)

	connected := make(chan struct{}, 1)
	sender := newTestPeer(t,
		note.WithBootstrap(receiver.Addr()),
		note.WithPeerConnected(func(_ string) {
			select {
			case connected <- struct{}{}:
			default:
			}
		}),
	)

	select {
	case <-connected:
	case <-time.After(10 * time.Second):
		t.Fatal("did not connect within 10s")
	}

	if n := sender.Broadcast(note.Msg("test/1.0", "PING", struct{}{})); n != 1 {
		t.Errorf("Broadcast returned %d, want 1", n)
	}

	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("broadcast handler did not fire within 5s")
	}
}

func TestNewVerifiedPeer_RunsOnCreation(t *testing.T) {
	kp, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate keypair: %v", err)
	}
	p, err := note.NewVerifiedPeer(kp, "127.0.0.1:29950")
	if err != nil {
		t.Fatalf("NewVerifiedPeer: %v", err)
	}
	defer p.Close()

	if p.ID() != kp.NodeID {
		t.Errorf("ID() = %q, want %q", p.ID(), kp.NodeID)
	}
}
