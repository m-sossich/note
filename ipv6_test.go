package note_test

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	note "github.com/m-sossich/note"
)

func requireIPv6(t *testing.T) {
	t.Helper()
	l, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skip("IPv6 not available:", err)
	}
	l.Close()
}

func freeIPv6PeerAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Fatalf("freeIPv6PeerAddr: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func newIPv6TestPeer(t *testing.T, addr string, opts ...note.Option) *note.Peer {
	t.Helper()
	opts = append([]note.Option{
		note.WithIdentityPath(filepath.Join(t.TempDir(), "node.id")),
		note.WithAdvertiseAddr(addr),
	}, opts...)
	p, err := note.NewPeer(addr, opts...)
	if err != nil {
		t.Fatalf("NewPeer [::1]: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

// TestNewPeer_IPv6_BoundAddr verifies that NewPeer on an IPv6 address reports
// an IPv6 BoundAddr and a non-empty node ID.
func TestNewPeer_IPv6_BoundAddr(t *testing.T) {
	requireIPv6(t)

	addr := freeIPv6PeerAddr(t)
	p := newIPv6TestPeer(t, addr)

	if p.ID() == "" {
		t.Fatal("ID() should be non-empty")
	}
	host, _, err := net.SplitHostPort(p.Addr())
	if err != nil {
		t.Fatalf("SplitHostPort(%q): %v", p.Addr(), err)
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.To4() != nil {
		t.Errorf("expected IPv6 BoundAddr, got %q", p.Addr())
	}
}

// TestTwoNodes_IPv6_SendReceive exercises the full peer connect → send →
// handler flow over IPv6 loopback.
func TestTwoNodes_IPv6_SendReceive(t *testing.T) {
	requireIPv6(t)

	type msg struct{ Text string }

	receiverAddr := freeIPv6PeerAddr(t)
	received := make(chan msg, 1)
	receiver := newIPv6TestPeer(t, receiverAddr,
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
	senderAddr := freeIPv6PeerAddr(t)
	sender := newIPv6TestPeer(t, senderAddr,
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
		t.Fatal("sender did not connect to IPv6 receiver within 10s")
	}

	if _, err := sender.Send(receiver.ID(), note.Msg("test/1.0", "TEXT", msg{Text: "hello ipv6"})); err != nil {
		t.Fatalf("Send over IPv6: %v", err)
	}

	select {
	case got := <-received:
		if got.Text != "hello ipv6" {
			t.Errorf("received %q, want %q", got.Text, "hello ipv6")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not fire over IPv6 within 5s")
	}
}
