package udp

import (
	"bytes"
	"net"
	"testing"
	"time"
)

func requireIPv6UDP(t *testing.T) {
	t.Helper()
	conn, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.ParseIP("::1"), Port: 0})
	if err != nil {
		t.Skip("IPv6 UDP not available:", err)
	}
	conn.Close()
}

func freeIPv6UDPAddr(t *testing.T) string {
	t.Helper()
	conn, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.ParseIP("::1"), Port: 0})
	if err != nil {
		t.Fatalf("freeIPv6UDPAddr: %v", err)
	}
	addr := conn.LocalAddr().String()
	conn.Close()
	return addr
}

// TestUDP_IPv6_RoundTrip verifies that the UDP transport sends and receives
// datagrams correctly over an IPv6 loopback connection.
func TestUDP_IPv6_RoundTrip(t *testing.T) {
	requireIPv6UDP(t)

	addrA := freeIPv6UDPAddr(t)
	addrB := freeIPv6UDPAddr(t)

	a, err := New(addrA)
	if err != nil {
		t.Fatalf("New(A) [::1]: %v", err)
	}
	defer a.Close()

	b, err := New(addrB)
	if err != nil {
		t.Fatalf("New(B) [::1]: %v", err)
	}
	defer b.Close()

	want := []byte("hello ipv6 udp")
	recvCh := make(chan []byte, 1)
	errCh := make(chan error, 1)

	go func() {
		_, data, err := b.ReceiveFrom()
		if err != nil {
			errCh <- err
			return
		}
		recvCh <- data
	}()

	if err := a.SendTo(addrB, want); err != nil {
		t.Fatalf("SendTo [::1]: %v", err)
	}

	select {
	case got := <-recvCh:
		if !bytes.Equal(got, want) {
			t.Errorf("received %q, want %q", got, want)
		}
	case err := <-errCh:
		t.Fatalf("ReceiveFrom: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for IPv6 UDP packet")
	}
}
