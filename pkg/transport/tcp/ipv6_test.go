package tcp

import (
	"net"
	"testing"
)

func requireIPv6TCP(t *testing.T) {
	t.Helper()
	l, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skip("IPv6 not available:", err)
	}
	l.Close()
}

// TestTransport_IPv6_DialAndReceive verifies that the TCP transport correctly
// sends and receives framed messages over an IPv6 loopback connection.
func TestTransport_IPv6_DialAndReceive(t *testing.T) {
	requireIPv6TCP(t)

	tr := New(0)
	ln, err := tr.Listen("[::1]:0")
	if err != nil {
		t.Fatalf("Listen [::1]:0: %v", err)
	}
	defer ln.Close()

	want := []byte("hello ipv6")
	received := make(chan []byte, 1)

	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		data, err := c.Receive()
		if err != nil {
			received <- nil
			return
		}
		received <- data
	}()

	client, err := tr.Dial(ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial %s: %v", ln.Addr(), err)
	}
	defer client.Close()

	if _, err := client.Send(want); err != nil {
		t.Fatalf("Send: %v", err)
	}

	got := <-received
	if string(got) != string(want) {
		t.Errorf("received %q, want %q", got, want)
	}
}
