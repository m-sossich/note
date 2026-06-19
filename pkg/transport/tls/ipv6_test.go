package tlstransport_test

import (
	"net"
	"testing"
	"time"

	"github.com/m-sossich/note/pkg/identity"
	tlstransport "github.com/m-sossich/note/pkg/transport/tls"
)

func requireIPv6TLS(t *testing.T) {
	t.Helper()
	l, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skip("IPv6 not available:", err)
	}
	l.Close()
}

// TestTLSTransport_IPv6_ConnectAndExchange verifies that verified-mode mTLS
// works correctly over an IPv6 loopback connection.
func TestTLSTransport_IPv6_ConnectAndExchange(t *testing.T) {
	requireIPv6TLS(t)

	kpA, _ := identity.Generate()
	kpB, _ := identity.Generate()

	trA := tlstransport.New(kpA, 0)
	trB := tlstransport.New(kpB, 0)

	ln, err := trA.Listen("[::1]:0")
	if err != nil {
		t.Fatalf("Listen [::1]:0: %v", err)
	}
	defer ln.Close()

	want := []byte("hello ipv6 tls")
	srv := make(chan []byte, 1)

	go func() {
		c, err := ln.Accept()
		if err != nil {
			srv <- nil
			return
		}
		defer c.Close()
		data, err := c.Receive()
		if err != nil {
			srv <- nil
			return
		}
		srv <- data
	}()

	client, err := trB.Dial(ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial %s: %v", ln.Addr(), err)
	}
	defer client.Close()

	if _, err := client.Send(want); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case got := <-srv:
		if string(got) != string(want) {
			t.Errorf("received %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for server receive")
	}
}
