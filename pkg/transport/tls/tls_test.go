package tlstransport_test

import (
	"testing"
	"time"

	"github.com/m-sossich/note/pkg/identity"
	tlstransport "github.com/m-sossich/note/pkg/transport/tls"
)

// TestTLSTransport_ConnectAndExchange verifies full message delivery over TLS.
// The server goroutine drives the TLS handshake by calling Receive — mirroring
// what acceptLoop's goroutine does in production (handleInbound → handshaker).
func TestTLSTransport_ConnectAndExchange(t *testing.T) {
	kpA, _ := identity.Generate()
	kpB, _ := identity.Generate()

	trA := tlstransport.New(kpA, 0)
	trB := tlstransport.New(kpB, 0)

	ln, err := trA.Listen("127.0.0.1:19900")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	type serverResult struct {
		data []byte
		err  error
	}
	srv := make(chan serverResult, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			srv <- serverResult{nil, err}
			return
		}
		defer c.Close()
		// Calling Receive drives the lazy TLS handshake on the server side,
		// allowing the client's Dial/Handshake to complete concurrently.
		data, err := c.Receive()
		srv <- serverResult{data, err}
	}()

	clientConn, err := trB.Dial("127.0.0.1:19900")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer clientConn.Close()

	want := []byte("hello verified network")
	if _, err := clientConn.Send(want); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case r := <-srv:
		if r.err != nil {
			t.Fatalf("server receive: %v", r.err)
		}
		if string(r.data) != string(want) {
			t.Errorf("got %q, want %q", r.data, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for server to receive message")
	}
}

// TestTLSTransport_AcceptsValidPeer verifies a second valid keypair connects
// successfully — both sides pass verifyP2PCert.
func TestTLSTransport_AcceptsValidPeer(t *testing.T) {
	kpA, _ := identity.Generate()
	kpB, _ := identity.Generate()

	trA := tlstransport.New(kpA, 0)
	trB := tlstransport.New(kpB, 0)

	ln, err := trA.Listen("127.0.0.1:19901")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	accepted := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			accepted <- err
			return
		}
		// Drive the handshake — Receive triggers the lazy TLS exchange.
		// The client will close after Dial completes, producing EOF here.
		c.Receive()
		c.Close()
		accepted <- nil
	}()

	c, err := trB.Dial("127.0.0.1:19901")
	if err != nil {
		t.Fatalf("valid keypair was rejected: %v", err)
	}
	c.Close()

	select {
	case err := <-accepted:
		if err != nil {
			t.Fatalf("server error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}
