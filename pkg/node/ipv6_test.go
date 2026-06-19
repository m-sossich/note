package node_test

import (
	"net"
	"testing"

	jsoncdc "github.com/m-sossich/note/pkg/codec/json"
	"github.com/m-sossich/note/pkg/node"
	"github.com/m-sossich/note/pkg/node/identify"
	tcptransport "github.com/m-sossich/note/pkg/transport/tcp"
)

func requireIPv6Node(t *testing.T) {
	t.Helper()
	l, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skip("IPv6 not available:", err)
	}
	l.Close()
}

// TestNode_IPv6_BoundAddr verifies that a node started on [::1]:0 reports
// an IPv6 address from BoundAddr — confirming the transport binds correctly.
func TestNode_IPv6_BoundAddr(t *testing.T) {
	requireIPv6Node(t)

	n, err := node.New(node.Config{
		NodeID:     "ipv6-addr-test",
		ListenAddr: "[::1]:0",
		Transport:  tcptransport.New(0),
		Codec:      jsoncdc.New(),
		Handshaker: identify.New(identify.Config{}),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	addr := n.BoundAddr()
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("SplitHostPort(%q): %v", addr, err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		t.Fatalf("BoundAddr host %q is not a valid IP", host)
	}
	if ip.To4() != nil {
		t.Errorf("expected IPv6 address from [::1]:0 bind, got IPv4-mapped: %q", addr)
	}
}
