package discovery_test

import (
	"net"
	"testing"
	"time"

	"github.com/m-sossich/note/pkg/discovery"
	"github.com/m-sossich/note/pkg/transport/udp"
)

func requireIPv6Discovery(t *testing.T) {
	t.Helper()
	conn, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.ParseIP("::1"), Port: 0})
	if err != nil {
		t.Skip("IPv6 not available:", err)
	}
	conn.Close()
}

func freeIPv6Addr(t *testing.T) string {
	t.Helper()
	conn, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.ParseIP("::1"), Port: 0})
	if err != nil {
		t.Fatalf("freeIPv6Addr: %v", err)
	}
	addr := conn.LocalAddr().String()
	conn.Close()
	return addr
}

// TestDiscovery_IPv6_AnnounceAndFindPeers verifies the full announce → find_peers
// → peer_found flow works correctly over IPv6 loopback.
func TestDiscovery_IPv6_AnnounceAndFindPeers(t *testing.T) {
	requireIPv6Discovery(t)

	bsAddr := freeIPv6Addr(t)
	nodeAddr := freeIPv6Addr(t)

	bsTr, err := udp.New(bsAddr)
	if err != nil {
		t.Fatalf("bootstrap transport [::1]: %v", err)
	}
	bs, err := discovery.New(discovery.Config{
		NodeID:        "ipv6-bootstrap",
		BindAddr:      bsAddr,
		AdvertiseAddr: bsAddr,
		PingInterval:  10 * time.Second,
		PingMaxMissed: 3,
	}, bsTr)
	if err != nil {
		t.Fatal(err)
	}
	bs.Start(nil)
	defer bs.Stop()

	nodeTr, err := udp.New(nodeAddr)
	if err != nil {
		t.Fatalf("node transport [::1]: %v", err)
	}
	nd, err := discovery.New(discovery.Config{
		NodeID:         "ipv6-node-1",
		BindAddr:       nodeAddr,
		AdvertiseAddr:  nodeAddr,
		BootstrapAddrs: []string{bsAddr},
		PingInterval:   10 * time.Second,
		PingMaxMissed:  3,
	}, nodeTr)
	if err != nil {
		t.Fatal(err)
	}
	nd.Start(nil)
	defer nd.Stop()

	select {
	case ev := <-bs.Events():
		if ev.Type != discovery.PeerFound || ev.PeerID != "ipv6-node-1" {
			t.Errorf("unexpected event: %+v", ev)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout: bootstrap did not see ipv6-node-1 announce over IPv6")
	}
}
