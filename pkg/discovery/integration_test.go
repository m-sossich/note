package discovery_test

import (
	"testing"
	"time"

	"github.com/m-sossich/note/pkg/discovery"
	"github.com/m-sossich/note/pkg/transport/udp"
)

func TestDiscovery_AnnounceAndFindPeers(t *testing.T) {
	// Bootstrap node
	bsTr, err := udp.New("127.0.0.1:19100")
	if err != nil {
		t.Fatalf("bootstrap transport: %v", err)
	}
	bs, err := discovery.New(discovery.Config{
		NodeID:        "bootstrap",
		BindAddr:      "127.0.0.1:19100",
		PingInterval:  10 * time.Second,
		PingMaxMissed: 3,
	}, bsTr)
	if err != nil {
		t.Fatal(err)
	}
	bs.Start(nil)
	defer bs.Stop()

	// Regular node
	nodeTr, err := udp.New("127.0.0.1:19101")
	if err != nil {
		t.Fatalf("node transport: %v", err)
	}
	nd, err := discovery.New(discovery.Config{
		NodeID:         "node-1",
		BindAddr:       "127.0.0.1:19101",
		BootstrapAddrs: []string{"127.0.0.1:19100"},
		PingInterval:   10 * time.Second,
		PingMaxMissed:  3,
	}, nodeTr)
	if err != nil {
		t.Fatal(err)
	}
	nd.Start(nil)
	defer nd.Stop()

	// Bootstrap should emit PeerFound for node-1 after it announces.
	select {
	case ev := <-bs.Events():
		if ev.Type != discovery.PeerFound || ev.PeerID != "node-1" {
			t.Errorf("unexpected event: %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: bootstrap did not see node-1 announce")
	}
}

func TestDiscovery_PeerEviction(t *testing.T) {
	bsTr, _ := udp.New("127.0.0.1:19200")
	bs, _ := discovery.New(discovery.Config{
		NodeID:        "bootstrap",
		BindAddr:      "127.0.0.1:19200",
		PingInterval:  100 * time.Millisecond,
		PingMaxMissed: 2,
	}, bsTr)
	bs.Start(nil)
	defer bs.Stop()

	nodeTr, _ := udp.New("127.0.0.1:19201")
	nd, _ := discovery.New(discovery.Config{
		NodeID:         "node-evict",
		BindAddr:       "127.0.0.1:19201",
		BootstrapAddrs: []string{"127.0.0.1:19200"},
		PingInterval:   10 * time.Second,
		PingMaxMissed:  2,
	}, nodeTr)
	nd.Start(nil)

	// Wait for peer to be discovered.
	select {
	case <-bs.Events():
	case <-time.After(2 * time.Second):
		t.Fatal("node not discovered by bootstrap")
	}

	// Stop the node (no more PONGs).
	nd.Stop()
	nodeTr.Close()

	// Bootstrap should emit PeerLost after PingMaxMissed ticks.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev := <-bs.Events():
			if ev.Type == discovery.PeerLost && ev.PeerID == "node-evict" {
				return // test passed
			}
		case <-deadline:
			t.Fatal("timeout: bootstrap did not emit PeerLost for stopped node")
		}
	}
}
