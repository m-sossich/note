package discovery

import (
	"fmt"
	"sync"
	"testing"
)

func TestPeerTable_AddAndList(t *testing.T) {
	table := newPeerTable(0)
	added, _ := table.Add("node-1", "127.0.0.1:9001", nil)
	if !added {
		t.Fatal("expected Add to return true for new peer")
	}
	peers := table.List()
	if len(peers) != 1 || peers[0].NodeID != "node-1" {
		t.Errorf("unexpected peers: %+v", peers)
	}
}

func TestPeerTable_AddDuplicate(t *testing.T) {
	table := newPeerTable(0)
	table.Add("node-1", "127.0.0.1:9001", nil)
	added, _ := table.Add("node-1", "127.0.0.1:9001", nil)
	if added {
		t.Error("expected Add to return false for duplicate peer")
	}
	if len(table.List()) != 1 {
		t.Error("duplicate add should not create two entries")
	}
}

func TestPeerTable_Remove(t *testing.T) {
	table := newPeerTable(0)
	table.Add("node-1", "127.0.0.1:9001", nil)
	table.Remove("node-1")
	if len(table.List()) != 0 {
		t.Error("expected empty table after Remove")
	}
}

func TestPeerTable_EvictionAtThreshold(t *testing.T) {
	table := newPeerTable(0)
	table.Add("node-1", "127.0.0.1:9001", nil)
	table.MarkPingSent("node-1")

	// First two ticks: increment but don't evict (threshold = 3)
	for i := 0; i < 2; i++ {
		evicted := table.CheckAndIncrementMissed(3)
		if len(evicted) != 0 {
			t.Fatalf("tick %d: expected no eviction, got %v", i+1, evicted)
		}
		table.MarkPingSent("node-1") // mark again for next tick
	}

	// Third tick: should evict
	evicted := table.CheckAndIncrementMissed(3)
	if len(evicted) != 1 || evicted[0].NodeID != "node-1" {
		t.Errorf("expected eviction of node-1, got %+v", evicted)
	}
	if len(table.List()) != 0 {
		t.Error("evicted peer should be removed from table")
	}
}

func TestPeerTable_ResetMissedCounter(t *testing.T) {
	table := newPeerTable(0)
	table.Add("node-1", "127.0.0.1:9001", nil)
	table.MarkPingSent("node-1")
	table.CheckAndIncrementMissed(3) // missed = 1
	table.MarkPingSent("node-1")
	table.HandlePongReceived("node-1") // reset

	// Should not evict even after 3 more ticks without response
	for i := 0; i < 3; i++ {
		table.MarkPingSent("node-1")
		evicted := table.CheckAndIncrementMissed(3)
		if i < 2 && len(evicted) != 0 {
			t.Fatalf("early eviction at tick %d", i+1)
		}
	}
}

func TestPeerTable_ConcurrentAccess(t *testing.T) {
	table := newPeerTable(0)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("node-%d", i)
			table.Add(id, "127.0.0.1:9000", nil)
			table.Remove(id)
		}(i)
	}
	wg.Wait()
}

func TestPeerTable_MarkPingSent_NonExistent(t *testing.T) {
	table := newPeerTable(0)
	// Must not panic when nodeID is not in the table.
	table.MarkPingSent("ghost-node")
}

func TestPeerTable_HandlePongReceived_NonExistent(t *testing.T) {
	table := newPeerTable(0)
	// Must not panic when nodeID is not in the table.
	table.HandlePongReceived("ghost-node")
}

func TestPeerTable_CheckAndIncrementMissed_EmptyTable(t *testing.T) {
	table := newPeerTable(0)
	evicted := table.CheckAndIncrementMissed(3)
	if len(evicted) != 0 {
		t.Errorf("empty table: expected no evictions, got %v", evicted)
	}
}

func TestPeerTable_CheckAndIncrementMissed_NoPendingPing(t *testing.T) {
	table := newPeerTable(0)
	table.Add("node-1", "127.0.0.1:9001", nil)
	// pingPending is false by default — missed counter must NOT be incremented.
	for i := 0; i < 10; i++ {
		evicted := table.CheckAndIncrementMissed(3)
		if len(evicted) != 0 {
			t.Fatalf("tick %d: unexpected eviction when pingPending=false", i+1)
		}
	}
	if len(table.List()) != 1 {
		t.Error("peer should still be in table after ticks with no pending ping")
	}
}

// TestPeerTable_Cap_EvictsOnFull verifies that adding a peer to a full table
// evicts the peer with the highest missed count.
func TestPeerTable_Cap_EvictsOnFull(t *testing.T) {
	table := newPeerTable(2)
	table.Add("node-1", "127.0.0.1:9001", nil)
	table.Add("node-2", "127.0.0.1:9002", nil)

	// Give node-1 a higher missed count.
	table.MarkPingSent("node-1")
	table.CheckAndIncrementMissed(99) // increment without evicting (threshold=99)

	added, evicted := table.Add("node-3", "127.0.0.1:9003", nil)
	if !added {
		t.Fatal("expected Add to succeed when eviction makes room")
	}
	if evicted == nil || evicted.NodeID != "node-1" {
		t.Errorf("expected node-1 (highest missed) to be evicted, got %+v", evicted)
	}
	if _, ok := table.Get("node-3"); !ok {
		t.Error("node-3 should be in the table after eviction")
	}
	if _, ok := table.Get("node-1"); ok {
		t.Error("node-1 should have been evicted")
	}
}

// TestPeerTable_Cap_RandomTiebreakerNoPanic verifies that random tiebreaking
// among peers with equal missed counts does not panic and evicts exactly one.
func TestPeerTable_Cap_RandomTiebreakerNoPanic(t *testing.T) {
	table := newPeerTable(3)
	table.Add("node-1", "127.0.0.1:9001", nil)
	table.Add("node-2", "127.0.0.1:9002", nil)
	table.Add("node-3", "127.0.0.1:9003", nil)

	// All peers have missed=0 — tiebreaker must pick one randomly.
	added, evicted := table.Add("node-4", "127.0.0.1:9004", nil)
	if !added {
		t.Fatal("expected Add to succeed")
	}
	if evicted == nil {
		t.Fatal("expected exactly one peer to be evicted")
	}
	if len(table.List()) != 3 {
		t.Errorf("table should have 3 peers after eviction+insert, got %d", len(table.List()))
	}
}

// TestPeerTable_Cap_NoDuplicateEviction verifies that re-adding an existing peer
// does not trigger eviction even when the table is full.
func TestPeerTable_Cap_NoDuplicateEviction(t *testing.T) {
	table := newPeerTable(2)
	table.Add("node-1", "127.0.0.1:9001", nil)
	table.Add("node-2", "127.0.0.1:9002", nil)

	added, evicted := table.Add("node-1", "127.0.0.1:9001", nil) // duplicate
	if added {
		t.Error("duplicate Add should return added=false")
	}
	if evicted != nil {
		t.Error("duplicate Add should not evict anyone")
	}
	if len(table.List()) != 2 {
		t.Errorf("table size should remain 2, got %d", len(table.List()))
	}
}

// TestPeerTable_Cap_Unbounded verifies that maxPeers=0 imposes no limit.
func TestPeerTable_Cap_Unbounded(t *testing.T) {
	table := newPeerTable(0)
	for i := range 200 {
		table.Add(fmt.Sprintf("node-%d", i), "127.0.0.1:9000", nil)
	}
	if len(table.List()) != 200 {
		t.Errorf("unbounded table should hold all 200 peers, got %d", len(table.List()))
	}
}

func TestPeerTable_Get_NonExistent(t *testing.T) {
	table := newPeerTable(0)
	info, ok := table.Get("no-such-node")
	if ok {
		t.Error("Get on non-existent node should return false")
	}
	if info.NodeID != "" || info.Address != "" || len(info.Protocols) != 0 {
		t.Errorf("Get on non-existent node should return zero PeerInfo, got %+v", info)
	}
}
