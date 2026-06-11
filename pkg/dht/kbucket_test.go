package dht

import (
	"fmt"
	"testing"
)

func makeNode(id string) NodeInfo {
	return NodeInfo{
		NodeID:  id,
		Address: id + ":9000",
		Key:     KeyFromString(id),
	}
}

func nopPing(_ NodeInfo) bool { return false }

func TestKBucket_AddAndLen(t *testing.T) {
	var b kBucket
	b.Add(makeNode("n1"), defaultBucketSize, nopPing)
	b.Add(makeNode("n2"), defaultBucketSize, nopPing)
	if len(b.Nodes()) != 2 {
		t.Errorf("Len = %d, want 2", len(b.Nodes()))
	}
}

func TestKBucket_RefreshExisting(t *testing.T) {
	var b kBucket
	n := makeNode("n1")
	b.Add(n, defaultBucketSize, nopPing)
	b.Add(makeNode("n2"), defaultBucketSize, nopPing)
	// Add n1 again to refresh it — should move to tail.
	b.Add(n, defaultBucketSize, nopPing)
	if len(b.Nodes()) != 2 {
		t.Errorf("Len after refresh = %d, want 2", len(b.Nodes()))
	}
	nodes := b.Nodes()
	if nodes[len(nodes)-1].NodeID != "n1" {
		t.Errorf("refreshed node should be at tail, got %q", nodes[len(nodes)-1].NodeID)
	}
}

func TestKBucket_Remove(t *testing.T) {
	var b kBucket
	b.Add(makeNode("n1"), defaultBucketSize, nopPing)
	b.Add(makeNode("n2"), defaultBucketSize, nopPing)
	b.Remove("n1")
	if len(b.Nodes()) != 1 {
		t.Errorf("Len after remove = %d, want 1", len(b.Nodes()))
	}
	nodes := b.Nodes()
	if nodes[0].NodeID != "n2" {
		t.Errorf("remaining node = %q, want n2", nodes[0].NodeID)
	}
}

func TestKBucket_Remove_NotPresent(t *testing.T) {
	var b kBucket
	b.Add(makeNode("n1"), defaultBucketSize, nopPing)
	b.Remove("n99") // should not panic
	if len(b.Nodes()) != 1 {
		t.Errorf("Len after removing absent node = %d, want 1", len(b.Nodes()))
	}
}

func TestKBucket_Full_PingFalse_EvictsLRS(t *testing.T) {
	var b kBucket
	for i := 0; i < defaultBucketSize; i++ {
		b.Add(makeNode(fmt.Sprintf("node-%d", i)), defaultBucketSize, nopPing)
	}
	if len(b.Nodes()) != defaultBucketSize {
		t.Fatalf("expected full bucket, got %d", len(b.Nodes()))
	}

	lrsBefore := b.Nodes()[0].NodeID

	newNode := makeNode("newcomer")
	pingCalled := false
	b.Add(newNode, defaultBucketSize, func(n NodeInfo) bool {
		pingCalled = true
		return false // LRS is dead
	})

	if !pingCalled {
		t.Error("ping was not called for full bucket")
	}
	if len(b.Nodes()) != defaultBucketSize {
		t.Errorf("Len after eviction = %d, want %d", len(b.Nodes()), defaultBucketSize)
	}
	found := false
	for _, n := range b.Nodes() {
		if n.NodeID == "newcomer" {
			found = true
		}
		if n.NodeID == lrsBefore {
			t.Errorf("LRS node %q should have been evicted", lrsBefore)
		}
	}
	if !found {
		t.Error("newcomer not found in bucket after eviction")
	}
}

func TestKBucket_Full_PingTrue_DiscardsNew(t *testing.T) {
	var b kBucket
	for i := 0; i < defaultBucketSize; i++ {
		b.Add(makeNode(fmt.Sprintf("node-%d", i)), defaultBucketSize, nopPing)
	}

	originalNodes := b.Nodes()

	b.Add(makeNode("newcomer"), defaultBucketSize, func(n NodeInfo) bool {
		return true // LRS is alive
	})

	if len(b.Nodes()) != defaultBucketSize {
		t.Errorf("Len = %d, want %d", len(b.Nodes()), defaultBucketSize)
	}
	for _, n := range b.Nodes() {
		if n.NodeID == "newcomer" {
			t.Error("newcomer should have been discarded (LRS alive)")
		}
	}
	nodeSet := make(map[string]struct{}, defaultBucketSize)
	for _, n := range b.Nodes() {
		nodeSet[n.NodeID] = struct{}{}
	}
	for _, orig := range originalNodes {
		if _, ok := nodeSet[orig.NodeID]; !ok {
			t.Errorf("original node %q missing after discard", orig.NodeID)
		}
	}
}

func TestKBucket_Nodes_ReturnsCopy(t *testing.T) {
	var b kBucket
	b.Add(makeNode("n1"), defaultBucketSize, nopPing)
	nodes := b.Nodes()
	nodes[0].NodeID = "mutated"
	if b.Nodes()[0].NodeID != "n1" {
		t.Error("Nodes() did not return a copy — mutation affected bucket")
	}
}
