package dht

import (
	"fmt"
	"testing"
)

func TestRoutingTable_UpdateAndFindClosest(t *testing.T) {
	local := KeyFromString("local")
	rt := newRoutingTable(local, defaultBucketSize)

	for i := 0; i < 20; i++ {
		n := makeNode(fmt.Sprintf("peer-%d", i))
		rt.Update(n, nopPing)
	}

	target := KeyFromString("peer-0")
	closest := rt.FindClosest(target, defaultBucketSize)
	if len(closest) == 0 {
		t.Fatal("FindClosest returned empty list")
	}
	if closest[0].NodeID != "peer-0" {
		t.Errorf("closest[0] = %q, want peer-0", closest[0].NodeID)
	}
}

func TestRoutingTable_FindClosest_OrderedByDistance(t *testing.T) {
	local := KeyFromString("local-node")
	rt := newRoutingTable(local, defaultBucketSize)

	nodes := []NodeInfo{
		makeNode("alpha"),
		makeNode("beta"),
		makeNode("gamma"),
		makeNode("delta"),
	}
	for _, n := range nodes {
		rt.Update(n, nopPing)
	}

	target := KeyFromString("alpha")
	result := rt.FindClosest(target, 4)

	for i := 1; i < len(result); i++ {
		d0 := XOR(result[i-1].Key, target)
		d1 := XOR(result[i].Key, target)
		if Less(d1, d0) {
			t.Errorf("result[%d] is closer than result[%d] — ordering violated", i, i-1)
		}
	}
}

func TestRoutingTable_FindClosest_LimitN(t *testing.T) {
	local := KeyFromString("local")
	rt := newRoutingTable(local, defaultBucketSize)

	for i := 0; i < 20; i++ {
		rt.Update(makeNode(fmt.Sprintf("n%d", i)), nopPing)
	}

	result := rt.FindClosest(KeyFromString("target"), 5)
	if len(result) > 5 {
		t.Errorf("FindClosest(n=5) returned %d nodes", len(result))
	}
}

func TestRoutingTable_Remove(t *testing.T) {
	local := KeyFromString("local")
	rt := newRoutingTable(local, defaultBucketSize)

	n := makeNode("victim")
	rt.Update(n, nopPing)

	before := rt.FindClosest(n.Key, 10)
	found := false
	for _, p := range before {
		if p.NodeID == "victim" {
			found = true
		}
	}
	if !found {
		t.Fatal("victim not found before Remove")
	}

	rt.Remove(n)

	after := rt.FindClosest(n.Key, 10)
	for _, p := range after {
		if p.NodeID == "victim" {
			t.Error("victim still present after Remove")
		}
	}
}

func TestRoutingTable_BucketIndex(t *testing.T) {
	local := KeyFromString("local")
	rt := newRoutingTable(local, defaultBucketSize)

	cases := []struct {
		key  DHTKey
		desc string
	}{
		{local, "equal keys → index 255"},
	}

	if idx := rt.bucketIndex(cases[0].key); idx != 255 {
		t.Errorf("bucketIndex(local) = %d, want 255", idx)
	}

	differentLast := local
	differentLast[31] ^= 0x01
	idx := rt.bucketIndex(differentLast)
	if idx != 255 {
		t.Errorf("bucketIndex(differ-last-bit) = %d, want 255", idx)
	}

	differentFirst := local
	if differentFirst[0]&0x80 == 0 {
		differentFirst[0] |= 0x80
	} else {
		differentFirst[0] &^= 0x80
	}
	idx = rt.bucketIndex(differentFirst)
	if idx != 0 {
		t.Errorf("bucketIndex(differ-first-bit) = %d, want 0", idx)
	}
}

func TestRoutingTable_FindClosest_Empty(t *testing.T) {
	rt := newRoutingTable(KeyFromString("local"), defaultBucketSize)
	result := rt.FindClosest(KeyFromString("target"), defaultBucketSize)
	if len(result) != 0 {
		t.Errorf("expected empty result for empty table, got %d", len(result))
	}
}
