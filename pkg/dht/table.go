package dht

import "sort"

type routingTable struct {
	local      DHTKey
	bucketSize int
	buckets    [keySpaceBits]kBucket
}

func newRoutingTable(local DHTKey, bucketSize int) *routingTable {
	return &routingTable{local: local, bucketSize: bucketSize}
}

// bucketIndex returns CommonPrefixLen(local, key) capped at 255. Keys equal to local map to bucket 255.
func (t *routingTable) bucketIndex(key DHTKey) int {
	idx := CommonPrefixLen(t.local, key)
	if idx >= keySpaceBits {
		idx = keySpaceBits - 1
	}
	return idx
}

func (t *routingTable) Update(node NodeInfo, ping func(NodeInfo) bool) {
	idx := t.bucketIndex(node.Key)
	t.buckets[idx].Add(node, t.bucketSize, ping)
}

func (t *routingTable) FindClosest(key DHTKey, n int) []NodeInfo {
	type candidate struct {
		info NodeInfo
		dist DHTKey
	}

	var all []candidate
	for i := range t.buckets {
		for _, node := range t.buckets[i].Nodes() {
			all = append(all, candidate{node, XOR(node.Key, key)})
		}
	}

	sort.Slice(all, func(i, j int) bool {
		return Less(all[i].dist, all[j].dist)
	})

	if n > len(all) {
		n = len(all)
	}
	result := make([]NodeInfo, n)
	for i := 0; i < n; i++ {
		result[i] = all[i].info
	}
	return result
}

func (t *routingTable) Remove(node NodeInfo) {
	idx := t.bucketIndex(node.Key)
	t.buckets[idx].Remove(node.NodeID)
}

// LRSOfFullBuckets returns the least-recently-seen entry from each full k-bucket.
func (t *routingTable) LRSOfFullBuckets() []NodeInfo {
	var result []NodeInfo
	for i := range t.buckets {
		nodes := t.buckets[i].Nodes()
		if len(nodes) >= t.bucketSize {
			result = append(result, nodes[0])
		}
	}
	return result
}
