package dht

import "sync"

// NodeInfo is the identity and address of a DHT node. PublicKey is nil in trusted mode.
// In verified mode, NodeID == SHA-256(PublicKey), verified by wireToNodeInfo.
type NodeInfo struct {
	NodeID    string
	Address   string
	Key       DHTKey
	PublicKey []byte
}

// kBucket is a bounded LRU list. Index 0 = least-recently-seen; tail = most-recently-seen.
type kBucket struct {
	nodes []NodeInfo
	mu    sync.RWMutex
}

// Add inserts or refreshes node. When full, pings the LRS: evicts it on ping=false, discards node on ping=true.
func (b *kBucket) Add(node NodeInfo, bucketSize int, ping func(NodeInfo) bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if i, found := b.findIndex(node.NodeID); found {
		b.moveToTail(i, node)
		return
	}

	if len(b.nodes) < bucketSize {
		b.nodes = append(b.nodes, node)
		return
	}

	lrs := b.nodes[0]
	if ping(lrs) {
		// LRS is alive — keep it, discard new node.
		b.nodes = append(b.nodes[1:], lrs)
		return
	}
	b.nodes = append(b.nodes[1:], node)
}

func (b *kBucket) Remove(nodeID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if i, found := b.findIndex(nodeID); found {
		b.nodes = append(b.nodes[:i], b.nodes[i+1:]...)
	}
}

// findIndex returns the index of nodeID. Must hold b.mu.
func (b *kBucket) findIndex(nodeID string) (int, bool) {
	for i, n := range b.nodes {
		if n.NodeID == nodeID {
			return i, true
		}
	}
	return -1, false
}

// moveToTail must hold b.mu.
func (b *kBucket) moveToTail(i int, node NodeInfo) {
	b.nodes = append(b.nodes[:i], b.nodes[i+1:]...)
	b.nodes = append(b.nodes, node)
}

func (b *kBucket) Nodes() []NodeInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()
	result := make([]NodeInfo, len(b.nodes))
	copy(result, b.nodes)
	return result
}
