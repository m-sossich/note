package discovery

import (
	"math/rand"
	"sync"
)

type peerInfo struct {
	NodeID    string
	Address   string
	Protocols []string
}

type peerRecord struct {
	info        peerInfo
	missed      int
	pingPending bool
}

type peerTable struct {
	mu       sync.RWMutex
	peers    map[string]*peerRecord
	maxPeers int // 0 = unbounded
}

func newPeerTable(maxPeers int) *peerTable {
	return &peerTable{peers: make(map[string]*peerRecord), maxPeers: maxPeers}
}

// Add inserts a new peer. Returns (true, nil) on insertion, (false, nil) if the
// peer already exists. When the table is full, evicts the peer with the highest
// missed-ping count (random tiebreaker) and returns (true, evicted).
func (t *peerTable) Add(nodeID, addr string, protocols []string) (bool, *peerInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.peers[nodeID]; exists {
		return false, nil
	}
	var evicted *peerInfo
	if t.maxPeers > 0 && len(t.peers) >= t.maxPeers {
		evicted = t.evictLocked()
	}
	t.peers[nodeID] = &peerRecord{info: peerInfo{NodeID: nodeID, Address: addr, Protocols: protocols}}
	return true, evicted
}

// evictLocked removes the peer with the highest missed count; breaks ties randomly.
// Must be called with t.mu held.
func (t *peerTable) evictLocked() *peerInfo {
	maxMissed := -1
	for _, p := range t.peers {
		if p.missed > maxMissed {
			maxMissed = p.missed
		}
	}
	var candidates []*peerRecord
	for _, p := range t.peers {
		if p.missed == maxMissed {
			candidates = append(candidates, p)
		}
	}
	victim := candidates[rand.Intn(len(candidates))]
	delete(t.peers, victim.info.NodeID)
	info := victim.info
	return &info
}

func (t *peerTable) Remove(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.peers, nodeID)
}

func (t *peerTable) Get(nodeID string) (peerInfo, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	p, ok := t.peers[nodeID]
	if !ok {
		return peerInfo{}, false
	}
	return p.info, true
}

func (t *peerTable) List() []peerInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]peerInfo, 0, len(t.peers))
	for _, p := range t.peers {
		out = append(out, p.info)
	}
	return out
}

func (t *peerTable) MarkPingSent(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if p, ok := t.peers[nodeID]; ok {
		p.pingPending = true
	}
}

func (t *peerTable) HandlePongReceived(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if p, ok := t.peers[nodeID]; ok {
		p.pingPending = false
		p.missed = 0
	}
}

// CheckAndIncrementMissed evicts peers with unanswered pings that exceed maxMissed.
func (t *peerTable) CheckAndIncrementMissed(maxMissed int) []peerInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	var evicted []peerInfo
	for nodeID, p := range t.peers {
		if !p.pingPending {
			continue
		}
		p.missed++
		if p.missed >= maxMissed {
			evicted = append(evicted, p.info)
			delete(t.peers, nodeID)
		}
	}
	return evicted
}
