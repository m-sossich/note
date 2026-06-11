package discovery

import "sync"

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
	mu    sync.RWMutex
	peers map[string]*peerRecord
}

func newPeerTable() *peerTable {
	return &peerTable{peers: make(map[string]*peerRecord)}
}

func (t *peerTable) Add(nodeID, addr string, protocols []string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.peers[nodeID]; exists {
		return false
	}
	t.peers[nodeID] = &peerRecord{info: peerInfo{NodeID: nodeID, Address: addr, Protocols: protocols}}
	return true
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
