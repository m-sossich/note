package discovery

import (
	"log/slog"
	"time"

	"github.com/google/uuid"
)

func (d *Discovery) livenessLoop() {
	defer d.wg.Done()
	ticker := time.NewTicker(d.cfg.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			d.livenessTick()
		case <-d.stopCh:
			return
		}
	}
}

// livenessTick evicts first (synchronously), then pings — order matters.
func (d *Discovery) livenessTick() {
	d.evictMissedPeers()
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.pingActivePeers()
	}()
}

func (d *Discovery) evictMissedPeers() {
	evicted := d.table.CheckAndIncrementMissed(d.cfg.PingMaxMissed)
	for _, p := range evicted {
		slog.Warn("discovery: peer evicted (missed pings)", "peer_id", p.NodeID, "addr", p.Address)
		d.emitEvent(PeerEvent{Type: PeerLost, PeerID: p.NodeID, Address: p.Address})
	}
}

// pingActivePeers replaces any outstanding nonce per peer; checks stopCh between sends.
func (d *Discovery) pingActivePeers() {
	for _, p := range d.table.List() {
		select {
		case <-d.stopCh:
			return
		default:
		}
		nonce := uuid.New().String()
		d.pendingMu.Lock()
		if old, exists := d.pendingByPeer[p.NodeID]; exists {
			delete(d.pending, old)
		}
		d.pending[nonce] = p.NodeID
		d.pendingByPeer[p.NodeID] = nonce
		d.pendingMu.Unlock()
		d.table.MarkPingSent(p.NodeID)
		msg := newPingMsg(d.cfg.NodeID, nonce)
		data, err := marshalMsg(msg, d.cfg.Codec)
		if err != nil {
			slog.Warn("discovery: marshal ping failed", "peer_id", p.NodeID, "err", err)
			continue
		}
		if err := d.tr.SendTo(p.Address, data); err != nil {
			slog.Warn("discovery: send ping failed", "peer_id", p.NodeID, "addr", p.Address, "err", err)
		}
	}
}

func (d *Discovery) handlePong(msg pongMsg) {
	d.pendingMu.Lock()
	nodeID, ok := d.pending[msg.Nonce]
	if ok {
		delete(d.pending, msg.Nonce)
		delete(d.pendingByPeer, nodeID)
	}
	d.pendingMu.Unlock()
	if !ok {
		return
	}
	d.table.HandlePongReceived(nodeID)
}
