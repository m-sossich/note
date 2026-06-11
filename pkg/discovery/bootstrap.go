package discovery

import (
	"encoding/base64"
	"log/slog"
)

// marshalAndSend is fire-and-forget; errors are logged.
func (d *Discovery) marshalAndSend(addr string, msg any, msgName string) {
	data, err := marshalMsg(msg, d.cfg.Codec)
	if err != nil {
		slog.Warn("discovery: marshal failed", "msg", msgName, "err", err)
		return
	}
	if err := d.tr.SendTo(addr, data); err != nil {
		slog.Warn("discovery: send failed", "msg", msgName, "addr", addr, "err", err)
	}
}

// sendAnnounce includes the protocol list. In verified mode, the message is signed.
func (d *Discovery) sendAnnounce(addr string) {
	var pubKeyB64, sigB64 string
	if kp := d.cfg.Keypair; kp != nil {
		data := announceSignData(d.cfg.NodeID, d.cfg.advertiseAddr())
		pubKeyB64 = base64.StdEncoding.EncodeToString(kp.PublicKey)
		sigB64 = base64.StdEncoding.EncodeToString(kp.Sign(data))
	}
	d.marshalAndSend(addr, newAnnounceMsg(
		d.cfg.NodeID,
		d.cfg.advertiseAddr(),
		pubKeyB64,
		sigB64,
		d.protocols(),
	), "ANNOUNCE")
}

func (d *Discovery) sendFindPeers(addr string) {
	d.marshalAndSend(addr, newFindPeersMsg(d.cfg.NodeID), "FIND_PEERS")
}

func (d *Discovery) handlePeers(msg peersMsg) {
	for _, p := range msg.Peers {
		if p.NodeID == d.cfg.NodeID {
			continue // never add self
		}
		added := d.table.Add(p.NodeID, p.Address, p.Protocols)
		if added {
			d.emitEvent(PeerEvent{
				Type:      PeerFound,
				PeerID:    p.NodeID,
				Address:   p.Address,
				Protocols: p.Protocols,
			})
		}
	}
}

func (d *Discovery) sendPong(toAddr string, nonce string) {
	d.marshalAndSend(toAddr, newPongMsg(d.cfg.NodeID, nonce), "PONG")
}
