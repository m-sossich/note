package discovery

import (
	"crypto/ed25519"
	"encoding/base64"
	"log/slog"
	"math/rand"

	"github.com/m-sossich/note/pkg/identity"
)

// maxPeersPerResponse caps the PEERS response size (DISC-13) to stay under the 65 KB UDP limit.
const maxPeersPerResponse = 100

// handleAnnounce registers the peer and replies with the peer list.
// In verified mode, drops the message if the signature is invalid.
func (d *Discovery) handleAnnounce(fromAddr string, msg announceMsg) {
	if msg.NodeID == d.cfg.NodeID {
		return
	}
	if msg.PublicKey != "" {
		if !verifyAnnounce(msg) {
			slog.Debug("discovery: ANNOUNCE failed verification, dropping",
				"node_id", msg.NodeID, "from", fromAddr)
			return
		}
	}
	added := d.table.Add(msg.NodeID, msg.Address, msg.Protocols)
	if added {
		d.emitEvent(PeerEvent{
			Type:      PeerFound,
			PeerID:    msg.NodeID,
			Address:   msg.Address,
			Protocols: msg.Protocols,
		})
	}
	d.sendPeerList(fromAddr, msg.NodeID)
}

func (d *Discovery) handleFindPeers(fromAddr string, msg findPeersMsg) {
	d.sendPeerList(fromAddr, msg.NodeID)
}

// sendPeerList sends up to maxPeersPerResponse peers, randomly sampled when over the cap.
// Always appends self (unless excluded) so receivers learn capabilities before connecting.
func (d *Discovery) sendPeerList(toAddr, excludeNodeID string) {
	all := d.table.List()
	entries := make([]peerEntry, 0, len(all))
	for _, p := range all {
		if p.NodeID == excludeNodeID {
			continue
		}
		entries = append(entries, peerEntry{
			NodeID:    p.NodeID,
			Address:   p.Address,
			Protocols: p.Protocols,
		})
	}
	limit := maxPeersPerResponse - 1 // reserve one slot for self-entry
	if len(entries) > limit {
		rand.Shuffle(len(entries), func(i, j int) {
			entries[i], entries[j] = entries[j], entries[i]
		})
		entries = entries[:limit]
	}
	if d.cfg.NodeID != excludeNodeID {
		entries = append(entries, peerEntry{
			NodeID:    d.cfg.NodeID,
			Address:   d.cfg.advertiseAddr(),
			Protocols: d.protocols(),
		})
	}
	d.marshalAndSend(toAddr, newPeersMsg(entries), "PEERS")
}

// verifyAnnounce checks the Ed25519 signature and that SHA-256(pubKey) == NodeID.
func verifyAnnounce(msg announceMsg) bool {
	pubKeyBytes, err := base64.StdEncoding.DecodeString(msg.PublicKey)
	if err != nil || len(pubKeyBytes) != ed25519.PublicKeySize {
		return false
	}
	if identity.NodeIDFrom(ed25519.PublicKey(pubKeyBytes)) != msg.NodeID {
		return false
	}
	sigBytes, err := base64.StdEncoding.DecodeString(msg.Signature)
	if err != nil {
		return false
	}
	return identity.VerifySignature(
		ed25519.PublicKey(pubKeyBytes),
		announceSignData(msg.NodeID, msg.Address, msg.Protocols),
		sigBytes,
	)
}
