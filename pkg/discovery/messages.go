package discovery

import (
	"sort"
	"strings"

	"github.com/m-sossich/note/pkg/codec"
)

const (
	msgAnnounce  = "ANNOUNCE"
	msgFindPeers = "FIND_PEERS"
	msgPeers     = "PEERS"
	msgPing      = "PING"
	msgPong      = "PONG"
)

// inboundMsg is a superset of all discovery fields; Type selects the handler.
type inboundMsg struct {
	Type      string
	NodeID    string
	Address   string
	Nonce     string
	Peers     []peerEntry
	PublicKey string   // base64-encoded Ed25519; verified-mode ANNOUNCE only
	Signature string   // base64-encoded sig over NodeID+"|"+Address; verified-mode only
	Protocols []string `json:"Protocols"`
}

type announceMsg struct {
	Type      string
	NodeID    string
	Address   string
	PublicKey string   // empty in trusted mode
	Signature string   // empty in trusted mode
	Protocols []string `json:"Protocols"`
}

type findPeersMsg struct {
	Type   string
	NodeID string
}

type peerEntry struct {
	NodeID  string
	Address string
	// omitempty removed: nil (unknown) must be distinguishable from []string{} (known empty).
	Protocols []string `json:"Protocols"`
}

type peersMsg struct {
	Type  string
	Peers []peerEntry
}

type pingMsg struct {
	Type   string
	NodeID string
	Nonce  string
}

type pongMsg struct {
	Type   string
	NodeID string
	Nonce  string
}

func newAnnounceMsg(nodeID, address, publicKey, signature string, protocols []string) announceMsg {
	return announceMsg{
		Type:      msgAnnounce,
		NodeID:    nodeID,
		Address:   address,
		PublicKey: publicKey,
		Signature: signature,
		Protocols: protocols,
	}
}

func newFindPeersMsg(nodeID string) findPeersMsg {
	return findPeersMsg{Type: msgFindPeers, NodeID: nodeID}
}

func newPeersMsg(peers []peerEntry) peersMsg {
	return peersMsg{Type: msgPeers, Peers: peers}
}

func newPingMsg(nodeID, nonce string) pingMsg {
	return pingMsg{Type: msgPing, NodeID: nodeID, Nonce: nonce}
}

func newPongMsg(nodeID, nonce string) pongMsg {
	return pongMsg{Type: msgPong, NodeID: nodeID, Nonce: nonce}
}

func marshalMsg(v any, c codec.Codec) ([]byte, error) {
	return c.Encode(v)
}

// announceSignData returns the bytes to sign for an ANNOUNCE. Protocols are sorted for canonical ordering.
func announceSignData(nodeID, address string, protocols []string) []byte {
	sorted := append([]string(nil), protocols...)
	sort.Strings(sorted)
	return []byte(nodeID + "|" + address + "|" + strings.Join(sorted, ","))
}
