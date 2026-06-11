package protocol

// Protocol is the sub-protocol identifier for gossip.
const Protocol = "gossip/1.0"

// MsgGossip is the only message type in gossip/1.0.
const MsgGossip = "GOSSIP"

// GossipMessage is a single unit of gossip. It propagates through the network
// hop by hop; each relay increments Hops so the receiver knows how many nodes
// the message traversed.
type GossipMessage struct {
	ID       string `json:"id"`        // UUID, unique per message
	OriginID string `json:"origin_id"` // NodeID of the node that originated the message
	Text     string `json:"text"`
	Hops     int    `json:"hops"` // 0 = arrived directly from origin
}
