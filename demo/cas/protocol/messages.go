package protocol

// Protocol is the sub-protocol identifier for CAS block exchange.
const Protocol = "cas/1.0"

// Message type constants for the cas/1.0 sub-protocol.
const (
	MsgWantBlock = "WANT_BLOCK"
	MsgBlock     = "BLOCK"
	MsgNotFound  = "NOT_FOUND"
)

// WantBlock requests a single block by CID.
type WantBlock struct {
	RequestID string `json:"request_id"`
	CID       string `json:"cid"`
}

// BlockMsg delivers the raw bytes for a requested block.
// Data is a []byte; the JSON codec encodes it as base64.
type BlockMsg struct {
	RequestID string `json:"request_id"`
	CID       string `json:"cid"`
	Data      []byte `json:"data"`
}

// NotFound signals that the peer does not hold the requested block.
type NotFound struct {
	RequestID string `json:"request_id"`
	CID       string `json:"cid"`
}
