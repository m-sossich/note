package protocol

// Protocol is the sub-protocol identifier for chat.
const Protocol = "chat/1.0"

// Message type constants for the chat/1.0 sub-protocol.
const (
	MsgAnnounce = "ANNOUNCE"
	MsgMessage  = "MESSAGE"
)

// Announce declares that the sending peer is present in Room as Username.
// In verified mode, Signature is an Ed25519 signature over "room:username"
// produced with the sender's private key. Receivers verify it against the
// peer's public key (obtained from the TLS handshake).
type Announce struct {
	Room      string `json:"room"`
	Username  string `json:"username"`
	Signature []byte `json:"signature,omitempty"`
}

// ChatMessage is a text message sent to a room.
type ChatMessage struct {
	Room     string `json:"room"`
	Username string `json:"username"`
	Text     string `json:"text"`
}
