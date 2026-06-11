package node

import (
	"github.com/m-sossich/note/pkg/codec"
	"github.com/m-sossich/note/pkg/p2p"
	"github.com/m-sossich/note/pkg/wire"
)

// HandshakeConfig is an alias for p2p.HandshakeConfig.
type HandshakeConfig = p2p.HandshakeConfig

// HandshakeResult is an alias for p2p.HandshakeResult.
type HandshakeResult = p2p.HandshakeResult

// Handshaker is an alias for p2p.Handshaker.
type Handshaker = p2p.Handshaker

type frameSender interface {
	Send([]byte) (int, error)
}

// rejectWithError sends a wire ERROR frame. Best-effort.
func rejectWithError(conn frameSender, c codec.Codec, code, msg string) {
	payload, _ := c.Encode(wire.WireError{ErrorCode: code, ErrorMessage: msg})
	conn.Send(wire.Encode(wire.Frame{Type: wire.TypeError, Payload: payload}))
}
