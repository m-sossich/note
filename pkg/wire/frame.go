package wire

import "fmt"

const (
	TypeIdent       byte = 0x01 // trusted-mode only: initiator declares its NodeID
	TypeDisconnect  byte = 0x03 // payload is Disconnect
	TypeError       byte = 0x04 // payload is WireError
	TypeApplication byte = 0x10 // payload is Envelope
)

const frameTypeLen = 1

// Frame is a decoded wire frame.
type Frame struct {
	Type    byte
	Payload []byte
}

// Encode returns [1-byte type][payload]. Length-prefix framing is the transport's responsibility.
func Encode(f Frame) []byte {
	out := make([]byte, frameTypeLen+len(f.Payload))
	out[0] = f.Type
	copy(out[frameTypeLen:], f.Payload)
	return out
}

func Decode(data []byte) (Frame, error) {
	if len(data) == 0 {
		return Frame{}, fmt.Errorf("empty frame data")
	}
	return Frame{Type: data[0], Payload: data[frameTypeLen:]}, nil
}
