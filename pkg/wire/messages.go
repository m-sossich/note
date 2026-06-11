package wire

// Disconnect is the TypeDisconnect payload.
type Disconnect struct {
	ReasonCode    string
	ReasonMessage string
}

// WireError is the TypeError payload.
type WireError struct {
	ErrorCode    string
	ErrorMessage string
}

// Envelope multiplexes application messages over a single connection.
type Envelope struct {
	Protocol string
	Type     string
	Payload  []byte
}
