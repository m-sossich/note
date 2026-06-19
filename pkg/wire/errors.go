package wire

// Error codes in TypeError frames.
const (
	CodeUnsupportedVersion = "UNSUPPORTED_VERSION"
	CodeDecodeError        = "DECODE_ERROR"
	CodeFrameTooLarge      = "FRAME_TOO_LARGE"
)

// Reason codes in TypeDisconnect frames.
const (
	ReasonShutdown = "SHUTDOWN"
	ReasonPeerLost = "PEER_LOST"
)
