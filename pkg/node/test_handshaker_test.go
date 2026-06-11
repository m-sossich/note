package node

import "github.com/m-sossich/note/pkg/transport"

// minHandshaker is a minimal Handshaker for unit tests inside package node.
// It exchanges NodeIDs over the connection without importing pkg/node/identify,
// which would create a circular dependency (identify imports node for its interface types).
type minHandshaker struct{}

func (h *minHandshaker) Initiate(conn transport.Conn, cfg HandshakeConfig) (HandshakeResult, error) {
	if _, err := conn.Send([]byte(cfg.NodeID)); err != nil {
		return HandshakeResult{}, err
	}
	data, err := conn.Receive()
	if err != nil {
		return HandshakeResult{}, err
	}
	return HandshakeResult{PeerID: string(data)}, nil
}

func (h *minHandshaker) Accept(conn transport.Conn, cfg HandshakeConfig) (HandshakeResult, error) {
	data, err := conn.Receive()
	if err != nil {
		return HandshakeResult{}, err
	}
	if _, err := conn.Send([]byte(cfg.NodeID)); err != nil {
		return HandshakeResult{}, err
	}
	return HandshakeResult{PeerID: string(data)}, nil
}
