package node

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/m-sossich/note/pkg/codec"
	"github.com/m-sossich/note/pkg/p2p"
	"github.com/m-sossich/note/pkg/transport"
)

type connectionState int

const (
	stateConnecting connectionState = iota
	stateConnected
	stateDisconnecting
	stateDisconnected
)

func (s connectionState) String() string {
	switch s {
	case stateConnecting:
		return "CONNECTING"
	case stateConnected:
		return "CONNECTED"
	case stateDisconnecting:
		return "DISCONNECTING"
	case stateDisconnected:
		return "DISCONNECTED"
	default:
		return "UNKNOWN"
	}
}

var validTransitions = map[connectionState][]connectionState{
	stateConnecting:    {stateConnected},
	stateConnected:     {stateDisconnecting},
	stateDisconnecting: {stateDisconnected},
	stateDisconnected:  {},
}

// ConnInfo is an alias for p2p.ConnInfo.
type ConnInfo = p2p.ConnInfo

type connection struct {
	peerID    string
	publicKey []byte // nil in trusted mode
	conn      transport.Conn
	codec     codec.Codec
	mu        sync.RWMutex
	connState connectionState
}

func newConnection(res HandshakeResult, c transport.Conn, codec codec.Codec) *connection {
	conn := &connection{
		peerID:    res.PeerID,
		publicKey: res.PublicKey,
		conn:      c,
		codec:     codec,
		connState: stateConnecting,
	}
	if err := conn.transition(stateConnected); err != nil {
		slog.Error("newConnection: unexpected state transition failure", "err", err)
	}
	return conn
}

func (c *connection) Send(data []byte) (int, error) { return c.conn.Send(data) }
func (c *connection) Receive() ([]byte, error)      { return c.conn.Receive() }

func (c *connection) Close() error {
	return c.conn.Close()
}

func (c *connection) RemoteAddr() string {
	return c.conn.RemoteAddr()
}

func (c *connection) state() connectionState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connState
}

func (c *connection) transition(next connectionState) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, allowed := range validTransitions[c.connState] {
		if allowed == next {
			c.connState = next
			return nil
		}
	}
	return fmt.Errorf("invalid connection transition: %s → %s", c.connState, next)
}

func (c *connection) encode(v any) ([]byte, error) {
	return c.codec.Encode(v)
}

func (c *connection) decode(data []byte, v any) error {
	return c.codec.Decode(data, v)
}
