package tcp

import (
	"fmt"
	"net"

	"github.com/m-sossich/note/pkg/transport"
)

// defaultMaxFrameSize is 64 KiB — bounds per-connection forced allocation from a peer claiming a large frame.
const defaultMaxFrameSize uint32 = 64 * 1024

// Transport implements transport.StreamTransport over TCP with 4-byte big-endian length-prefix framing.
type Transport struct {
	maxFrameSize uint32
}

func New(maxFrameSize uint32) *Transport {
	if maxFrameSize == 0 {
		maxFrameSize = defaultMaxFrameSize
	}
	return &Transport{maxFrameSize: maxFrameSize}
}

func (t *Transport) Dial(addr string) (transport.Conn, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("tcp dial %s: %w", addr, err)
	}
	return newConn(c, t.maxFrameSize), nil
}

func (t *Transport) Listen(addr string) (transport.Listener, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("tcp listen %s: %w", addr, err)
	}
	return &listener{inner: l, maxFrameSize: t.maxFrameSize}, nil
}

func (t *Transport) Close() error { return nil }
