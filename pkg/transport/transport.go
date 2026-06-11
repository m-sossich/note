package transport

import (
	"errors"
	"net"
	"time"
)

// ErrFrameTooLarge is returned by Conn.Receive when a frame exceeds the configured max size.
var ErrFrameTooLarge = errors.New("frame too large")

// PacketTransport is a connectionless datagram transport.
type PacketTransport interface {
	SendTo(addr string, data []byte) error
	ReceiveFrom() (addr string, data []byte, err error)
	Close() error
}

// StreamTransport is a connection-oriented transport.
type StreamTransport interface {
	Dial(addr string) (Conn, error)
	Listen(addr string) (Listener, error)
	Close() error
}

// Conn is a single stream connection. Send/Receive operate on discrete framed messages.
type Conn interface {
	Send(data []byte) (int, error)
	Receive() ([]byte, error)
	RemoteAddr() string
	Close() error
}

// DeadlineConn is an optional Conn extension used during handshake.
type DeadlineConn interface {
	SetDeadline(t time.Time) error
}

// Listener accepts inbound StreamTransport connections.
type Listener interface {
	Accept() (Conn, error)
	Close() error
	Addr() net.Addr // reflects OS-assigned port when started with ":0"
}
