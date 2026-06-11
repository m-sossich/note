package tlstransport

import (
	gotls "crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/m-sossich/note/pkg/transport"
)

const frameHeaderLen = 4

type conn struct {
	inner        *gotls.Conn
	maxFrameSize uint32
	writeMu      sync.Mutex
}

func newConn(c *gotls.Conn, maxFrameSize uint32) *conn {
	return &conn{inner: c, maxFrameSize: maxFrameSize}
}

func (c *conn) Send(data []byte) (int, error) {
	buf := make([]byte, frameHeaderLen+len(data))
	binary.BigEndian.PutUint32(buf[:frameHeaderLen], uint32(len(data)))
	copy(buf[frameHeaderLen:], data)
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	n, err := c.inner.Write(buf)
	if n != len(buf) && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		return 0, fmt.Errorf("tls send: %w", err)
	}
	return len(data), nil
}

func (c *conn) Receive() ([]byte, error) {
	var header [frameHeaderLen]byte
	if _, err := io.ReadFull(c.inner, header[:]); err != nil {
		return nil, fmt.Errorf("tls receive header: %w", err)
	}
	length := binary.BigEndian.Uint32(header[:])
	if length > c.maxFrameSize {
		return nil, fmt.Errorf("%w: frame size %d exceeds max %d", transport.ErrFrameTooLarge, length, c.maxFrameSize)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(c.inner, buf); err != nil {
		return nil, fmt.Errorf("tls receive payload: %w", err)
	}
	return buf, nil
}

func (c *conn) RemoteAddr() string            { return c.inner.RemoteAddr().String() }
func (c *conn) Close() error                  { return c.inner.Close() }
func (c *conn) SetDeadline(t time.Time) error { return c.inner.SetDeadline(t) }

func (c *conn) Handshake() error { return c.inner.Handshake() }

// PeerCertificate requires Handshake to have completed.
func (c *conn) PeerCertificate() *x509.Certificate {
	certs := c.inner.ConnectionState().PeerCertificates
	if len(certs) == 0 {
		return nil
	}
	return certs[0]
}
