package tcp

import (
	"fmt"
	"net"

	"github.com/m-sossich/note/pkg/transport"
)

type listener struct {
	inner        net.Listener
	maxFrameSize uint32
}

func (l *listener) Accept() (transport.Conn, error) {
	c, err := l.inner.Accept()
	if err != nil {
		return nil, fmt.Errorf("accept: %w", err)
	}
	return newConn(c, l.maxFrameSize), nil
}

func (l *listener) Close() error   { return l.inner.Close() }
func (l *listener) Addr() net.Addr { return l.inner.Addr() }
