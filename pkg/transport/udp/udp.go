package udp

import (
	"fmt"
	"net"
	"sync"
)

const maxUDPPacketSize = 65535 // theoretical max; discovery messages are well under 1500 bytes

// Transport implements transport.PacketTransport over UDP.
type Transport struct {
	conn      *net.UDPConn
	addrCache sync.Map // string → *net.UDPAddr; discovery targets are stable
}

func New(bindAddr string) (*Transport, error) {
	addr, err := net.ResolveUDPAddr("udp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve udp addr %s: %w", bindAddr, err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen udp %s: %w", bindAddr, err)
	}
	return &Transport{conn: conn}, nil
}

func (t *Transport) SendTo(addr string, data []byte) error {
	udpAddr, err := t.resolveAddr(addr)
	if err != nil {
		return err
	}
	_, err = t.conn.WriteTo(data, udpAddr)
	return err
}

func (t *Transport) resolveAddr(addr string) (*net.UDPAddr, error) {
	if v, ok := t.addrCache.Load(addr); ok {
		return v.(*net.UDPAddr), nil
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("resolve target addr %s: %w", addr, err)
	}
	t.addrCache.Store(addr, udpAddr)
	return udpAddr, nil
}

func (t *Transport) ReceiveFrom() (string, []byte, error) {
	buf := make([]byte, maxUDPPacketSize)
	n, addr, err := t.conn.ReadFromUDP(buf)
	if err != nil {
		return "", nil, err
	}
	return addr.String(), buf[:n], nil
}

func (t *Transport) Close() error {
	return t.conn.Close()
}
