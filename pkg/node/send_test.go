package node

import (
	"io"
	"strings"
	"testing"

	jsoncdc "github.com/m-sossich/note/pkg/codec/json"
	tcptransport "github.com/m-sossich/note/pkg/transport/tcp"
)

// nopConn is a minimal transport.Conn for injecting connections in tests.
// Never actually written to — used in paths that return before transport send.
type nopConn struct{}

func (nopConn) Send(data []byte) (int, error) { return len(data), nil }
func (nopConn) Receive() ([]byte, error)      { return nil, io.EOF }
func (nopConn) RemoteAddr() string            { return "test:0" }
func (nopConn) Close() error                  { return nil }

func TestSend_UnknownPeer(t *testing.T) {
	n, err := New(Config{
		NodeID:     "send-test-node",
		ListenAddr: "127.0.0.1:19535",
		Transport:  tcptransport.New(0),
		Handshaker: &minHandshaker{},
		Codec:      jsoncdc.New(),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = n.Send("nonexistent", "dht/1.0", "MSG", nil)
	if err == nil {
		t.Fatal("expected error sending to unknown peer")
	}
	if !strings.Contains(err.Error(), "no connection") {
		t.Fatalf("unexpected error message: %v", err)
	}
}
