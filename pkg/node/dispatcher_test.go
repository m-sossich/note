package node

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	jsoncdc "github.com/m-sossich/note/pkg/codec/json"
	"github.com/m-sossich/note/pkg/wire"
)

// framedPipe wraps a net.Conn with 4-byte big-endian length-prefix framing,
// matching the TCP transport's wire format, for use in tests.
type framedPipe struct{ conn net.Conn }

func (p *framedPipe) Send(data []byte) (int, error) {
	var h [4]byte
	binary.BigEndian.PutUint32(h[:], uint32(len(data)))
	if _, err := p.conn.Write(h[:]); err != nil {
		return 0, err
	}
	n, err := p.conn.Write(data)
	return n, err
}

func (p *framedPipe) Receive() ([]byte, error) {
	var h [4]byte
	if _, err := io.ReadFull(p.conn, h[:]); err != nil {
		return nil, err
	}
	data := make([]byte, binary.BigEndian.Uint32(h[:]))
	if _, err := io.ReadFull(p.conn, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (p *framedPipe) RemoteAddr() string { return "pipe:0" }
func (p *framedPipe) Close() error       { return p.conn.Close() }

// pipePair creates a connected pair of framedPipe transports backed by net.Pipe.
func pipePair() (*framedPipe, *framedPipe) {
	a, b := net.Pipe()
	return &framedPipe{a}, &framedPipe{b}
}

// sendFrame writes a pre-encoded frame (type+payload) to a framedPipe.
func sendFrame(t *testing.T, p *framedPipe, frame wire.Frame) {
	t.Helper()
	if _, err := p.Send(wire.Encode(frame)); err != nil {
		t.Fatalf("sendFrame: %v", err)
	}
}

// readFrame reads the next frame from a framedPipe.
func readFrame(t *testing.T, p *framedPipe) wire.Frame {
	t.Helper()
	p.conn.SetDeadline(time.Now().Add(2 * time.Second))
	data, err := p.Receive()
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	f, err := wire.Decode(data)
	if err != nil {
		t.Fatalf("readFrame decode: %v", err)
	}
	return f
}

// TestDispatcher_NoHandler verifies that receiving an APPLICATION frame for an
// unregistered protocol is silently dropped — no ERROR frame is sent back and
// the connection stays open so subsequent frames are still processed.
func TestDispatcher_NoHandler(t *testing.T) {
	jc := jsoncdc.New()
	server, client := pipePair()
	defer server.Close()
	defer client.Close()

	conn := newConnection(HandshakeResult{PeerID: "peer-no-handler"}, server, jc)

	done := make(chan struct{})
	go func() {
		defer close(done)
		runReadLoop(conn, func(string) (ProtocolHandler, bool) { return nil, false }, func(string) uint32 { return 0 })
	}()

	// Send an APPLICATION frame for an unregistered protocol — must be dropped silently.
	env := wire.Envelope{Protocol: "unknown/1.0", Type: "MSG", Payload: []byte("body")}
	envBytes, _ := jc.Encode(env)
	sendFrame(t, client, wire.Frame{Type: wire.TypeApplication, Payload: envBytes})

	// No response should arrive — give a brief window then confirm loop is still running.
	client.conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	buf := make([]byte, 1)
	_, err := client.conn.Read(buf)
	if err == nil {
		t.Fatal("expected no response for unregistered protocol, got data")
	}
	client.conn.SetReadDeadline(time.Time{})

	select {
	case <-done:
		t.Fatal("runReadLoop exited after unregistered-protocol frame — must stay open")
	default:
	}

	// Terminate the loop by sending DISCONNECT; verify it exits cleanly.
	sendFrame(t, client, wire.Frame{Type: wire.TypeDisconnect})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runReadLoop did not exit after DISCONNECT")
	}
}

// TestDispatcher_HandlerError verifies that when a registered handler returns
// an error the connection stays open and subsequent frames are still processed.
func TestDispatcher_HandlerError(t *testing.T) {
	jc := jsoncdc.New()
	server, client := pipePair()
	defer server.Close()
	defer client.Close()

	calls := make(chan string, 2)
	handler := func(_ string, msgType string, _ func(any) error) error {
		calls <- msgType
		return fmt.Errorf("handler deliberately failed")
	}

	conn := newConnection(HandshakeResult{PeerID: "peer-handler-err"}, server, jc)

	done := make(chan struct{})
	go func() {
		defer close(done)
		runReadLoop(conn, func(p string) (ProtocolHandler, bool) {
			if p == "test/1.0" {
				return handler, true
			}
			return nil, false
		}, func(string) uint32 { return 0 })
	}()

	sendEnvelope := func(msgType string) {
		t.Helper()
		env := wire.Envelope{Protocol: "test/1.0", Type: msgType, Payload: []byte(`"x"`)}
		envBytes, _ := jc.Encode(env)
		sendFrame(t, client, wire.Frame{Type: wire.TypeApplication, Payload: envBytes})
	}

	// First message: handler returns error — connection must stay open.
	sendEnvelope("MSG_A")
	select {
	case got := <-calls:
		if got != "MSG_A" {
			t.Fatalf("expected MSG_A, got %s", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler not called for MSG_A")
	}

	// Second message: connection should still be alive.
	sendEnvelope("MSG_B")
	select {
	case got := <-calls:
		if got != "MSG_B" {
			t.Fatalf("expected MSG_B, got %s", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler not called for MSG_B — connection closed prematurely")
	}

	sendFrame(t, client, wire.Frame{Type: wire.TypeDisconnect})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runReadLoop did not exit after DISCONNECT")
	}
}

// TestDispatcher_DecodeError verifies that an APPLICATION frame whose payload
// cannot be decoded as an Envelope sends a DECODE_ERROR frame and closes the
// connection (NOD-8: envelope decode failure is fatal).
func TestDispatcher_DecodeError(t *testing.T) {
	jc := jsoncdc.New()
	server, client := pipePair()
	defer server.Close()
	defer client.Close()

	conn := newConnection(HandshakeResult{PeerID: "peer-decode-err"}, server, jc)

	done := make(chan struct{})
	go func() {
		defer close(done)
		runReadLoop(conn, func(string) (ProtocolHandler, bool) { return nil, false }, func(string) uint32 { return 0 })
	}()

	// Send an APPLICATION frame with garbage payload (not a valid Envelope).
	sendFrame(t, client, wire.Frame{Type: wire.TypeApplication, Payload: []byte("not-json!!!")})

	// Must receive a DECODE_ERROR frame.
	f := readFrame(t, client)
	if f.Type != wire.TypeError {
		t.Fatalf("expected ERROR frame, got 0x%02x", f.Type)
	}
	var werr wire.WireError
	if err := json.Unmarshal(f.Payload, &werr); err != nil {
		t.Fatalf("unmarshal WireError: %v", err)
	}
	if werr.ErrorCode != wire.CodeDecodeError {
		t.Fatalf("expected DECODE_ERROR, got %q", werr.ErrorCode)
	}

	// Loop must exit — connection is closed after envelope decode failure (NOD-8).
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runReadLoop did not exit after envelope DECODE_ERROR")
	}
}
