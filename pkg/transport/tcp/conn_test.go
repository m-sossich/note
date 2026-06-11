package tcp

import (
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"syscall"
	"testing"

	"github.com/m-sossich/note/pkg/transport"
)

func TestNewConn_SetsKeepalive(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		accepted <- c
	}()

	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	defer func() { (<-accepted).Close() }()

	tc := raw.(*net.TCPConn)
	newConn(tc, 0)

	sc, err := tc.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}

	var enabled int
	var sockoptErr error
	if err := sc.Control(func(fd uintptr) {
		enabled, sockoptErr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_KEEPALIVE)
	}); err != nil {
		t.Fatal(err)
	}
	if sockoptErr != nil {
		t.Fatalf("GetsockoptInt SO_KEEPALIVE: %v", sockoptErr)
	}

	// POSIX: boolean socket options are non-zero when enabled, not necessarily 1.
	if enabled == 0 {
		t.Error("SO_KEEPALIVE should be enabled on TCP connections wrapped by newConn")
	}
}

// TestNewConn_NonTCP verifies that newConn does not panic when the
// underlying connection is not a *net.TCPConn (e.g. net.Pipe in tests).
func TestNewConn_NonTCP(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	// Must not panic — the type assertion should be a no-op.
	c := newConn(client, 1024)
	if c == nil {
		t.Fatal("expected non-nil conn")
	}
}

// TestConn_FrameTooLarge verifies that Receive returns an error containing
// ErrFrameTooLarge when the announced frame length exceeds maxFrameSize.
// The connection is left in an unusable state — the caller must close it.
func TestConn_FrameTooLarge(t *testing.T) {
	const maxSize = 64
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	receiver := newConn(client, maxSize)

	// Write a raw length-prefixed frame from the server side with length > maxSize.
	go func() {
		var h [4]byte
		binary.BigEndian.PutUint32(h[:], maxSize+1)
		server.Write(h[:])
	}()

	_, err := receiver.Receive()
	if err == nil {
		t.Fatal("expected error for oversized frame")
	}
	if !errors.Is(err, transport.ErrFrameTooLarge) {
		t.Fatalf("expected transport.ErrFrameTooLarge, got: %v", err)
	}
}

// TestConn_Send_EmptyPayload verifies that a zero-length payload round-trips correctly.
func TestConn_Send_EmptyPayload(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	sender := newConn(client, 1024)
	receiver := newConn(server, 1024)

	done := make(chan []byte, 1)
	go func() {
		got, _ := receiver.Receive()
		done <- got
	}()

	if _, err := sender.Send([]byte{}); err != nil {
		t.Fatalf("Send(empty): %v", err)
	}

	got := <-done
	if len(got) != 0 {
		t.Errorf("expected empty payload, got %d bytes", len(got))
	}
}

// TestConn_Send_Concurrent verifies that 10 concurrent Send calls do not
// interleave their frames. Each message must be received intact.
func TestConn_Send_Concurrent(t *testing.T) {
	const workers = 10
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	sender := newConn(client, 4*1024*1024)
	receiver := newConn(server, 4*1024*1024)

	// Collect all received messages.
	received := make(chan []byte, workers)
	go func() {
		for i := 0; i < workers; i++ {
			data, err := receiver.Receive()
			if err != nil {
				t.Errorf("Receive[%d]: %v", i, err)
				received <- nil
				continue
			}
			received <- data
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			payload := make([]byte, 256)
			for j := range payload {
				payload[j] = byte(n)
			}
			if _, err := sender.Send(payload); err != nil {
				t.Errorf("Send[%d]: %v", n, err)
			}
		}(i)
	}
	wg.Wait()

	// Verify all messages arrived and each byte within a message is uniform
	// (i.e. no two sends were interleaved at the frame level).
	for i := 0; i < workers; i++ {
		msg := <-received
		if len(msg) != 256 {
			t.Errorf("message %d: wrong length %d", i, len(msg))
			continue
		}
		first := msg[0]
		for j, b := range msg {
			if b != first {
				t.Errorf("message %d: interleaved at byte %d (got %d, want %d)", i, j, b, first)
				break
			}
		}
	}
}

// TestConn_Receive_ErrorMidHeader verifies that Receive returns an error (not a
// panic) when the underlying connection is closed while reading the header.
func TestConn_Receive_ErrorMidHeader(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	receiver := newConn(client, 1024)

	done := make(chan error, 1)
	go func() {
		_, err := receiver.Receive()
		done <- err
	}()

	// Close before writing anything — receiver will get EOF mid-header.
	client.Close()

	select {
	case err := <-done:
		if err == nil {
			t.Error("expected error when connection closed mid-header, got nil")
		}
	}
}

// TestConn_Receive_ZeroLengthFrame verifies that a length=0 frame is received
// as an empty byte slice with no error.
func TestConn_Receive_ZeroLengthFrame(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	receiver := newConn(client, 1024)

	go func() {
		var h [4]byte
		binary.BigEndian.PutUint32(h[:], 0)
		server.Write(h[:]) // 4-byte header, no payload
	}()

	got, err := receiver.Receive()
	if err != nil {
		t.Fatalf("Receive: unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty payload, got %d bytes", len(got))
	}
}

// TestTransport_Dial_ConnectionRefused verifies that Dial returns an error
// when no listener is bound on the target address.
func TestTransport_Dial_ConnectionRefused(t *testing.T) {
	// Bind and immediately close to get a port that's guaranteed to be free.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()

	tr := New(0)
	_, err = tr.Dial(addr)
	if err == nil {
		t.Fatal("expected connection-refused error, got nil")
	}
}

// TestTransport_Listen_AddressInUse verifies that Listen returns an error
// when the same address is already bound.
func TestTransport_Listen_AddressInUse(t *testing.T) {
	tr := New(0)
	first, err := tr.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("first Listen: %v", err)
	}
	defer first.Close()

	// Extract the actual bound address.
	// We need to cast to net.Listener via the underlying struct.
	// Instead, bind on the same explicit port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	// Bind the address with the real net package so it's held.
	held, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("setup held listener: %v", err)
	}
	defer held.Close()

	_, err = tr.Listen(addr)
	if err == nil {
		t.Fatal("expected address-in-use error, got nil")
	}
}
