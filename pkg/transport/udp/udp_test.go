package udp

import (
	"bytes"
	"net"
	"testing"
	"time"
)

// freeUDPAddr picks a free UDP port on loopback by binding briefly.
func freeUDPAddr(t *testing.T) string {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("freeUDPAddr: %v", err)
	}
	addr := conn.LocalAddr().String()
	conn.Close()
	return addr
}

func TestNew_InvalidAddress(t *testing.T) {
	_, err := New("not-a-valid-address:::")
	if err == nil {
		t.Fatal("expected error for invalid bind address, got nil")
	}
}

func TestNew_BindSameAddressTwice(t *testing.T) {
	addr := freeUDPAddr(t)
	first, err := New(addr)
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	defer first.Close()

	_, err = New(addr)
	if err == nil {
		t.Fatal("expected error binding already-bound address, got nil")
	}
}

func TestSendTo_ReceiveFrom_RoundTrip(t *testing.T) {
	addrA := freeUDPAddr(t)
	addrB := freeUDPAddr(t)

	a, err := New(addrA)
	if err != nil {
		t.Fatalf("New(A): %v", err)
	}
	defer a.Close()

	b, err := New(addrB)
	if err != nil {
		t.Fatalf("New(B): %v", err)
	}
	defer b.Close()

	want := []byte("hello udp")
	errCh := make(chan error, 1)
	recvCh := make(chan []byte, 1)

	go func() {
		_, data, err := b.ReceiveFrom()
		if err != nil {
			errCh <- err
			return
		}
		recvCh <- data
	}()

	if err := a.SendTo(addrB, want); err != nil {
		t.Fatalf("SendTo: %v", err)
	}

	select {
	case got := <-recvCh:
		if !bytes.Equal(got, want) {
			t.Errorf("received %q, want %q", got, want)
		}
	case err := <-errCh:
		t.Fatalf("ReceiveFrom: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for UDP packet")
	}
}

func TestResolveAddr_Caching(t *testing.T) {
	addr := freeUDPAddr(t)
	tr, err := New(addr)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tr.Close()

	target := "127.0.0.1:9991"
	first, err := tr.resolveAddr(target)
	if err != nil {
		t.Fatalf("first resolveAddr: %v", err)
	}
	second, err := tr.resolveAddr(target)
	if err != nil {
		t.Fatalf("second resolveAddr: %v", err)
	}
	if first != second {
		t.Error("resolveAddr should return the same pointer from the cache")
	}
}

func TestResolveAddr_InvalidAddress(t *testing.T) {
	addr := freeUDPAddr(t)
	tr, err := New(addr)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tr.Close()

	_, err = tr.resolveAddr("not-a-valid-address:::")
	if err == nil {
		t.Fatal("expected error for invalid target address, got nil")
	}
}

func TestClose_MakesReceiveFromReturnError(t *testing.T) {
	addr := freeUDPAddr(t)
	tr, err := New(addr)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, _, err := tr.ReceiveFrom()
		errCh <- err
	}()

	tr.Close()

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("expected ReceiveFrom to return an error after Close, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: ReceiveFrom did not return after Close")
	}
}

func TestSendTo_InvalidAddress(t *testing.T) {
	addr := freeUDPAddr(t)
	tr, err := New(addr)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tr.Close()

	err = tr.SendTo("not-a-valid-address:::", []byte("data"))
	if err == nil {
		t.Fatal("expected error sending to invalid address, got nil")
	}
}
