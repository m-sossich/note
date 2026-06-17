package discovery

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// fakeTransport implements transport.PacketTransport with controllable behavior.
type fakeTransport struct {
	// failN causes ReceiveFrom to return an error for the first N calls.
	failN   atomic.Int32
	closed  chan struct{}
	packets chan fakePacket
}

type fakePacket struct {
	addr string
	data []byte
}

func newFakeTransport(failN int) *fakeTransport {
	ft := &fakeTransport{
		closed:  make(chan struct{}),
		packets: make(chan fakePacket, 8),
	}
	ft.failN.Store(int32(failN))
	return ft
}

func (ft *fakeTransport) SendTo(_ string, _ []byte) error { return nil }
func (ft *fakeTransport) Close() error                    { close(ft.closed); return nil }

func (ft *fakeTransport) ReceiveFrom() (string, []byte, error) {
	if ft.failN.Load() > 0 {
		ft.failN.Add(-1)
		return "", nil, errors.New("transient receive error")
	}
	select {
	case p := <-ft.packets:
		return p.addr, p.data, nil
	case <-ft.closed:
		return "", nil, errors.New("transport closed")
	}
}

// TestReceiveLoop_SurvivesTransientErrors verifies that receiveLoop does not
// exit or deadlock when ReceiveFrom returns transient errors before Stop.
func TestReceiveLoop_SurvivesTransientErrors(t *testing.T) {
	ft := newFakeTransport(5) // 5 consecutive errors before any packet

	d := &Discovery{
		cfg:    Config{NodeID: "test-node", PingInterval: time.Hour, PingMaxMissed: 3},
		tr:     ft,
		table:  newPeerTable(0),
		events: make(chan PeerEvent, 8),
		stopCh: make(chan struct{}),
	}
	d.wg.Add(1)
	go d.receiveLoop()

	// Give the loop time to burn through all 5 errors and reach the blocking
	// ReceiveFrom call (waiting for a packet or Close).
	time.Sleep(100 * time.Millisecond)

	// Verify it's still alive: all errors should have been consumed.
	if remaining := ft.failN.Load(); remaining != 0 {
		t.Fatalf("loop stalled: %d transient errors not consumed", remaining)
	}

	// Shut down cleanly.
	close(d.stopCh)
	ft.Close()

	done := make(chan struct{})
	go func() { d.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("receiveLoop did not exit after Stop")
	}
}
