package node

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/m-sossich/note/pkg/transport"
)

// errListener is a fake Listener that returns a configurable error on Accept
// for a given number of calls, then blocks until Close is called.
type errListener struct {
	errCount int
	calls    int
	closed   chan struct{}
}

func newErrListener(errCount int) *errListener {
	return &errListener{errCount: errCount, closed: make(chan struct{})}
}

func (l *errListener) Accept() (transport.Conn, error) {
	if l.calls < l.errCount {
		l.calls++
		return nil, fmt.Errorf("transient accept error #%d", l.calls)
	}
	<-l.closed
	return nil, fmt.Errorf("listener closed")
}

func (l *errListener) Addr() net.Addr { return &net.TCPAddr{} }

func (l *errListener) Close() error {
	close(l.closed)
	return nil
}

// TestAcceptLoop_TransientError verifies that acceptLoop does not exit or spin
// uncontrolled on transient Accept errors, and shuts down cleanly via stopCh.
func TestAcceptLoop_TransientError(t *testing.T) {
	fake := newErrListener(3)

	n := &nodeImpl{
		listener:     fake,
		stopCh:       make(chan struct{}),
		pendingSem:   make(chan struct{}, 1),
		peerSem:      make(chan struct{}, 1),
		inboundGuard: make(chan struct{}, 1),
	}
	n.wg.Add(1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		n.acceptLoop()
	}()

	// Give the loop time to process the transient errors and block on Accept.
	time.Sleep(50 * time.Millisecond)

	// Signal shutdown — acceptLoop must exit.
	close(n.stopCh)
	fake.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("acceptLoop did not exit after stopCh closed")
	}

	if fake.calls != 3 {
		t.Fatalf("expected 3 transient errors processed, got %d", fake.calls)
	}
}
