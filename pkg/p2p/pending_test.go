package p2p_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/m-sossich/note/pkg/p2p"
)

func TestPendingMap_Wait_DeliverBeforeSelect(t *testing.T) {
	m := p2p.NewPendingMap[string]()
	// Deliver arrives before Wait reaches the select — buffered channel must not drop it.
	ch := m.Register("req-1")
	m.Deliver("req-1", "hello")
	select {
	case v := <-ch:
		if v != "hello" {
			t.Errorf("got %q, want %q", v, "hello")
		}
	default:
		t.Fatal("value was not buffered")
	}
	m.Delete("req-1")
}

func TestPendingMap_Wait_SendThenDeliver(t *testing.T) {
	m := p2p.NewPendingMap[int]()
	sent := false
	result, err := m.Wait(context.Background(), "req-2", func() error {
		sent = true
		go func() { m.Deliver("req-2", 42) }()
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !sent {
		t.Fatal("send callback was not called")
	}
	if result != 42 {
		t.Errorf("got %d, want 42", result)
	}
}

func TestPendingMap_Wait_SendError(t *testing.T) {
	m := p2p.NewPendingMap[string]()
	sendErr := errors.New("connection refused")
	_, err := m.Wait(context.Background(), "req-3", func() error {
		return sendErr
	})
	if !errors.Is(err, sendErr) {
		t.Errorf("expected send error, got %v", err)
	}
	// Channel must be cleaned up after send failure.
	m.Deliver("req-3", "orphan") // must not panic
}

func TestPendingMap_Wait_ContextCancelled(t *testing.T) {
	m := p2p.NewPendingMap[string]()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := m.Wait(ctx, "req-4", func() error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestPendingMap_Wait_Timeout(t *testing.T) {
	m := p2p.NewPendingMap[string]()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := m.Wait(ctx, "req-5", func() error { return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

func TestPendingMap_Deliver_NoWaiter(t *testing.T) {
	m := p2p.NewPendingMap[string]()
	m.Deliver("nonexistent", "value") // must not panic
}

func TestPendingMap_Deliver_FullChannel(t *testing.T) {
	m := p2p.NewPendingMap[string]()
	m.Register("req-6")
	m.Deliver("req-6", "first")
	m.Deliver("req-6", "second") // must not block or panic
	m.Delete("req-6")
}

func TestPendingMap_Wait_Concurrent(t *testing.T) {
	m := p2p.NewPendingMap[int]()
	const n = 50
	var wg sync.WaitGroup
	results := make([]int, n)

	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := string(rune('A'+i%26)) + string(rune('0'+i/26))
			v, err := m.Wait(context.Background(), id, func() error {
				go m.Deliver(id, i)
				return nil
			})
			if err != nil {
				t.Errorf("goroutine %d: unexpected error: %v", i, err)
				return
			}
			results[i] = v
		}(i)
	}
	wg.Wait()

	for i, v := range results {
		if v != i {
			t.Errorf("results[%d] = %d, want %d", i, v, i)
		}
	}
}

func TestPendingMap_Delete_CleansUp(t *testing.T) {
	m := p2p.NewPendingMap[string]()
	m.Register("req-7")
	m.Delete("req-7")
	m.Deliver("req-7", "after-delete") // must not panic
}
