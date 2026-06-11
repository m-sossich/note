package p2p

import (
	"context"
	"sync"
)

// PendingMap tracks in-flight request/response pairs keyed by a request ID.
// It is the standard way to implement request/response semantics on top of
// note's fire-and-forget Send: Register before sending, Deliver when the
// response handler fires, Wait to block the caller until delivery.
//
// The zero value is not usable — create with NewPendingMap.
type PendingMap[T any] struct {
	mu      sync.RWMutex
	waiting map[string]chan T
}

func NewPendingMap[T any]() *PendingMap[T] {
	return &PendingMap[T]{waiting: make(map[string]chan T)}
}

// Register allocates a buffered response channel for id.
// Must be called before the request is sent so a response that arrives
// before the caller reaches Wait is buffered rather than dropped.
func (m *PendingMap[T]) Register(id string) <-chan T {
	ch := make(chan T, 1)
	m.mu.Lock()
	m.waiting[id] = ch
	m.mu.Unlock()
	return ch
}

// Deliver routes v to the waiter registered for id.
// Non-blocking — no-op if no waiter exists or the channel is already full.
func (m *PendingMap[T]) Deliver(id string, v T) {
	m.mu.RLock()
	ch, ok := m.waiting[id]
	m.mu.RUnlock()
	if !ok {
		return
	}
	select {
	case ch <- v:
	default:
	}
}

// Delete removes the channel for id. Call after Wait or when abandoning a request.
func (m *PendingMap[T]) Delete(id string) {
	m.mu.Lock()
	delete(m.waiting, id)
	m.mu.Unlock()
}

// Wait registers id, calls send, then blocks until a response is delivered or
// ctx is cancelled. Register happens before send, eliminating the race where a
// response arrives before the channel is registered.
func (m *PendingMap[T]) Wait(ctx context.Context, id string, send func() error) (T, error) {
	ch := m.Register(id)
	defer m.Delete(id)
	if err := send(); err != nil {
		var zero T
		return zero, err
	}
	select {
	case v := <-ch:
		return v, nil
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}
