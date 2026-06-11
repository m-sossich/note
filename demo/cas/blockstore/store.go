// Package blockstore defines the storage contract for content-addressed blocks
// and provides an in-memory implementation.
//
// To replace MemStore with a disk-backed implementation (BadgerDB, bbolt,
// flat-file), implement BlockStore and pass it to protocol.NewHandler instead
// of blockstore.New(). No other code needs to change.
package blockstore

import "sync"

// BlockStore is the storage contract for content-addressed blocks.
// Keys are CIDs (hex SHA-256 of the block's raw bytes).
type BlockStore interface {
	Put(cid string, data []byte)
	Get(cid string) ([]byte, bool)
	Has(cid string) bool
}

// MemStore is a thread-safe in-memory BlockStore.
// All blocks are lost when the process exits.
type MemStore struct {
	mu     sync.RWMutex
	blocks map[string][]byte
}

func New() *MemStore {
	return &MemStore{blocks: make(map[string][]byte)}
}

func (s *MemStore) Put(cid string, data []byte) {
	s.mu.Lock()
	s.blocks[cid] = data
	s.mu.Unlock()
}

func (s *MemStore) Get(cid string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.blocks[cid]
	return d, ok
}

func (s *MemStore) Has(cid string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.blocks[cid]
	return ok
}
