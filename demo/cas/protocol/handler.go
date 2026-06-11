package protocol

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/m-sossich/note/demo/cas/blockstore"
	"github.com/m-sossich/note/pkg/node"
	"github.com/m-sossich/note/pkg/p2p"
)

type blockResponse struct {
	data  []byte
	found bool
}

// Handler implements the cas/1.0 block exchange sub-protocol.
type Handler struct {
	store   blockstore.BlockStore
	send    func(peerID, msgType string, payload any) error
	pending p2p.PendingMap[blockResponse]
}

// NewHandler creates a Handler wired to n and registers the cas/1.0 protocol
// on it. The handler is ready to use immediately.
func NewHandler(n node.Node, store blockstore.BlockStore) *Handler {
	h := &Handler{
		store: store,
		send: func(peerID, msgType string, payload any) error {
			_, err := n.Send(peerID, Protocol, msgType, payload)
			return err
		},
		pending: *p2p.NewPendingMap[blockResponse](),
	}
	n.Register(Protocol, h.Handle)
	return h
}

// Handle dispatches incoming cas/1.0 messages.
func (h *Handler) Handle(peerID, msgType string, decode func(any) error) error {
	switch msgType {
	case MsgWantBlock:
		return h.handleWantBlock(peerID, decode)
	case MsgBlock:
		return h.handleBlock(peerID, decode)
	case MsgNotFound:
		return h.handleNotFound(peerID, decode)
	default:
		return fmt.Errorf("cas: unknown message type %q", msgType)
	}
}

// FetchBlock sends a WANT_BLOCK to peerID and waits for the response.
// Returns the block bytes, or an error on NOT_FOUND or timeout.
// Safe to call concurrently — each call gets its own pending channel.
func (h *Handler) FetchBlock(ctx context.Context, peerID, cid string) ([]byte, error) {
	reqID := uuid.New().String()
	resp, err := h.pending.Wait(ctx, reqID, func() error {
		return h.send(peerID, MsgWantBlock, WantBlock{RequestID: reqID, CID: cid})
	})
	if err != nil {
		return nil, fmt.Errorf("WANT_BLOCK: %w", err)
	}
	if !resp.found {
		return nil, fmt.Errorf("block %s not found on %s", trunc(cid), trunc(peerID))
	}
	return resp.data, nil
}

func (h *Handler) handleWantBlock(peerID string, decode func(any) error) error {
	var req WantBlock
	if err := decode(&req); err != nil {
		return fmt.Errorf("parse WANT_BLOCK: %w", err)
	}
	slog.Debug("block requested", "peer", trunc(peerID), "cid", trunc(req.CID))
	data, ok := h.store.Get(req.CID)
	if !ok {
		return h.send(peerID, MsgNotFound, NotFound{RequestID: req.RequestID, CID: req.CID})
	}
	return h.send(peerID, MsgBlock, BlockMsg{RequestID: req.RequestID, CID: req.CID, Data: data})
}

func (h *Handler) handleBlock(peerID string, decode func(any) error) error {
	var msg BlockMsg
	if err := decode(&msg); err != nil {
		return fmt.Errorf("parse BLOCK: %w", err)
	}
	slog.Debug("block received", "peer", trunc(peerID), "cid", trunc(msg.CID), "bytes", len(msg.Data))
	h.pending.Deliver(msg.RequestID, blockResponse{data: msg.Data, found: true})
	return nil
}

func (h *Handler) handleNotFound(peerID string, decode func(any) error) error {
	var msg NotFound
	if err := decode(&msg); err != nil {
		return fmt.Errorf("parse NOT_FOUND: %w", err)
	}
	slog.Debug("block not found on peer", "peer", trunc(peerID), "cid", trunc(msg.CID))
	h.pending.Deliver(msg.RequestID, blockResponse{found: false})
	return nil
}

func trunc(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}
