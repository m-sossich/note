package node

import (
	"fmt"
	"log/slog"

	"github.com/m-sossich/note/pkg/wire"
)

func runReadLoop(conn *connection, lookupHandler func(string) (ProtocolHandler, bool), limitFor func(string) uint32) {
	defer func() {
		if err := conn.transition(stateDisconnecting); err != nil {
			slog.Error("runReadLoop: invalid transition on disconnect", "peer_id", conn.peerID, "err", err)
			conn.Close()
			return
		}
		conn.Close()
		if err := conn.transition(stateDisconnected); err != nil {
			slog.Error("runReadLoop: invalid transition to disconnected", "peer_id", conn.peerID, "err", err)
		}
	}()

	for {
		data, err := conn.Receive()
		if err != nil {
			return
		}
		frame, err := wire.Decode(data)
		if err != nil {
			rejectWithError(conn, conn.codec, wire.CodeDecodeError, err.Error())
			return
		}

		switch frame.Type {
		case wire.TypeDisconnect:
			return
		case wire.TypeError:
			return
		case wire.TypeApplication:
			if fatal := handleApplicationFrame(conn, frame, lookupHandler, limitFor); fatal {
				return
			}
		}
	}
}

// handleApplicationFrame dispatches to the handler. Returns true if the connection must be closed.
// Envelope decode failure is fatal; handler errors are not.
func handleApplicationFrame(conn *connection, frame wire.Frame, lookupHandler func(string) (ProtocolHandler, bool), limitFor func(string) uint32) (fatal bool) {
	var env wire.Envelope
	if err := conn.decode(frame.Payload, &env); err != nil {
		rejectWithError(conn, conn.codec, wire.CodeDecodeError, "envelope decode error: "+err.Error())
		return true
	}
	// Checked after envelope decode: global limit is the DoS ceiling; this is the per-protocol app policy.
	if limit := limitFor(env.Protocol); limit > 0 && uint32(len(frame.Payload)) > limit {
		slog.Warn("node: frame exceeds per-protocol limit",
			"peer_id", conn.peerID, "protocol", env.Protocol,
			"size", len(frame.Payload), "limit", limit)
		rejectWithError(conn, conn.codec, wire.CodeFrameTooLarge,
			fmt.Sprintf("frame size %d exceeds limit %d for protocol %s", len(frame.Payload), limit, env.Protocol))
		return true
	}
	handler, ok := lookupHandler(env.Protocol)
	if !ok {
		// Drop silently — sending TypeError would close the connection, disrupting relay nodes without handlers.
		slog.Warn("node: no handler registered for protocol — call Register before Start",
			"peer_id", conn.peerID, "protocol", env.Protocol)
		return false
	}
	// decode is re-entrant: each call re-decodes from the same captured bytes.
	// DHT dispatches it twice — once to extract RequestID, once for the full response.
	decode := func(v any) error {
		return conn.decode(env.Payload, v)
	}
	if err := handler(conn.peerID, env.Type, decode); err != nil {
		slog.Debug("protocol handler error", "peer_id", conn.peerID, "protocol", env.Protocol, "type", env.Type, "err", err)
	}
	return false
}

// sendDisconnect is best-effort; peer may already be gone.
func sendDisconnect(conn *connection, code, msg string) {
	payload, _ := conn.encode(wire.Disconnect{ReasonCode: code, ReasonMessage: msg})
	conn.Send(wire.Encode(wire.Frame{Type: wire.TypeDisconnect, Payload: payload}))
}
