# Node Layer

The wire layer (see [`01-wire.md`](01-wire.md)) handles bytes. The node layer handles connections. Its job is to take a raw TCP connection, give it an identity through a handshake, and then act as a switchboard: incoming messages are routed to the application handler registered for their sub-protocol, and outgoing messages are encoded and delivered to the right peer.

This is also where the integration with peer discovery lives. When the discovery layer reports that a new peer is reachable, the node layer dials it. When discovery reports that a peer has gone silent, the node layer closes the connection. The two layers communicate through a narrow event interface — the node layer knows nothing about how discovery works internally.


## Node Identity

Stable identity is what makes the network stateful across reconnections. If a node's identity changes on every restart, other nodes' connection maps point to a NodeID that no longer exists. Peers that had cached your address under "peerX" will try to reconnect but find a stranger — from their perspective, the node they knew has vanished and an unknown new one has appeared at the same address. A stable identifier is the contract that lets the rest of the network recognize you as the same participant after a restart.

Every node in the network has a stable identifier — a string that persists across restarts. The library does not generate or store this identifier; that is the caller's responsibility. The identifier must be unique within the network and stable — the same node appearing with a different identifier on each restart looks like a new node to all its peers, and its old identity's connections will eventually time out without a clean disconnect.

A UUID v4 is a reasonable default. What matters is uniqueness and persistence, not format. Store it to disk on first generation and read it back on every subsequent startup.

The identity is used in two ways: it is exchanged during the handshake so each side knows who it is talking to, and it is the key under which connections are tracked internally. When the application calls `Send(peerID, ...)`, it looks up the connection by this identity.


## Connection Lifecycle

Every connection between two nodes passes through four states in strict order:

```
CONNECTING ──▶ CONNECTED ──▶ DISCONNECTING ──▶ DISCONNECTED
```

No other transitions are valid. The states mean:

- **CONNECTING** — A TCP connection exists but the handshake has not yet completed. No application messages can flow. The connection is not yet registered as a peer.
- **CONNECTED** — The handshake succeeded. Both sides know each other's identity. Application messages can flow in both directions. The connection is registered in the node's peer map.
- **DISCONNECTING** — One side has decided to close the connection. A `DISCONNECT` frame is in flight or has been sent. No new application messages should be sent.
- **DISCONNECTED** — The connection is closed and cleaned up. The peer is removed from the peer map.

A `Connection` object only exists for connections that have reached `CONNECTED`. If the handshake fails for any reason, the TCP connection is closed and no connection object is created — the application never sees a failed handshake as a peer.

The `OnPeerConnected` and `OnPeerDisconnected` callbacks fire at the `CONNECTED` and `DISCONNECTED` transitions respectively. They are **protocol hooks**, not readiness signals.

`OnPeerConnected` fires once per established connection. Use it when your sub-protocol needs to react immediately to a new peer — for example, sending an initial handshake message or presence announcement to that specific peer. Do not use it to gate application logic on "having at least one peer." A node with zero peers is operational; it simply has no one to talk to yet. Operations that require peers — DHT lookups, file fetches, broadcasts — will naturally fail or return empty results, which is the correct behavior.

`OnPeerDisconnected` fires once per torn-down connection. Use it for per-peer cleanup: removing the peer from a membership roster, cancelling in-flight requests, releasing per-peer state.

Both callbacks run in the connection goroutine. Anything slow must be dispatched to a separate goroutine from within the callback.


## The Handshake

When a TCP connection is first established, neither side knows who the other is. The handshake is the exchange that changes that. It is the first thing that happens on every connection, before any application message can be sent.

The roles are asymmetric: the side that opened the connection is the **initiator**; the side that accepted it is the **acceptor**. Both sides must support both roles — any node may dial out or accept incoming connections.

The minimum requirement for a conformant handshake is simple: both sides learn each other's stable identity. Everything beyond that — version checks, capability negotiation, authentication tokens — is application-level concern. The node library does not prescribe it.

**NOD-1** — Every new connection MUST complete a handshake before any `APPLICATION` frames are exchanged.

**NOD-2** — The handshake MUST establish the remote peer's stable node identity. This is the only output the node library requires from a handshake.

**NOD-3** — The handshake MUST complete within a configured timeout. A connection that fails or times out MUST be closed without registering a peer.

**NOD-4** — The handshake implementation is pluggable. Any implementation that satisfies NOD-2 and NOD-3 is conformant.

### Trusted Handshake (Identity Exchange)

The trusted handshaker is one-way: the **initiator** sends a single `IDENT` frame containing its `NodeID`; the **acceptor** reads it. No response is sent. This step exists only because TCP carries no application-layer identity — the ephemeral source port cannot be matched to a declared peer address.

```
Initiator                                  Acceptor
    │                                          │
    │── IDENT { NodeID: "initiator-id" } ────▶│
    │                                          │
    │         [application messages]           │
```

**This handshake provides no authentication.** Either side can claim any node identity without proof. It is appropriate for trusted networks where all participants are operator-controlled. For networks with untrusted participants, replace it with the verified handshaker.

The frame is encoded with the network codec. If it fails to decode the handshake fails and the connection is closed. A node that cannot decode the handshake is not part of the network.

### Verified Handshake (mTLS)

When operating in verified mode, the handshaker runs mutual TLS first. Both sides present their Ed25519 certificates; the TLS handshake proves key ownership and binds identity. The acceptor reads the peer's `NodeID` directly from the verified certificate Common Name — no `IDENT` frame is sent or read in either direction.

**NOD-18** — A verified-mode handshaker MUST NOT send or read an `IDENT` frame. Identity is derived exclusively from the TLS certificate Common Name. Any implementation that sends a wire frame during the verified handshake is non-conformant.

### Custom Handshakes

The default handshake is the right choice for networks where you control all nodes and trust is implicit. If your network needs to gate access, implement a custom handshaker. The interface requires two methods — one for the initiator role and one for the acceptor — both receiving the raw connection and local configuration, both returning the remote peer's identity on success or an error on failure.

Common patterns:

**Authentication.** The initiator sends a signed token; the acceptor verifies it against a known public key. Nodes with invalid tokens are rejected during the handshake and never become peers. This is the right place to enforce access control — after the handshake, the node layer assumes the connection is trustworthy.

**Version enforcement.** Both sides send their protocol version number. If they disagree on required versions, return an error and use the `UNSUPPORTED_VERSION` error code so the remote side gets a meaningful rejection rather than a generic decode failure.

Whatever the handshake does, it must complete within the configured timeout. A handshake that hangs indefinitely blocks an accept goroutine slot and can exhaust the inbound connection pool. Keep handshake I/O fast.


## Sub-Protocol Registration

A sub-protocol is a named, versioned namespace for application messages. The name is a free-form string; the `name/version` convention (`chat/1.0`, `sync/2.0`, `dht/1.0`) is strongly recommended. The version is not parsed by the node layer — it is there for your own operational clarity and for log messages to be meaningful to a human debugging dispatch issues.

Sub-protocols must be registered before the node starts. There is no dynamic registration after startup. The set of protocols a node supports is a static property of its role in the network.

**NOD-5** — All sub-protocol handlers MUST be registered before the node begins accepting or initiating connections.

Each sub-protocol has exactly one handler. If a node receives a message for a sub-protocol with no registered handler, it silently drops the message and logs a warning. The connection stays open — the peer can continue sending on other sub-protocols. An error frame is intentionally NOT sent because sending it would close the connection, disrupting relay nodes that forward protocols they do not handle locally.

Multiple sub-protocols share the same TCP connections. There is no separate connection per protocol. This is intentional: opening a new TCP connection for each protocol would multiply handshake overhead and connection counts. Multiplexing over one connection is efficient and keeps lifecycle management simple.

**Versioning strategy.** Name your sub-protocols with versions from the start, even if you think you will never change them. When you need to make a breaking change, register `sync/2.0` alongside `sync/1.0` and run both handlers during a transition window. Messages for the old protocol are silently dropped on nodes that have removed it; the connection stays alive. Mixed-version deployments are safe. Once all nodes have migrated, remove the old handler.

**Separation of concerns.** If your application does file transfer and presence updates, those should be two sub-protocols — `fileshare/1.0` and `presence/1.0` — not one protocol with mixed message types. Each handler is simpler, each can be tested independently, and each can be versioned independently.


## Message Dispatch

When the node's read loop receives bytes from a connection, it processes them through the following chain:

```
raw bytes
    │
    ▼
transport strips length prefix → frame bytes
    │
    ▼
wire.Decode → Frame { Type, Payload }
    │
    ├── TypeDisconnect  → close connection, no error
    ├── TypeError       → close connection (see error handling)
    └── TypeApplication →
            │
            ▼
        codec.Decode(Payload) → Envelope { Protocol, Type, Payload }
            │
            ▼
        lookup handler for Envelope.Protocol
            │
            ├── not found → log WARN, drop message, keep connection open
            └── found →
                    │
                    ▼
                handler(peerID, Envelope.Type, decodeFn)
```

The `decodeFn` passed to the handler is a closure over `Envelope.Payload` and the connection's codec. When the handler calls `decodeFn(&myStruct)`, it decodes the inner message body. The handler never imports or references the codec — it just calls a function. This means handlers can be unit-tested with a trivial mock decode function, with no wire format involved.

**NOD-6** — Every inbound `APPLICATION` message MUST be delivered to the handler registered for its sub-protocol. If no handler is registered, the node MUST silently drop the message (log a WARN) and MUST keep the connection open. The node MUST NOT send an error frame — doing so would close the connection and disrupt relay nodes that legitimately forward protocols they do not handle.

**NOD-7** — A `DECODE_ERROR` at the frame level (the outer `wire.Decode` call) MUST close the connection after sending an `ERROR` frame.

**NOD-8** — A `DECODE_ERROR` at the envelope level (decoding the `APPLICATION` payload) MUST close the connection after sending an `ERROR` frame.

Both NOD-7 and NOD-8 close the connection because a decode failure at either level means the two sides can no longer communicate meaningfully — there is no way to recover a connection whose framing or encoding is broken. Contrast this with the unregistered-handler case (NOD-6), where the wire protocol is working correctly and only the routing failed; the connection remains useful for other sub-protocols and relay purposes.


## Handler Policies

The library applies no rate limiting, quotas, or filtering to inbound messages. This is intentional. Rate policy belongs at the protocol layer, not the transport layer, because the library cannot know what a "normal" rate is for any given sub-protocol. DHT traffic during a lookup burst and chat traffic are incomparable: 100 messages per second is an attack against `chat/1.0` and completely normal for `dht/1.0`. Setting a default would either break legitimate high-throughput protocols or do nothing useful against abuse.

The composition surface for cross-cutting concerns is the `ProtocolHandler` type itself: it is a function, and functions compose. Rate limiting, deduplication, authentication checks, and audit logging all follow the same pattern — wrap the handler before registering it:

```
rateLimitedHandler = rateLimit(originalHandler, limit, burst)
n.Register("chat/1.0", rateLimitedHandler)
```

Each sub-protocol sets its own policy based on its own semantics. Protocols that need high throughput register without wrapping. Protocols where per-peer rate is a meaningful constraint wrap with an appropriate limit.

**Handler errors are non-fatal.** A handler that returns an error causes the library to log the error and continue reading from the connection. The connection stays open and subsequent messages are processed normally. A rate-limited handler that returns an error on limit exceeded drops that one message — the peer is not disconnected.

**Disconnecting a peer for sustained abuse** is an application decision. Nothing in the library prevents an application from tracking a per-peer error count and calling an external mechanism to disconnect that peer after a threshold. The `OnPeerDisconnected` callback and the `peer-lost` event on the discovery layer provide the hooks for cleanup. The decision of when abuse has occurred, and what threshold justifies disconnection, belongs to the application — the library cannot make it.

## Sending Messages

To send a message to a peer, the caller provides the peer's identity, the sub-protocol, the message type, and the payload. The node layer encodes the payload, wraps it in an envelope, builds an `APPLICATION` frame, and passes everything to the transport.

If no connection exists for the requested peer identity, or the connection is not in `CONNECTED`, the call returns an error immediately. The application is responsible for ensuring a connection exists before sending — typically by waiting for an `OnPeerConnected` callback, checking the peer list, or building a send queue that drains when a peer connects.

Do not send from within `OnPeerConnected` for anything timing-sensitive. The connection is live at that point, but the remote peer may not yet have finished processing the handshake on their side. A brief yield or a queued send is safer than an immediate synchronous call.


## Discovery Integration

The node layer integrates with the discovery layer through a single abstract interface: the discovery layer notifies the node layer when peers become reachable or go away. The mechanism for delivering these notifications is implementation-defined — a channel, a callback registration, a polling loop, or an observer pattern all satisfy the contract. What matters is the guarantee: the node layer eventually learns about every `peer-found` and `peer-lost` transition that the discovery layer produces.

**NOD-9** — When the discovery layer notifies the node that a peer is reachable, the node SHOULD initiate a TCP connection to the peer's reported address and run the handshake, subject to the following rules:
- If the notification's peer identity matches the local node's identity, MUST silently ignore — a node does not connect to itself.
- If a connection or an in-flight dial already exists for that peer identity, MUST silently ignore — duplicate connections are not allowed.
- If the notification carries a non-nil, explicitly empty protocol list (`[]string{}`), the node MUST NOT initiate an outbound dial. An empty list means the peer has no application protocols and does not need application connections; the peer will dial in if it requires one. A `nil` protocol list means capabilities are unknown; the node MUST dial as normal.

**NOD-10** — When the discovery layer notifies the node that a peer has gone away, the node SHOULD close the connection to that peer if one exists.

**NOD-16** — When a connected peer's capabilities become known (via a notification carrying a non-nil protocol list for a peer that is already connected), the node MUST record the capability set and fire `OnPeerCapabilitiesKnown`. Consumers of this callback MAY use it to remove the peer from overlay structures (e.g., the DHT routing table) if the peer does not support the required protocol.

**NOD-17** — `Send` MUST reject a message to a peer whose known protocol list does not include the target protocol, returning an error without transmitting the frame. If the peer's protocol list is unknown (nil in the capability map), the send MUST be allowed — the conservative default is to attempt delivery rather than silently drop.

**NOD-15** — When two nodes discover each other simultaneously and both complete a successful handshake before either has registered the other, two connections to the same peer exist. This MUST be resolved deterministically: the connection where the local node's identity is lexicographically greater than the remote peer's identity MUST be closed with a `DISCONNECT` frame. Both sides applying this rule independently will close the same connection, leaving exactly one.

The node layer does not need to know how peer discovery works, or how notifications are delivered. Any source that produces `peer-found` and `peer-lost` notifications satisfies the contract. This means you can replace the UDP bootstrap mechanism with mDNS, a static seed list, or a centralized registry without changing any node layer code. The discovery implementation details — UDP sockets, nonces, ping timers — are invisible from here.


## Graceful Disconnect

**NOD-11** — Before closing an active connection, the node SHOULD send a `DISCONNECT` frame with a reason code. The send is best-effort: if the peer is already gone, the frame will fail silently and that is acceptable.

**NOD-12** — A node that receives a `DISCONNECT` frame MUST close the connection. It MUST NOT treat the received `DISCONNECT` as an error condition.

**NOD-13** — After receiving `DISCONNECT`, the node MUST NOT send any further `APPLICATION` frames on that connection.

The reason codes (`SHUTDOWN`, `PEER_LOST`) are informational. Implementations may log them or surface them to the application, but are not required to take different actions based on the code.


## Peer Limits

Peer limits protect the node against two distinct attacks: CPU exhaustion from concurrent cryptographic handshakes, and memory/goroutine exhaustion from maintaining too many simultaneous connections. A single limit cannot address both — they require separate controls at different points in the connection lifecycle.

**NOD-14a — Pending peer limit (`MaxPendingPeers`).** A node MUST enforce a configurable cap on simultaneous handshakes in progress, covering both inbound and outbound directions. Connections that cannot acquire a pending slot MUST be rejected immediately — for inbound, by closing the TCP connection before the handshake begins; for outbound, by dropping the `peer-found` event. This bounds the CPU cost of TLS work regardless of which side initiated the connection.

**NOD-14b — Total peer limit (`MaxPeers`).** A node MUST enforce a configurable cap on total established peers (inbound plus outbound). A connection that completes the handshake successfully but cannot acquire a peer slot MUST be closed. This cap is held for the entire lifetime of the connection and released when it ends.

**NOD-14c — Inbound peer sub-limit (`MaxInboundPeers`).** A node SHOULD enforce a configurable cap on established inbound peers, set below `MaxPeers`. This reserves capacity for outbound connections initiated by the node itself — preventing aggressive inbound traffic from crowding out the node's own peer discovery. The default is two-thirds of `MaxPeers`, matching the convention established by Ethereum's devp2p.

The three limits work together: an inbound connection must satisfy both `MaxInboundPeers` and `MaxPeers` to be established. An outbound connection must satisfy only `MaxPeers`. The `MaxPendingPeers` cap applies to both directions during the handshake phase and is released once the handshake completes — whether it succeeded or failed.


## Requirements Summary

| ID | Level | Requirement |
|---|---|---|
| NOD-1 | MUST | Complete a handshake before any APPLICATION frames are exchanged |
| NOD-2 | MUST | Handshake establishes the remote peer's stable identity |
| NOD-3 | MUST | Handshake completes within a configured timeout; failure closes the connection |
| NOD-4 | — | Handshake implementation is pluggable |
| NOD-5 | MUST | Register all handlers before the node starts |
| NOD-6 | MUST | Silently drop (log WARN) APPLICATION messages for unregistered sub-protocols; keep connection open; MUST NOT send an error frame |
| NOD-7 | MUST | Close connection on frame-level DECODE_ERROR |
| NOD-8 | MUST | Close connection on envelope-level DECODE_ERROR |
| NOD-9 | SHOULD / MUST | Dial on peer-found; ignore self and duplicate dials; skip dial when event carries an empty (non-nil) protocol list |
| NOD-10 | SHOULD | Close connection on peer-lost |
| NOD-11 | SHOULD | Send DISCONNECT frame before closing an active connection |
| NOD-12 | MUST | Close connection on receiving DISCONNECT; do not treat it as an error |
| NOD-13 | MUST | Send no APPLICATION frames after receiving DISCONNECT |
| NOD-14a | MUST | Enforce MaxPendingPeers: cap concurrent handshakes (both directions); reject before handshake begins |
| NOD-14b | MUST | Enforce MaxPeers: cap total established peers; close connection if full after handshake |
| NOD-14c | SHOULD | Enforce MaxInboundPeers ≤ MaxPeers: reserve outbound capacity; default MaxPeers × 2/3 |
| NOD-15 | MUST | Resolve simultaneous dual connections by closing the one where local ID > remote ID |
| NOD-16 | MUST | Record peer capabilities when known; fire OnPeerCapabilitiesKnown for connected peers |
| NOD-17 | MUST | Reject Send to peers with a known protocol list that excludes the target protocol; allow when list is unknown (nil) |
| NOD-18 | MUST NOT | Verified-mode handshaker MUST NOT send or read an IDENT frame; identity comes from TLS certificate CN only |
