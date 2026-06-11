# Wire Layer

The wire layer is the foundation everything else builds on. It answers a deceptively simple question: how does a struct become bytes on a TCP socket, and how do those bytes become a struct again on the other side?

There are three distinct problems here, and they are often confused:

1. **Framing** — A TCP connection is a stream of bytes with no inherent boundaries. If you send two messages back to back, the receiver cannot tell where the first ends and the second begins. Someone has to add that boundary information.

2. **Encoding** — Structured data needs to be serialized into bytes. JSON, MessagePack, Protobuf — these are all different answers to the same question. All nodes in a network must agree on which one to use.

3. **Routing** — Once a message is decoded, the receiving node needs to know which application handler to call. The encoded bytes alone do not carry that information; there must be a wrapper that identifies the destination.

This spec addresses all three, in order from the bottom of the stack up.


## Transport

The transport is the layer that moves bytes between two endpoints. This library uses two transport models depending on the use case.

### Connectionless Transport

Used by the discovery layer (see [`03-discovery.md`](03-discovery.md)). A connectionless transport sends and receives discrete datagrams addressed to a specific endpoint. There is no setup required before sending; each datagram is independent.

The reference implementation uses UDP.

**Requirements:**

- **TRP-C1** — A sender MUST be able to transmit a datagram to any reachable address without prior connection setup.
- **TRP-C2** — A receiver MUST be able to accept incoming datagrams and learn the sender's network address.
- **TRP-C3** — Delivery is best-effort. The transport makes no guarantees about ordering, delivery, or deduplication. Discovery is designed around this — all discovery messages are idempotent and the liveness protocol tolerates loss gracefully.
- **TRP-C4** — Discovery messages are expected to be well under 1500 bytes. Implementations MAY impose a maximum datagram size consistent with the underlying protocol's MTU.

### Connection-Oriented Transport

Used by the node layer (see [`02-node.md`](02-node.md)). A connection-oriented transport establishes a persistent, bidirectional channel between two endpoints over which discrete messages are exchanged.

The critical word is *discrete*. TCP is a byte stream — it does not know anything about message boundaries. The transport implementation is responsible for adding framing so that a single `send` call on one side corresponds to exactly one `receive` call on the other. The node layer and everything above it depends on this property; they never deal with partial messages.

The reference implementation uses TCP with 4-byte big-endian length-prefix framing. Before each message payload, the transport prepends the payload's length as a 4-byte unsigned integer in big-endian order. The receiver reads 4 bytes, learns how many bytes to expect, and then reads exactly that many. This is all the framing the protocol needs. TCP keep-alives are enabled to detect dead connections through NAT mappings that would otherwise appear alive indefinitely.

If you are implementing a custom transport over a protocol that already preserves message boundaries — QUIC streams, WebRTC data channels — you do not need to add length-prefix framing. Implement `Send` and `Receive` as direct writes and reads. If your underlying protocol is stream-based, the length prefix approach is the simplest correct solution. For testing purposes, an in-memory transport that connects two endpoints through a pair of buffered channels is invaluable: it removes all network I/O from unit tests and makes connection lifecycle tests deterministic.

**Requirements:**

- **TRP-S1** — A connection MUST preserve message boundaries. One `send` call MUST produce exactly one `receive` call on the other end.
- **TRP-S2** — `send(payload)` MUST either write all payload bytes or return an error. Partial success is not a valid outcome. The return value MUST be the number of payload bytes written, not including any framing overhead added by the implementation.
- **TRP-S3** — `receive()` MUST block until a complete message is available and return only the message body, without any framing headers.
- **TRP-S4** — `remote_address()` returns the remote endpoint as seen by the transport layer. For outbound connections this is the peer's declared listening address. For inbound connections this is the peer's ephemeral source address — which is NOT necessarily their listening address. This matters for protocol design: when node A connects to node B, A's TCP stack picks an ephemeral source port for that connection only. If B later tries to dial A using that port, nothing is listening there. The only reliable way to learn a peer's actual listening address is from the handshake or a higher-layer message — never from the TCP source port. Callers that need the peer's listening address must obtain it through the handshake or a higher-layer mechanism.
- **TRP-S5** — A connection MAY support setting a deadline that causes pending operations to fail after a specified time. Implementations that do not support deadlines ignore such requests gracefully.
- **TRP-S6** — Closing a connection MUST cause any caller blocked on `receive` to return with an error.
- **TRP-S7** — A receiver that encounters an unrecognized frame type MUST use the length prefix to skip past the frame and continue reading. Unknown frame types MUST NOT be treated as a fatal error. This enables future protocol versions to introduce new frame types without breaking existing implementations.

- **TRP-S8** — The transport MUST enforce a configurable maximum frame size. The default MUST be conservative — sized for structured protocol messages, not for bulk data transfer. The reason is that the length field is peer-controlled: a connected peer can claim any frame size up to the maximum and force that allocation on the receiver before sending a single payload byte. With `n` inbound connections and a maximum frame size of `M`, the worst-case forced allocation is `n × M`. An application that needs to transfer large payloads MUST set a higher limit explicitly. This is an intentional opt-in that forces the application to acknowledge the memory tradeoff. The reference implementation uses 64 KiB as the default.


## Codec

Once the transport delivers raw bytes, the codec turns them back into structured data. The codec is the shared encoding agreement across all nodes in a network.

The most important thing to understand about the codec is that it is **not negotiated**. Two nodes do not connect and say "I speak JSON, do you?" The codec is chosen when the network is designed, and every node that joins uses it. A node using a different codec will fail to decode messages from its peers — and that is the correct outcome. It is simply not part of the network.

This might seem rigid, but it is the right constraint. Negotiating encoding at connection time adds complexity, versioning surface, and the possibility of mismatched behavior during upgrades. Treating the codec as a network-level property keeps the connection lifecycle simple and puts the decision where it belongs: in the hands of the network designer, at design time.

**Choosing a codec.** Start with JSON. It is slower and more verbose than binary formats, but those are features during development — you can read messages with any text tool, write tests with inline string fixtures, and debug serialization issues without a decoder. JSON's field-name-based mapping is also forgiving across minor struct changes, which matters while the protocol is still evolving.

Switch to a binary codec when you have a working network and a measurement showing wire size or CPU is a bottleneck. MessagePack is the lowest-friction upgrade from JSON: same data model, binary encoding, roughly 2–5× smaller and faster, no schema compilation step. Protobuf gives you stronger schema guarantees and cross-language type safety at the cost of a `.proto` file and a build step. Whatever you choose, the swap is a single configuration change — the codec is injected, not embedded in protocol logic.

**Requirements:**

- **COD-1** — A codec MUST correctly round-trip any value it encodes: `decode(encode(v)) == v`. A codec MUST NOT return a partial result on encode or decode failure — either the full value is produced, or an error is returned.
- **COD-2** — Every codec MUST have a unique, short string identifier. Two codec implementations with the same identifier MUST produce interoperable output.
- **COD-3** — A codec instance MUST be safe for concurrent use by multiple callers without external synchronization.
- **COD-4** — Message types SHOULD be plain data structures without serialization annotations. The codec is responsible for mapping field names to wire representations through its own convention. Keeping domain types free of codec-specific tags means they can be used in tests, logs, and other contexts without carrying wire format concerns into application code.
- **COD-5** — All nodes in the same network MUST use the same codec. A node that connects with a mismatched codec will fail to decode messages — this is the correct and expected outcome.

**Designing message types.** Because of COD-4, your message types should compile and run cleanly with no import from the codec package. Use explicit, named message type strings within each sub-protocol rather than relying on type names or reflection — strings like `"TEXT"`, `"FILE_REQUEST"`, `"STORE_ACK"` appear on the wire and in logs and should be legible. Keep message structs flat where possible; deeply nested structures are harder to evolve and more likely to produce codec-specific surprises. If you need to embed a variable-length payload (a file chunk, an encrypted blob), use a byte slice field — the codec handles the encoding.


## Frame Format

Transport and framing are separate concerns, and keeping them separate is what makes the stack replaceable. TCP is a byte stream with no concept of message boundaries. QUIC has message boundaries natively. The length-prefix framing described here is what lets you swap TCP for QUIC — or any other boundary-preserving transport — without changing anything in the layers above: if the transport already preserves boundaries, the framing is a no-op. Defining framing separately from transport is what makes that substitution possible.

Between the transport and the application sits the frame. A frame is the smallest unit the node layer works with. It has two fields:

- A **type byte** that identifies the frame's role in the protocol.
- A **payload** — the codec-encoded content, whose meaning depends on the type.

```
┌─────────────────┬──────────────────────────────────────────┐
│  Type (1 byte)  │  Payload (N bytes, codec-encoded)         │
└─────────────────┴──────────────────────────────────────────┘
```

This is what the node layer sees. The transport layer adds a length prefix around this before putting it on the wire:

```
TCP stream:
┌───────────────────────┬─────────────────┬──────────────────────────────┐
│  Length (4 bytes,     │  Type (1 byte)  │  Payload (N bytes,           │
│  big-endian uint32)   │                 │  codec-encoded)               │
└───────────────────────┴─────────────────┴──────────────────────────────┘
│←── transport framing ──────────────────────────────────────────────────→│
                        │←── wire frame (what the node layer encodes) ───→│
                                          │←── codec ────────────────────→│
```

The type byte and the length prefix are raw binary — they are never passed through the codec. Only the payload is codec-encoded. An implementation that passes the entire TCP payload through the codec decoder will fail. The layers are strictly separated: the transport owns the length, the frame owns the type, the codec owns the payload.

### Frame Types

| Value | Name | Description |
|---|---|---|
| `0x01` | `IDENT` | Handshake identity frame. Sent by the initiator in trusted mode. Payload content is defined by the handshake implementation. |
| `0x03` | `DISCONNECT` | Graceful teardown notification. Payload: `{ ReasonCode, ReasonMessage }`. |
| `0x04` | `ERROR` | Protocol-level error. Payload: `{ ErrorCode, ErrorMessage }`. |
| `0x10` | `APPLICATION` | Application message. Payload is an envelope (see below). |

`IDENT` is used only by the trusted-mode handshaker. The verified-mode handshaker sends no frames in either direction — identity is read from the TLS certificate Common Name.

### Error Codes

`ERROR` frames carry a string error code in their payload. The codes have defined meanings:

| Code | Meaning | Connection after |
|---|---|---|
| `DECODE_ERROR` | A frame or envelope could not be decoded. | Closed. |
| `FRAME_TOO_LARGE` | A frame exceeds the configured maximum size. | Closed. |
| `UNSUPPORTED_VERSION` | Reserved for handshake implementations that enforce version compatibility. | Closed. |

`DECODE_ERROR` and `FRAME_TOO_LARGE` close the connection because they represent a fundamental breakdown in the communication contract. When an `APPLICATION` message arrives for an unregistered sub-protocol, the node drops the message silently rather than sending an error — sending an error frame would close the connection, disrupting relay nodes that forward protocols they do not handle locally. See NOD-6 in [`02-node.md`](02-node.md).

### Disconnect Reason Codes

`DISCONNECT` frames carry a reason code:

| Code | Meaning |
|---|---|
| `SHUTDOWN` | The node is shutting down gracefully. |
| `PEER_LOST` | The peer was evicted by the discovery liveness check. |


## Application Message Encoding

An `APPLICATION` frame carries an **envelope**. The envelope is what allows a single connection to serve multiple sub-protocols simultaneously — without it, the node layer would not know which handler to call.

The envelope has three fields:

- **Protocol** — The sub-protocol identifier string (e.g. `"chat/1.0"`).
- **Type** — The message type string within that protocol (e.g. `"TEXT"`). Protocols define their own type strings.
- **Payload** — The encoded message body.

Encoding happens in two steps, both using the same codec:

```
Step 1: Encode the message body
    message = { Text: "hello", Author: "alice" }
    inner_bytes = codec.Encode(message)

Step 2: Encode the envelope
    envelope = { Protocol: "chat/1.0", Type: "TEXT", Payload: inner_bytes }
    envelope_bytes = codec.Encode(envelope)

Step 3: Build and send the frame
    frame_bytes = [0x10] + envelope_bytes          ← type byte + encoded envelope
    transport.Send([length_prefix] + frame_bytes)  ← transport adds length
```

Decoding reverses this:

```
Step 1: Transport strips the length prefix, returns frame_bytes
Step 2: Split frame_bytes into type byte (0x10) and envelope_bytes
Step 3: codec.Decode(envelope_bytes) → envelope
Step 4: Route envelope.Protocol to the registered handler
Step 5: Handler calls decode(&myStruct) → codec.Decode(envelope.Payload, &myStruct)
```

The handler never sees the envelope or the codec directly. It receives the `Protocol`, `Type`, and a decode function that performs Step 5 on demand. This means handlers are fully isolated from wire format concerns and can be tested independently of any codec — pass a test decode function that returns a hardcoded struct, and the handler has no idea it is not on a real connection.

The two-level encoding also means the envelope structure is opaque to any party that does not share the codec. There is no plaintext protocol header visible to a network observer.


## Requirements Summary

| ID | Level | Requirement |
|---|---|---|
| TRP-C1 | MUST | Send a datagram to any address without prior setup |
| TRP-C2 | MUST | Accept datagrams and report the sender's address |
| TRP-C3 | MUST | Make no ordering, delivery, or deduplication guarantees |
| TRP-C4 | MAY | Impose a maximum datagram size bounded by the MTU |
| TRP-S1 | MUST | Preserve message boundaries: one send → one receive |
| TRP-S2 | MUST | Send all bytes or return an error; no partial success |
| TRP-S3 | MUST | Block on receive until a complete message is available; strip framing |
| TRP-S4 | MUST | Report remote address: declared for outbound, ephemeral for inbound |
| TRP-S5 | MAY | Support per-connection deadlines |
| TRP-S6 | MUST | Closing a connection unblocks any pending receive with an error |
| TRP-S7 | MUST | Skip unknown frame types; do not treat them as fatal |
| TRP-S8 | MUST | Enforce a configurable max frame size; default MUST be conservative (64 KiB); opt-in required for larger payloads |
| COD-1 | MUST | Round-trip any encoded value; no partial results on failure |
| COD-2 | MUST | Carry a unique string identifier; same ID implies interoperable output |
| COD-3 | MUST | Be safe for concurrent use without external synchronization |
| COD-4 | SHOULD | Accept plain structs without serialization annotations |
| COD-5 | MUST | All nodes in a network use the same codec |
