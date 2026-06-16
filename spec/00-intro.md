# A P2P Framework: Introduction

## The Problem with a Center

Most networked software is built around a center. A chat app has a server that stores messages and delivers them. A file-sharing site has a server that holds the files. A game has a server that maintains the authoritative state. Clients connect to it, ask for things, and disconnect. The model is simple to reason about: you always know who to talk to.

The center is also a liability. It is a single point of failure. It is a scaling bottleneck. It is a target for censorship, attack, or operator abandonment. When the server goes away, so does everything it hosted.

Peer-to-peer (P2P) networks remove the center. Every participant is both a client and a server. No single node is the authority. The network continues to function as long as enough peers remain reachable — removing any one node, or any hundred nodes, does not bring it down.

This resilience comes at a cost: coordination is harder. In a centralized system you always know the server's address. In a P2P network, you need a mechanism to find other peers, a way to maintain those connections, and — if you want to store data across the network — a strategy for deciding where data lives and how to find it. These are not incidental problems; they are the core engineering challenges of P2P design. This library addresses each one as a separate, composable layer.


## Vocabulary

These terms appear throughout the specs. They are defined here, before anything else, so you encounter them with meaning already attached.

**Node** — A single participant in the network. Has a stable identity, listens on a TCP address for incoming connections, and sends and receives messages over those connections.

**Peer** — Another node with which a connection has been established. "Peer" always implies an active, handshaked connection. A node that has been discovered but not yet connected is not yet a peer.

**Sub-protocol** — A named, versioned namespace for application messages. Examples: `chat/1.0`, `sync/2.0`, `dht/1.0`. Multiple sub-protocols share the same connection. Each sub-protocol has exactly one handler registered per node.

**Handler** — A function the application provides for a sub-protocol. Called once per inbound message with the sender's identity, the message type string, and a decode function for the payload.

**Codec** — The encoding shared by all nodes in a network. Turns a struct into bytes and back. The codec is not negotiated between peers — it is a design-time property of the network, like a shared language. Every node must use the same one.

**Frame** — The unit of transmission at the wire layer. Every frame has a type byte and a codec-encoded payload. The transport adds a length prefix to handle stream reassembly.

**Envelope** — The structure inside an APPLICATION frame. Carries the sub-protocol identifier, the message type string, and the encoded message body. The node layer uses the envelope to route the message to the correct handler.

**Bootstrap node** — A node with a known, stable address that helps new nodes join the network. Every node handles `ANNOUNCE` and `FIND_PEERS`, so any node can act as a bootstrap. In practice, bootstrap nodes are run on stable infrastructure with no application handlers — their protocol list is empty, which tells peers not to initiate application-layer TCP connections to them. It is a deployment role, not a protocol type.

**Peer table** — A discovery-layer registry of currently live peers, keyed by node identity. Distinct from the node layer's connection map, though they stay in sync through `peer-found` / `peer-lost` events.

**DHT key** — A 256-bit value derived by SHA-256 hashing an arbitrary input. Every node has a position in the DHT key space based on its identity. Closeness between two keys is measured by XOR.

**Wire layer** — The combination of transport, framing, and codec. It is the foundation every other layer sits on: it turns structured data into bytes, delivers those bytes, and turns them back. Covered in [`01-wire.md`](01-wire.md).


## How the Layers Fit Together

```
┌──────────────────────────────────────────────────────────────┐
│                   Your Application Protocols                  │
│            e.g. chat/1.0, sync/1.0, fileshare/1.0            │
├──────────────────────────────────────────────────────────────┤
│                         Node Layer                            │
│       connection lifecycle · handshake · message dispatch     │
├──────────────────────────────────────────────────────────────┤
│         Codec (pluggable)    │    Transport (pluggable)       │
├──────────────────────────────┴───────────────────────────────┤
│                       Discovery Layer                         │
│              UDP · bootstrap · liveness probing               │
└──────────────────────────────────────────────────────────────┘
                              ↕  optional
┌──────────────────────────────────────────────────────────────┐
│                Kademlia DHT  (dht/1.0 sub-protocol)           │
└──────────────────────────────────────────────────────────────┘
```

The **wire layer** (transport + codec + framing) handles the mechanics of turning structured data into bytes and back. It sits beneath everything else and is covered in [`01-wire.md`](01-wire.md). Every design decision at this layer — which codec, which transport — propagates to all nodes in the network and cannot be changed without replacing the network.

The **node layer** sits on top of wire. It manages the lifecycle of connections to remote peers, runs the handshake that establishes identity, and routes incoming messages to the right handler. It is covered in [`02-node.md`](02-node.md). The sub-protocols your application registers here define the message contracts your network speaks.

The **discovery layer** runs in parallel, over UDP. It finds peers and monitors whether they are still alive, emitting events that the node layer consumes to open and close connections. It is covered in [`03-discovery.md`](03-discovery.md). The bootstrap nodes you configure are the entry points to your network — their addresses are the one piece of hardcoded configuration every node carries.

The **DHT** is an optional layer that sits above the node layer. It registers itself as a sub-protocol (`dht/1.0`) and implements Kademlia-based distributed key-value storage. It is covered in [`04-dht.md`](04-dht.md). You only need it if your application needs to answer "which node holds X?" without a central index.


### Protocol Patterns

The node layer's `Send` and handler registration are fire-and-forget primitives: you send a message and the handler is called when one arrives; neither side blocks waiting for the other. Three messaging patterns compose from this primitive. **Fire-and-forget** is the primitive itself — a sub-protocol calls `Send` and the peer's registered handler receives it; there is no return value. **Request/response** requires correlation: the caller generates a request ID, registers a pending entry keyed by that ID before sending, and delivers the response to that entry when a matching reply arrives. The pending entry must be registered before the message is sent — a response that arrives before the caller registers would otherwise be silently dropped. **Broadcast** is `Send` called for every connected peer; the node layer has no built-in broadcast, so sub-protocols that need it iterate the peer list themselves.


## A Concrete Scenario

Reading specs is easier when you have a story to anchor the terms to. Here is what happens when a node joins the network and exchanges a message.

**Step 1 — Identity.** A node generates a stable identifier it carries across restarts. In trusted mode, this is a UUID — generated once, written to disk, and read back on every subsequent startup. In verified mode, the node generates an Ed25519 keypair; its identity is `SHA-256(public_key)` as lowercase hex. Either way, identity is stable — the same node restarting appears under the same identity to its peers.

**Step 2 — Register handlers.** Before starting, the application registers a handler for each sub-protocol it supports — say `chat/1.0`. This tells the node: "when you receive a message addressed to `chat/1.0`, call this function." Registration must happen before the node starts; there is no dynamic registration once connections begin.

**Step 3 — Announcing presence.** The node starts. Discovery sends an `ANNOUNCE` message over UDP to each configured bootstrap address: "I exist, my identity is X, I can be reached at address Y, and I speak these protocols." The node announces before asking for peers — this ensures the bootstrap has recorded it before handing back peer lists that others might use to find it.

**Step 4 — Finding peers.** Immediately after announcing, discovery sends `FIND_PEERS` to each bootstrap. The bootstrap responds with a `PEERS` message containing known live nodes — each entry includes the peer's address and protocol list. The response also includes the bootstrap's own entry, so the receiver immediately learns the bootstrap's capabilities without a separate TCP connection. For each returned peer that is not the local node, discovery adds it to a local peer table and emits a `peer-found` event.

**Step 5 — Connecting.** The node layer watches `peer-found` events. When one arrives, it dials the peer's TCP address and runs a handshake. The handshake's minimum job is to learn the remote peer's identity. Once it succeeds, the connection is registered under that peer's identity and becomes available for sending and receiving.

**Step 6 — Receiving a message.** The peer sends a message. It arrives as bytes on the TCP connection. The node's read loop decodes the frame, then decodes the envelope inside to learn which sub-protocol and message type it belongs to. It calls the registered handler for `chat/1.0`, passing the peer's identity, the message type, and a decode function the handler uses to deserialize the payload.

**Step 7 — Staying alive.** Every few seconds, discovery sends a `PING` to each known peer and waits for a `PONG`. A peer that misses enough consecutive pings is evicted from the peer table. Discovery emits a `peer-lost` event and the node layer closes the connection.

This is the full loop. Every layer has one job. They communicate through narrow interfaces: discovery produces events; the node layer consumes them. The application registers handlers; the node layer calls them.


## Security

This section is upfront because it determines which components you choose and what guarantees your network carries.

The library operates in two modes. Both are fully implemented. The choice is a design decision, not a version gap.

### Trusted mode

**Do not use trusted mode on any network you do not completely own. This includes the open internet, any shared cloud environment, and any machine with more than one user.**

Trusted mode uses a UUID identity, plaintext TCP, and no transport encryption. It is appropriate for development environments, private infrastructure meshes where you control every node, and local testing. The assumption is that all participants are legitimate by construction — and that assumption must hold for every node in the network, not just most of them.

What trusted mode actually provides is a *label*, not an identity. The UUID is written to a file and read back on startup. It proves nothing. Any process that can read that file, any node that knows the format, can announce itself under any NodeID it chooses. The IDENT handshake is a self-assertion: "I am X." There is no cryptographic proof. The receiver has no way to challenge it.

In trusted mode:
- **Impersonation is trivial.** Any reachable node can claim any NodeID. Filesystem access to the identity file, or simply knowing the format, is enough to take over any peer's identity in the network.
- **All traffic is plaintext.** Every message is readable by any observer on the network path. Frame injection is possible. A forged `DISCONNECT` frame closes any connection. A forged `ANNOUNCE` can poison the bootstrap's peer table.
- **The DHT is structurally correct but completely untrustworthy.** Kademlia's XOR metric finds the nodes that *should* hold a key — but in trusted mode, identity is free. An adversary generates NodeIDs that cluster around any target key and controls every lookup for that region. This is not a difficult attack: it takes minutes to script. **Kademlia alone does not provide eclipse resistance.**

Trusted mode is not a weaker version of verified mode. It is a different operational model — one where the network boundary is an administrative boundary, not a cryptographic one. If that boundary can be crossed by an untrusted party, use verified mode.

### Verified mode

Verified mode replaces UUID identity with Ed25519 keypairs and plaintext TCP with mutual TLS. It is implemented in `pkg/identity`, `pkg/transport/tls`, and `identify.NewSecure`. The switch from trusted to verified is two lines in the node configuration — everything else (discovery, sub-protocols, DHT, codec) is unchanged.

**How it works.** Each node generates an Ed25519 keypair. Its NodeID is `SHA-256(public_key)` as lowercase hex. The keypair is wrapped in a self-signed X.509 certificate whose Common Name is the NodeID. When two nodes connect:

```
TLS handshake (inside pkg/transport/tls):
  ├── both sides present Ed25519 self-signed certificates
  ├── verifyP2PCert confirms: CN == SHA-256(public_key)   ← identity binding
  └── TLS proves: presenter holds the corresponding private key ← authentication

identify.NewSecure (verified handshaker):
  └── reads PeerID from the verified certificate CN
      (no application-layer HELLO frame is sent — TLS already provided the proof)
```

Key ownership and identity binding are verified inside the TLS handshake, which runs within the application handshaker (`identify.NewSecure`) before the connection is promoted to an established peer. A certificate with a mismatched or fabricated NodeID fails during the TLS exchange and the connection is closed without ever being registered.

**What verified mode provides:**

| Property | Trusted | Verified |
|---|---|---|
| Transport confidentiality | ✗ Plaintext TCP | ✓ TLS |
| Transport integrity | ✗ No MAC | ✓ TLS record MAC |
| Authenticated identity | ✗ UUID, asserted | ✓ Ed25519, cryptographically bound |
| Impersonation resistance | ✗ | ✓ Requires holding the private key |
| Targeted key-space positioning | ✗ Free | ✓ Infeasible (preimage of SHA-256) |
| Bootstrap identity forgery | ✗ Trivial | ✓ Closed — signed ANNOUNCE |
| Routing table injection | ✗ Trivial | ✓ Closed — entries without proof of identity rejected |
| Eclipse via Sybil (real nodes) | ✗ Trivial | ⚠ Mitigated — requires real infrastructure |
| Discovery (UDP) | ✗ Plaintext | ✗ Plaintext |
| Peer table size cap | ✗ Unbounded | ✗ Unbounded (planned) |
| Disjoint-path lookups | ✗ | ✗ Planned (S/Kademlia) |

**What verified mode closes.**

**Bootstrap identity forgery.** In verified mode, every `ANNOUNCE` message carries an Ed25519 signature over the claimed NodeID and address. The bootstrap verifies `SHA-256(public_key) == NodeID` and validates the signature before adding the peer to its directory. An adversary cannot announce a NodeID they do not own the private key for. This closes the primary bootstrap poisoning vector.

**Routing table injection.** Every `FIND_NODE` response entry must carry the responder's public key. The receiver verifies `SHA-256(public_key) == NodeID` and rejects any entry that omits the key or where the key does not match the claimed identity. A malicious node cannot inject fabricated entries — neither a wrong key nor a missing key passes. The routing table only accumulates entries backed by cryptographic proof of identity.

Together, these two defenses mean that in verified mode, eclipse via fabrication is closed at both the discovery layer and the DHT routing layer. An adversary cannot insert nodes they don't own into either layer.

**What verified mode does not fully close.**

**Eclipse via Sybil with real nodes.** An adversary who generates many valid Ed25519 keypairs and runs actual network nodes can still statistically dominate key space regions. They cannot target specific positions (SHA-256 produces unpredictable positions), but with enough nodes they can achieve probabilistic dominance. The attack now requires real running infrastructure — not just free identity generation — which raises the cost significantly. For most deployments this is an acceptable remaining risk, but it is not zero.

**Bootstrap peer table is unbounded.** A Sybil attack with many real signing nodes can flood a bootstrap's peer table without limit. There is no eviction policy or maximum size. An adversary who pre-populates the bootstrap table with thousands of real adversarial nodes before legitimate peers announce will dominate the FIND_PEERS responses seen by joining nodes.

**Discovery traffic is plaintext UDP.** The signature on ANNOUNCE proves identity but does not encrypt the announcement. Observers on the network path see which nodes are joining the network and which bootstraps they contact.

### What remains

**Peer table size cap.** Bootstrap nodes currently accept all verified ANNOUNCEs without a table size limit. A Sybil attack with real nodes can flood the table. Adding a configurable maximum with an eviction policy (e.g., evict the oldest uncontacted entry when full) would bound memory and limit the effectiveness of pre-join flooding.

**S/Kademlia disjoint-path lookups.** Lookups follow a single converging path. Parallel lookups on `k` disjoint paths (Baumgart & Mies, 2007) are required for full eclipse resistance against an adversary with many legitimate-but-malicious nodes already in the routing table. Without this, an adversary who controls one routing path can steer a lookup even with signed routing entries. This is the remaining architectural gap for complete eclipse resistance in verified mode.


## Reading Order

If you are implementing this protocol from scratch, read the specs in order:

1. [`01-wire.md`](01-wire.md) — Start here. Every other layer sits on top of the byte machinery explained here.
2. [`02-node.md`](02-node.md) — How connections come alive, exchange identity, and route messages.
3. [`03-discovery.md`](03-discovery.md) — How nodes find each other without a central directory.
4. [`04-dht.md`](04-dht.md) — How data is stored and retrieved across the network. Optional if you do not need distributed storage.

If you are implementing a specific layer in isolation — for example, replacing the discovery mechanism while keeping everything else — start with the wire spec (because all layers share the encoding machinery) and then read only the spec for the layer you are building.
