# Discovery

The node layer (see [`02-node.md`](02-node.md)) knows how to manage connections. What it cannot do on its own is find peers to connect to. In a centralized system that is trivial — the server's address is baked in. In a P2P network there is no central registry, and yet every new node needs to find at least a few others to bootstrap its view of the network.

Discovery is the layer that solves this problem. It runs over UDP, is independent of the TCP connection layer, and has one output: notifications that tell the node layer when peers become reachable or go away. The mechanism used to deliver those notifications — callback, event queue, observer, channel — is an implementation detail. The contract is behavioral: the node layer eventually receives every transition.


## The Bootstrap Problem

The first node to join a new network has an easy time: it has no peers yet, and that is fine. The second node has to find the first. Every subsequent node has the same challenge — without already knowing someone, how do you join?

The standard solution is the **bootstrap node**: a node with a known, stable address that is always running and maintains a directory of active peers. New nodes contact the bootstrap on startup, announce themselves, and receive a list of peers in return. Once they have a few peers, they can discover more on their own through ongoing liveness checks.

A bootstrap node is not a special binary or a special type. **Every node handles `ANNOUNCE` and `FIND_PEERS`** — there is no flag or configuration that distinguishes a bootstrap node from a regular peer at the protocol level. A "bootstrap node" is simply a `NewPeer` with no application handlers (and therefore an empty protocol list), deployed on stable infrastructure with a well-known address. It participates in discovery like any other node.

**Resilience.** A network with a single bootstrap node has a single point of failure at join time — new nodes cannot join if the bootstrap is down, even if thousands of peers are already running. Run at least two bootstrap nodes on separate infrastructure. Their addresses are the one piece of hardcoded configuration every node carries, so use stable domain names rather than raw IP addresses. This lets you replace the underlying machines without reconfiguring existing nodes.


## Startup Sequence

When a node starts discovery, it performs two operations in order for each configured bootstrap address:

```
Node                                    Bootstrap
  │                                         │
  │── ANNOUNCE { NodeID, Address,           │
  │             Protocols: ["chat/1.0"] } ─▶│  "I exist, here's my capabilities"
  │                                         │  (Bootstrap records the node)
  │── FIND_PEERS { NodeID } ───────────────▶│  "Who else is out there?"
  │                                         │
  │◀── PEERS [{ NodeID, Address,            │
  │            Protocols: [...] }, ...]     │  "Here are the peers I know about"
  │       + self-entry (bootstrap itself)   │     (including bootstrap's own entry)
  │                                         │
  │  (for each returned entry)              │
  │  → add to peer table                    │
  │  → emit peer-found event with protocols │
```

The order matters. The node announces first so the bootstrap records its address before it asks for peers. `ANNOUNCE` carries the node's registered protocol list — this tells the bootstrap (and any peer that receives it) which sub-protocols the announcing node supports, without requiring a TCP connection first.

**DISC-1** — On startup, a node MUST send an `ANNOUNCE` message to every configured bootstrap address before sending `FIND_PEERS`. The announcement MUST include the node's identity, address, and its current registered protocol list.

**DISC-2** — After announcing, a node MUST send a `FIND_PEERS` request to each configured bootstrap address. For each peer returned that is not the local node, the peer MUST be added to the peer table and a `peer-found` event MUST be emitted if the peer is new to the table.

**What happens when bootstraps are unavailable.** If all configured bootstrap nodes are unreachable at startup, a node will announce into silence and receive no peers. It will not error out — it will start with an empty peer table and begin probing. This is a recoverable situation: once a bootstrap comes back, nodes that re-announce will rejoin the directory. An established network with active liveness pings continues operating without the bootstrap — the bootstrap is only needed at join time.


## The Peer Table

The peer table is the discovery layer's local registry of currently live peers, keyed by node identity. It is distinct from the node layer's connection map — discovery can know about a peer before the TCP connection is established, and discovery's liveness checks are independent of whether a TCP connection exists.

A `peer-found` event is emitted only when a peer is added to an empty slot — that is, when the peer is genuinely new. If a peer is re-confirmed alive by a ping response, the peer table updates internally but does not re-emit `peer-found`.

**DISC-3** — A `peer-found` event MUST NOT be emitted more than once for the same peer while that peer remains in the peer table.


## Liveness

Knowing that a peer existed five minutes ago is not the same as knowing it exists now. Nodes crash, get rebooted, or lose connectivity. The discovery layer monitors peer liveness continuously so the node layer does not have to — the `peer-lost` event is the mechanism by which dead connections get cleaned up.

The liveness mechanism is a simple ping-pong protocol over UDP:

```
Node A                    Node B
  │── PING { Nonce } ────▶│
  │◀── PONG { Nonce } ────│
```

A **nonce** is a random value included in the `PING` and echoed unchanged in the `PONG`. Its purpose is to match responses to their requests — without it, a `PONG` received after a long delay could be matched to the wrong outstanding ping. The nonce MUST be a UUID v4 string in canonical hyphenated form (e.g. `"f47ac10b-58cc-4372-a567-0e02b2c3d479"`). This format is consistent with DHT request IDs and gives sufficient randomness to make collisions negligible across concurrent in-flight pings.

At each liveness interval, the discovery layer sends a `PING` to every known peer and records the nonce for that peer. If a matching `PONG` arrives before the next interval, the probe is satisfied and the missed-ping counter resets. If no matching `PONG` arrives, the counter increments. When the counter exceeds the configured maximum, the peer is evicted.

**DISC-4** — The discovery layer MUST periodically probe each known peer with a `PING` carrying a unique nonce. At most one outstanding nonce per peer is tracked — a new probe cycle replaces any unmatched nonce from the previous cycle.

**DISC-5** — A peer that accumulates more than the configured number of consecutive missed pings MUST be removed from the peer table and a `peer-lost` event MUST be emitted.

**DISC-6** — Any node receiving a `PING` MUST reply with a `PONG` carrying the same nonce unchanged. Both bootstrap and non-bootstrap nodes respond to pings.

The missed-ping counter only increments for pings that were actually sent. A peer added to the table very recently, before the first ping of a given interval, does not accumulate a missed ping for that interval.

**Tuning liveness for your network.** The defaults (1-second interval, 3 missed pings, ~3-second eviction window) are appropriate for most deployments — a 1-second ping catches dead peers quickly without generating excessive UDP traffic on typical internet or LAN links. For a LAN or container network where packet loss is near zero, tighten further: 2 missed pings gives a 2-second eviction window and faster connection cleanup. For a network spanning continents where round-trip times can reach 200–300ms and occasional loss is expected, loosen them: a 5-second interval and 5 missed pings gives a 25-second window. Aggressive eviction on a high-latency network causes spurious disconnections and reconnections that waste bandwidth and destabilize routing tables.


## Discovery Role

Every node handles `ANNOUNCE` and `FIND_PEERS` — there is no separate bootstrap mode at the protocol level. The common peer directory and notification logic is identical for all nodes.

This design is deliberate. Dedicated bootstrap infrastructure would recreate the same centralization problem that P2P networks are designed to avoid: a single failure point that new nodes depend on to join. When every node handles `ANNOUNCE` and `FIND_PEERS`, any node that is online and reachable can help a new node join the network. A "bootstrap node" is a deployment role — running a stable instance at a well-known address — not a protocol type. There is no protocol distinction between a bootstrap and a regular peer.

**DISC-7** — On receiving `ANNOUNCE`: the node MUST add the announcing peer to its peer table (if new) and MUST respond with a `PEERS` message. In verified mode, the node MUST first verify the ANNOUNCE signature before adding the peer; invalid signatures MUST be silently dropped.

**DISC-8** — On receiving `FIND_PEERS`: the node MUST respond with a `PEERS` message.

**DISC-9** — On receiving `PING`: the node MUST respond with a `PONG` carrying the same nonce.

**DISC-10** — *(removed — all nodes handle ANNOUNCE and FIND_PEERS)*

**DISC-13** — A `PEERS` response MUST NOT contain more than 100 peer entries. If the table exceeds this count, the response SHOULD contain a random sample. Receivers MUST be prepared to handle any count from 0 to 100.

**DISC-14** — Every `PEERS` response MUST include the responding node's own entry (NodeID, Address, Protocols), unless the requester's NodeID equals the local NodeID. This ensures that the receiver learns the responder's capabilities from the first UDP exchange, before any TCP connection is established.

The peer directory is kept live by the same ping-pong mechanism for all nodes. Nodes do not need to know about each other as bootstraps. A node that contacts multiple well-known addresses receives multiple peer lists and merges them — duplicates are harmless.


## Codec Agreement

Discovery messages are encoded with the network codec, the same codec used by the node layer. All nodes in a discovery group must use the same one.

**DISC-11** — A packet that cannot be decoded by the configured codec MUST be silently dropped.

**DISC-12** — A packet missing the `Type` discriminator field, or whose `Type` is not recognized, MUST be silently dropped.

Silent dropping (rather than returning an error or logging a warning) is the correct behavior for malformed packets. In shared network environments, spurious UDP traffic is common. Logging every unrecognized packet would produce noise that obscures real issues.


## The Advertise Address

The `AdvertiseAddr` is the address the node announces to the bootstrap and that gets shared with other peers as the address to dial for TCP connections. It must be a routable address — one that other nodes on the network can actually reach.

This is the single most common misconfiguration in UDP-based P2P networks. When a node runs behind NAT — inside a Docker container, in a cloud VM with a private IP, or on a home network — the address it listens on (`0.0.0.0:9000` or `192.168.1.5:9000`) is not routable from the outside. If `AdvertiseAddr` is not set explicitly, the listen address is used as a fallback, which produces peer table entries that no one can dial.

Set `AdvertiseAddr` to the externally routable address. In a cloud environment, this is the instance's public or Elastic IP. In Docker, it is the host machine's IP. For local development and testing, use `127.0.0.1` explicitly rather than `0.0.0.0` — even if everything runs on one machine, the distinction matters once you run more than one node.


## Peer Lifecycle Notifications

The discovery layer's output is two notification types. How they are delivered to the node layer is implementation-defined — a callback, a queue, an observer pattern, or any other mechanism that preserves the behavioral guarantee: the node layer eventually receives every transition.

| Event | Meaning | Carries |
|---|---|---|
| `peer-found` | A peer is confirmed reachable for the first time. | Peer identity, peer address, protocol list (nil if unknown, `[]string{}` if known-empty) |
| `peer-lost` | A peer was evicted for failing liveness checks. | Peer identity |

The `Protocols` field in a `peer-found` notification is significant: `nil` means the discovery message did not include a protocol list (capabilities unknown — dial anyway); `[]string{}` (non-nil, explicitly empty) means the peer has no application protocols. The node layer uses this distinction to decide whether to initiate an outbound TCP connection (see NOD-9).

Implementations SHOULD deliver notifications with a bounded backlog. If the consumer does not process notifications promptly and the backlog fills, excess `peer-found` notifications MAY be dropped. The result is one fewer peer connection, not a crashed node — the network remains functional. Implementations MUST NOT block the discovery receive loop waiting for a slow consumer; the discovery loop must stay responsive to incoming UDP traffic regardless of how fast higher layers process its output.


## Message Reference

This is the wire format contract. Any implementation in any language that produces and consumes these messages correctly is interoperable with any other conformant implementation.

All messages carry a `Type` field as the discriminator.

| Message | Direction | Fields |
|---|---|---|
| `ANNOUNCE` | Any → Any | `Type`, `NodeID`, `Address` (routable advertise address), `Protocols` (string list; omitempty allowed for unknown), `PublicKey` (base64 Ed25519; verified mode only), `Signature` (base64; verified mode only) |
| `FIND_PEERS` | Any → Any | `Type`, `NodeID` |
| `PEERS` | Any → Any | `Type`, `Peers[]` where each entry is `{ NodeID, Address, Protocols }` |
| `PING` | Any → Any | `Type`, `NodeID` (sender), `Nonce` |
| `PONG` | Any → Any | `Type`, `NodeID` (sender), `Nonce` (echoed unchanged) |

`Protocols` in `PEERS` entries is always present (never omitted): `[]` (empty array) means the peer has no application protocols; a non-empty array lists the protocols. `nil` / absent means capabilities unknown — implementations that write legacy messages without the field decode it as `nil` and must treat it as unknown.


## Configuration Reference

| Parameter | Default | Description |
|---|---|---|
| Node identity | required | Stable identifier for this node. Must match the identity used by the node layer. |
| Listen address | required | UDP address to receive messages on. |
| Advertise address | listen address | The address announced to peers. Must be routable — never `0.0.0.0`. See above. |
| Bootstrap addresses | none | UDP addresses of well-known nodes to contact on startup. |
| Ping interval | 1s | How often to probe peer liveness. |
| Max missed pings | 3 | Missed probes before eviction. With defaults, a silent peer is evicted within ~3s. |
| Max peers | 0 (unbounded) | Maximum peer table size. When full and a new ANNOUNCE arrives, the peer with the most consecutive missed pings is evicted; ties are broken randomly. Set this on bootstrap nodes to bound memory and limit Sybil pre-join flooding. |
| Codec | required | Encoding for all UDP messages. Must match the codec used by all other nodes. |


## Requirements Summary

| ID | Level | Requirement |
|---|---|---|
| DISC-1 | MUST | Send ANNOUNCE (with protocol list) to all bootstrap addresses before FIND_PEERS on startup |
| DISC-2 | MUST | Send FIND_PEERS after ANNOUNCE; add returned peers to table; emit peer-found (with protocols) for new ones |
| DISC-3 | MUST | Emit peer-found at most once per peer per table entry |
| DISC-4 | MUST | Probe each known peer with a PING carrying a UUID v4 nonce; invalidate prior nonce on new cycle |
| DISC-5 | MUST | Evict a peer after exceeding the missed-ping threshold; emit peer-lost |
| DISC-6 | MUST | Reply to any PING with a PONG carrying the same nonce unchanged |
| DISC-7 | MUST | On ANNOUNCE: record peer (verify signature in verified mode) and reply with PEERS |
| DISC-8 | MUST | On FIND_PEERS: reply with PEERS |
| DISC-9 | MUST | On PING: reply with a PONG |
| DISC-11 | MUST | Silently drop packets that fail codec decoding |
| DISC-12 | MUST | Silently drop packets with missing or unrecognized Type field |
| DISC-13 | MUST / SHOULD | Cap PEERS responses at 100 entries; sample randomly when over limit |
| DISC-14 | MUST | Include self-entry (NodeID, Address, Protocols) in every PEERS response, unless requester == self |
| DISC-15 | SHOULD | When MaxPeers is set and the table is full, evict the peer with the most missed pings before inserting a new one; break ties randomly; emit peer-lost for the evicted peer |
