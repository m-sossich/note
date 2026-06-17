# Distributed Hash Table

Discovery (see [`03-discovery.md`](03-discovery.md)) tells you who is online. It does not tell you what they have. Imagine you want to find a file shared somewhere across the network, or locate a service running on one of a thousand nodes. Without a central index, the only option is to ask everyone — which scales like a disaster.

The **distributed hash table** (DHT) solves this. It spreads key-value storage across all participating nodes in a structured way, so that any node can find the node responsible for a given key using only a small number of hops — proportional to the logarithm of the network size, not the network size itself.

This implementation follows the Kademlia protocol, introduced by Maymounkov and Mazières in their 2002 paper [*Kademlia: A Peer-to-peer Information System Based on the XOR Metric*](https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf). Reading the paper alongside this spec is worthwhile — it provides the theoretical underpinning for design decisions that might otherwise seem arbitrary.

The DHT is optional. If your application does not need to answer "which node holds X?" without a central authority — if your network is about communication between peers rather than distributed storage — you do not need it. A mesh chat, a presence system, a streaming overlay: these only need discovery and direct peer messaging. The DHT is the right tool when data must outlive any single node's session, or when consumers need to find providers without knowing their address in advance.

The DHT runs as a sub-protocol (`dht/1.0`) layered on top of the node layer. It registers a handler and sends messages like any other application protocol. It requires no changes to transport, codec, or connection lifecycle.


## The Key Space

Every piece of data stored in the DHT is identified by a **key** — a 256-bit value derived by applying SHA-256 to an arbitrary input. When you store the string `"readme.txt"`, the DHT internally computes `SHA-256("readme.txt")` and uses that 32-byte value as the key.

Every **node** also has a position in this same 256-bit key space, derived by applying SHA-256 to its node identity string. This is the node's **DHT key**.

The key space is shared between data and nodes. This shared space is what makes routing possible: you can measure the "distance" between a data key and a node key, and use that distance to determine which node is "closest" to the data — and therefore responsible for storing it.


## Distance: The XOR Metric

Distance in Kademlia is measured by the **XOR** of two keys:

```
distance(A, B) = A XOR B
```

XOR has two properties that make it ideal as a distance metric:

**It is symmetric.** `distance(A, B) == distance(B, A)`. The distance from A to B is the same as the distance from B to A.

**It is deterministic.** Given A and B, the distance is always the same across all nodes. Every node independently agrees on which node is closest to a given key, without any coordination.

A lower XOR value means closer proximity. Two identical keys have distance zero.

To build intuition: two keys are "close" in XOR space when their leading bits match. The more leading bits they share, the closer they are. This is quantified by the **common prefix length** (CPL): the number of leading bits that are the same in both keys. A higher CPL means a smaller XOR distance.


## The Routing Table

Each node maintains a **routing table** — its local knowledge of other nodes in the network, organized for efficient routing.

### K-Buckets

The routing table is divided into 256 **k-buckets**, one per possible CPL value. A node's entry belongs in bucket `i` where `i` is the CPL between that node's key and the local node's key — equivalently, the position of the highest set bit in their XOR distance.

Each k-bucket holds up to `k` entries (the **bucket size**, default 8). Entries are ordered by last-seen time, oldest first.

When a new node is discovered for a bucket that already has `k` entries, the protocol does not blindly replace the oldest entry. Long-lived nodes are more likely to stay alive than newly seen ones — this is an empirically observed property of P2P networks. The replacement policy reflects this:

```
New node arrives for a full bucket:

  1. Send a liveness probe to the oldest entry.
  2. If the oldest entry does NOT respond:
       → Replace it with the new node.
  3. If the oldest entry DOES respond:
       → Discard the new node.
       → Move the responding entry to the most-recently-seen position.
```

The result is that stable, long-running nodes stay in the routing table even as newer ones come and go. Networks built this way converge faster and remain more stable over time.

**Probe implementation.** The liveness probe is a `FIND_NODE` RPC sent to the LRS entry with its own local key as the target. This confirms reachability and returns useful routing information if the peer responds. The probe uses a shorter timeout than regular DHT operations — at most half the configured `RequestTimeout`, capped at 2 seconds — because a liveness check should fail fast.

Because DHT messages travel over TCP sub-protocol connections, a probe only succeeds if an active connection to the LRS currently exists. A node in the routing table that this node is not presently connected to fails the probe immediately and is evicted. This is the correct outcome: a peer that cannot be reached has no routing value regardless of how long it has been known.

### Routing Table Updates

**DHT-1** — The routing table MUST be updated on every inbound message — both requests and responses. A response from a peer confirms they are alive, so their last-seen time SHOULD be refreshed.

**DHT-2** — The routing table MUST NOT be populated with entries derived from ephemeral source addresses. When a peer sends a message over an inbound TCP connection, the address observed by the transport is their ephemeral source port — not their declared listening address. Routing table entries built from ephemeral addresses cannot be dialed by other nodes. See [Known Limitations](#known-limitations).


## Provider Records

The fundamental storage unit in this DHT is the **provider record** — a declaration by a node that it holds data for a given key. A provider record carries:

- **NodeID** — the announcing node's identity.
- **Address** — the node's declared listening address, included explicitly so any node that receives the record can dial the provider without relying on the ephemeral source port of the current connection.
- **Value** — optional metadata (base64-encoded bytes). Its meaning is application-defined.

Multiple nodes MAY announce themselves as providers for the same key. The DHT accumulates all provider records found during a lookup and returns them all. What the consumer does with multiple providers is an application decision.

### Single-holder and multi-holder patterns

These two patterns emerge naturally from the same protocol. No configuration flag distinguishes them — cardinality is an application-level choice.

**Single-holder**: one node announces for a key. Useful for routing (`service-key → endpoint`), identity resolution (`nodeID → current address`), or any resource with a single authoritative holder. The consumer checks that at least one provider was found and uses the first.

**Multi-holder**: multiple nodes announce for the same key. Useful for content distribution — any node that holds a piece of content announces under its hash. The consumer can dial any provider and fetch from the closest or most responsive. This is the model used by BitTorrent and IPFS: `content_hash → [peer_A, peer_B, peer_C]`, where each peer stores and serves the same data independently.

In the multi-holder model, nodes republish periodically. A provider that stops republishing before its TTL (as configured by the receiver's storage policy) is silently forgotten. The consumer's view of "who has X" naturally converges to currently-live holders.

## Operations

The DHT exposes three operations. Each is implemented as a request-response exchange over the `dht/1.0` sub-protocol.

### FIND_NODE

Ask a peer for the `k` nodes in its routing table that are closest to a given key.

- Request carries: a target key (lowercase hex-encoded 32 bytes), a request ID.
- Response carries: up to `k` node descriptors, the same request ID.

A node descriptor includes the node identity, its declared network address, and its DHT key.

### FIND_PROVIDERS

Ask a peer for all provider records stored at a given key, or the closest nodes if none are held.

- Request carries: a target key (lowercase hex-encoded 32 bytes), a request ID.
- Response (providers found): a list of provider records, the same request ID.
- Response (not found): up to `k` closest nodes, the same request ID.

The two response cases are mutually exclusive:

**DHT-7** — A `FIND_PROVIDERS_RESULT` MUST contain either a non-empty `Providers` list OR a `Nodes` list — never both non-empty simultaneously. If `Providers` is non-empty, the receiver MUST treat the response as a found result and ignore any `Nodes` field. If `Providers` is absent or empty, the receiver MUST treat `Nodes` as the closest-peers response. A response where both are non-empty is a protocol error; the receiver SHOULD log it and treat it as a not-found result.

### STORE

Ask a peer to register the sender as a provider for a key.

- Request carries: a key (lowercase hex-encoded), an optional value (base64-encoded), a request ID, and the sender's declared listening address.
- Response carries: a boolean confirmation (`OK`), the same request ID.

The `Address` field carries the sender's declared listening address — not the source port of the current TCP connection, which is ephemeral. Receivers MUST use this address when storing the provider record, so that the record remains dialable by other nodes who receive it in future `FIND_PROVIDERS` responses.

### Request Correlation

Every request carries a **request ID** — a UUID v4 string in canonical hyphenated form (e.g. `"f47ac10b-58cc-4372-a567-0e02b2c3d479"`). The responder echoes it unchanged in the response.

**DHT-3** — Every request MUST carry a unique request ID in canonical hyphenated UUID v4 form. Responders MUST echo the request ID byte-for-byte unchanged. Requests that do not receive a matching response within the configured deadline MUST be abandoned.

**Usage patterns.** The three operations compose into the single-holder and multi-holder patterns described above:

- **File or content location (multi-holder)**: each holder calls `Store(content_hash, nil)` — the address is implicit. `FindProviders(content_hash)` returns all holders. The consumer dials any provider's `Address` directly.
- **Service registry (single-holder)**: one node calls `Store(service_key, service_url)`. `FindProviders(service_key)` returns one record. The consumer reads `providers[0].Value` for the URL and `providers[0].Address` for the node to contact.
- **Peer routing**: `Store(nodeID, nil)` registers this node's current address. `FindProviders(nodeID)` returns where to dial it.


## Iterative Lookup

A single node's routing table covers only a small slice of the network. To find the nodes closest to a given key, you ask the nodes you know, then ask the nodes they tell you about, converging progressively toward the target. This is the **iterative lookup**.

The `α` (alpha) parameter controls concurrency: at each step, `α` nodes are queried in parallel. Higher `α` converges faster at the cost of more simultaneous network requests. The default is 3.

### FIND_NODE Lookup (Closest-Node Search)

```
Goal: find the k nodes closest to a target key.

1. Seed the candidate set with the α locally-known nodes closest to the target.
   If the routing table is empty, return an empty result.

2. Select the next α candidates that have not yet been queried.
   If none remain, terminate.

3. Send FIND_NODE concurrently to all α selected candidates.
   Mark them as queried.

4. For each response:
   a. Add any newly reported nodes to the candidate set.
   b. Update the routing table with each reported node.

5. Re-sort the candidate set by XOR distance to the target.
   Retain only the k closest candidates.

6. If the round produced no new (unseen) candidates, terminate.
   The network has converged — no closer nodes exist.

7. Otherwise, go to step 2.

Return the k closest nodes found.
```

### FIND_PROVIDERS Lookup (Provider Search)

The FIND_PROVIDERS lookup runs the same convergent algorithm as FIND_NODE, with one difference: instead of discarding responses, it accumulates provider records from every responsive node along the way.

```
Same algorithm as FIND_NODE, with:
  - FIND_PROVIDERS sent instead of FIND_NODE.
  - Provider records collected from every response (not just routing candidates).
  - Termination by convergence — when no new routing candidates are found.
  - All collected provider records are deduplicated by NodeID and returned.
```

Unlike the old single-value lookup, FIND_PROVIDERS does **not** stop at the first result. The entire converged neighborhood is queried because multiple nodes may be providers — returning only the first one found would silently hide the others. A torrent-like network where ten nodes hold the same content needs all ten addresses, not just the first one that responds.

"Not found" (empty provider list) is not an error. It is a valid network state: no node has announced for this key, or every provider has gone offline or stopped republishing. Callers must handle both outcomes.


## Store Procedure

Storing a value does not write to a single node. It finds the `k` nodes closest to the key and replicates the value to all of them. This redundancy ensures the value remains retrievable even if some of those nodes go offline.

```
1. Derive the DHT key: SHA-256(input_key).

2. Register this node as a provider locally immediately.
   The local record is available for FIND_PROVIDERS queries even with an empty routing table.

3. Run an iterative FIND_NODE lookup to find the k closest nodes to the key.

4. Send STORE to all k nodes (excluding self) concurrently, including this
   node's declared listening address in the message payload. Wait for all
   responses before returning — worst case is one RequestTimeout, not k×RequestTimeout.

5. Log any STORE failures and continue.
   The local record ensures this node is findable for this key until the record expires.
```

The operation always succeeds locally — step 2 is unconditional. Network replication is best-effort: individual `STORE` RPCs can fail if a peer is temporarily unreachable. The operation returns a result that tells the caller exactly what happened: how many nodes were attempted and how many accepted. `Attempted == 0` means the routing table is empty — the record is stored locally but no other nodes exist yet, which is expected when the network is starting up or this is the first node to announce for the key. `Replicated < Attempted` means some peers were unreachable at the time; the record is still available locally. A caller that needs stronger availability guarantees must re-announce periodically or retry after detecting partial failure — the store procedure itself does not retry.

**Record TTL.** Every stored provider record carries an implicit expiry. Records that are not re-announced within the configured TTL (default 24h) are silently dropped — callers of `FIND_PROVIDERS` will no longer see them. Re-announcing before expiry resets the clock. This is the mechanism by which stale providers (nodes that have gone offline) are eventually forgotten without explicit removal.

**DHT-4** — A node receiving a `STORE` request MUST persist the key-value pair and respond with `STORE_ACK { OK: true }`.

**DHT-5** — A node MUST store the value locally before attempting replication. The local store is the fallback that prevents data loss if replication fails or the routing table is empty.


## Seeding the Routing Table

An empty routing table cannot perform any lookups. When a node first joins the network, it should populate its routing table before attempting DHT operations.

**DHT-6** — A node MAY be seeded with known peers without performing a liveness probe. Seeded peers are inserted directly into the routing table. When a bucket is full, the least-recently-seen entry is unconditionally evicted to make room — the normal liveness-check replacement policy does not apply to seeding.

**DHT-8** — The routing table MUST only be seeded with peers whose capability set includes `dht/1.0` or whose capability set is unknown (nil). Peers with a known protocol list that does not include `dht/1.0` MUST NOT be inserted. If a peer's capabilities become known after insertion (via `OnPeerCapabilitiesKnown`) and do not include `dht/1.0`, the peer MUST be removed from the routing table.

The idiomatic way to seed the routing table is from the `OnPeerConnected` callback: when the node layer establishes a connection to a new peer, check its protocol list (via `PeerProtocols`) and insert into the routing table only if DHT-capable. Use `ConnectionInfo.DeclaredAddr` as the routing table address — it is populated from the peer's `ANNOUNCE` message and is always dialable. Skip peers where `DeclaredAddr` is empty; their address will be recorded once their `ANNOUNCE` is received.


## Known Limitations

Every distributed system makes tradeoffs between security, simplicity, and performance. The limitations described here are not oversights — they are conscious tradeoffs that any open-membership P2P system must make. Understanding WHY each limitation exists is more useful than a list of bugs, because it tells you what would be required to close each one and what you give up in exchange.

### Eclipse and Sybil Attacks

**Trusted mode** is fully exposed. Node IDs are free UUID strings. An adversary generates IDs near a target key, injects them via FIND_NODE responses, and controls all lookups for that region. No defense exists at this layer in trusted mode — it is appropriate only for operator-controlled networks.

**Verified mode** closes two attack vectors at the DHT layer:

*Routing table injection.* Every `FIND_NODE` response entry carries the responder's public key. This node verifies `SHA-256(public_key) == NodeID` before inserting the entry. Entries that fail are dropped. An adversary controlling one node in the routing table can only advertise nodes it actually operates — fabricated NodeIDs cannot be injected.

*Bootstrap poisoning.* `ANNOUNCE` messages in verified mode carry an Ed25519 signature. Bootstrap nodes reject ANNOUNCEs where the signature does not prove ownership of the claimed NodeID. The bootstrap directory contains only cryptographically-verified peers.

**What remains open in verified mode.**

Sybil via real nodes is still possible. An adversary who generates many valid keypairs and runs actual nodes gets random positions in the key space. With enough nodes they can statistically dominate a region. The cost is infrastructure, not cryptography — raising the bar significantly but not eliminating the threat. This is the Sybil tradeoff inherent to open-membership design: closing it fully requires either a trust anchor (a certificate authority, proof-of-work, or staking) or closed membership. Open-membership networks accept this residual risk in exchange for permissionless participation.

Single-path lookups leave one remaining gap: an adversary with legitimate-but-malicious nodes already in the routing table can steer lookups by controlling one path. S/Kademlia disjoint-path lookups (Baumgart & Mies, 2007) are required for full eclipse resistance. Without them, routing manipulation by a well-resourced adversary remains possible. This is the next planned addition.

In verified mode this DHT provides strong protection against fabrication attacks and is appropriate for networks where participants are not fully trusted. It is not yet fully eclipse-resistant against a determined adversary with significant real infrastructure.

### Address Limitation: Routing Table Entries from Inbound Connections

When a peer connects TO this node (an inbound TCP connection), the address observed by the transport layer is the peer's ephemeral source port — not their declared listening address. When node A connects to node B, A's TCP stack picks an ephemeral source port (e.g., 54321) for that connection only. If B later tries to dial A:54321, nothing is listening there. A's actual listening address is what A declared in its `ANNOUNCE` message (e.g., A:9000).

The node layer resolves this by storing each peer's declared address from the `ANNOUNCE`-derived `peer-found` event. `ConnectionInfo` returns this declared address as `DeclaredAddr`, which is what `refreshSender`, `seedDHT`, and the `STORE` fallback use. A peer whose `ANNOUNCE` has not yet been received has an empty `DeclaredAddr` — those peers are skipped rather than inserted with an undialable ephemeral address.

**Residual gap.** A peer that connects inbound before its `ANNOUNCE` has propagated to this node will have an empty `DeclaredAddr` and will not appear in the routing table until a `peer-found` event arrives. In practice this window is short — discovery and TCP connection setup happen concurrently — but it is not zero.


## Wire Message Reference

All messages are `APPLICATION` frames with protocol identifier `dht/1.0`. Keys are lowercase hex-encoded 32-byte values (64-character strings, e.g. `"0a1b2c..."`). Implementations MUST produce and accept lowercase hex only — `0a` not `0A`. Values are standard base64-encoded bytes.

| Message | Type string | Fields |
|---|---|---|
| Find node request | `FIND_NODE` | `RequestID` (UUID v4, hyphenated), `Key` (lowercase hex) |
| Find node response | `FIND_NODE_RESULT` | `RequestID`, `Nodes[]` |
| Find providers request | `FIND_PROVIDERS` | `RequestID` (UUID v4, hyphenated), `Key` (lowercase hex) |
| Find providers response | `FIND_PROVIDERS_RESULT` | `RequestID`, `Providers[]` (if found) OR `Nodes[]` (if not found) |
| Store request | `STORE` | `RequestID` (UUID v4, hyphenated), `Key` (lowercase hex), `Value` (base64), `Address` (host:port) |
| Store acknowledgement | `STORE_ACK` | `RequestID`, `OK` (bool) |

Each `Nodes[]` entry carries: `NodeID` (string), `Address` (host:port string), `DHTKey` (lowercase hex-encoded 32 bytes).

Each `Providers[]` entry carries: `NodeID` (string), `Address` (host:port string), `Value` (base64-encoded bytes, may be empty string for nil value).

In `FIND_PROVIDERS_RESULT`, the presence of a non-empty `Providers` list distinguishes the two cases (DHT-7). The `Address` field in `STORE` is the sender's declared listening address — not the ephemeral source port of the connection. Receivers MUST store this address in the provider record so future consumers can dial the provider.


## Configuration Reference

| Parameter | Default | Description |
|---|---|---|
| `k` (bucket size) | 8 | Maximum entries per k-bucket. For small networks (under 20 nodes), 4 is sufficient. For large networks (thousands of nodes), 20 is the standard — it dramatically improves resilience to churn at a moderate memory cost. |
| `α` (alpha) | 3 | Concurrent RPCs per lookup round. Increase to 5 for faster convergence on large networks. Going higher adds load without proportional improvement. |
| Request timeout | 10s | How long to wait for a single RPC response. Tune to your network's 95th-percentile round-trip time plus a margin. LAN networks can use 500ms; global networks may need 30s. A timeout that fires before most responses arrive forces every lookup to run to candidate exhaustion, which is slow and network-intensive. |
| Record TTL | 24h | How long a provider record remains live without re-announcement. Re-announcing resets the clock. A provider that stops republishing is silently forgotten after this window. High-churn networks should use a shorter TTL and re-announce more frequently. |
| Store cleanup interval | 10m | How often the background cleaner evicts expired records. `lookupLocal` also filters on every read, so stale data is never returned between cleanup runs — the interval only affects memory reclaim timing. |


## Requirements Summary

| ID | Level | Requirement |
|---|---|---|
| DHT-1 | MUST / SHOULD | Update routing table on every inbound message; refresh last-seen on responses |
| DHT-2 | MUST NOT | Populate routing table with ephemeral source addresses from inbound connections |
| DHT-3 | MUST | Every request carries a unique UUID v4 request ID in canonical hyphenated form; responders echo it byte-for-byte; abandon on timeout |
| DHT-4 | MUST | On receiving STORE, persist the key-value pair and reply STORE_ACK { OK: true } |
| DHT-5 | MUST | Store locally before attempting replication. The operation returns replication counts (attempted, accepted); retry and re-announce policy is the caller's responsibility |
| DHT-6 | MAY | Seed routing table without liveness probe; evict LRU unconditionally when bucket is full |
| DHT-7 | MUST / SHOULD | FIND_PROVIDERS_RESULT contains Providers OR Nodes, never both non-empty; treat both-non-empty as not-found |
| DHT-8 | MUST | Only seed routing table with peers that include dht/1.0 in their protocol list or have unknown (nil) capabilities; remove peers when capabilities become known and exclude dht/1.0 |
| DHT-9 | MUST | Provider records MUST expire after the configured TTL. Expired records MUST NOT be returned by FIND_PROVIDERS. Re-announcing MUST reset the TTL. |
