# gossip demo

An epidemic broadcast network built on the **note** framework. Each node publishes one message. The gossip protocol ensures every node eventually receives every message — with no central coordinator, no global broadcaster, and no guaranteed delivery order.

This is a demo and a library stress test. The things it deliberately does that the other demos don't:

- Every node is both a publisher and a relay simultaneously
- The library provides no flood control: deduplication lives entirely in user space
- `Broadcast()` cannot be used — gossip must skip the sender, so the demo uses `p.Peers()` + `p.Send()` directly
- Multiple nodes publish at the same time, testing the message pipeline under concurrent load

---

## How it works

```
Node A publishes "hello"
  → sends to peers B, C, D

Node B receives "hello" (hops=0, new)
  → fires onReceive callback
  → forwards to C, D (skipping A)

Node C receives "hello" from A (hops=0) and from B (hops=1)
  → first arrival fires callback and is forwarded
  → second arrival is a duplicate — silently dropped
```

Every message carries an `ID` (UUID) used for deduplication, an `OriginID` identifying who created it, and a `Hops` counter incremented by each relay. A node that has already processed a message ID drops any further copies without forwarding, which prevents infinite loops.

The library does not manage any of this. The handler maintains a `seen map[string]struct{}` and the relay logic is a `for _, id := range n.Peers()` loop with one `if id == senderID { continue }` check. This is the deliberate design: the library provides the messaging primitives; flood control is an application concern.

---

## Protocol reference

| Sub-protocol | Message  | Direction   | Description |
|---|---|---|---|
| `gossip/1.0` | `GOSSIP` | Any → peers | Epidemic broadcast. Receivers forward to all peers except the sender. |

**GOSSIP wire format**

```json
{
  "id":        "uuid-of-this-message",
  "origin_id": "node-id-of-originator",
  "text":      "the message text",
  "hops":      2
}
```

---

## Quick start — local network

```bash
git clone git@github.com:m-sossich/note.git
cd note/demo/gossip
make build
```

**Terminal 1 — bootstrap**

```bash
./gossip --mode bootstrap --addr 127.0.0.1:6000
```

**Terminal 2**

```bash
./gossip --bootstrap 127.0.0.1:6000 --message "hello from alice"
```

**Terminal 3**

```bash
./gossip --bootstrap 127.0.0.1:6000 --message "hello from bob"
```

**Terminal 4**

```bash
./gossip --bootstrap 127.0.0.1:6000 --message "hello from carol"
```

Each terminal will print its own published message and every message it receives:

```
[gossip] node started  node_id=a1b2c3...  addr=0.0.0.0:6000
[gossip] 2 peer(s) connected — publishing in 1s
[published] "hello from alice"  id=f3a8b1c2
[received]  "hello from bob"    origin=d4e5f6  hops=0
[received]  "hello from carol"  origin=g7h8i9  hops=1
```

`hops=0` means the message arrived directly from the originator. `hops=1` means it passed through one relay node first — alice wasn't yet directly connected to carol when carol published, so bob relayed it.

---

## What the demo reveals about the library

**`Broadcast()` is too coarse for gossip.** It sends to every connected peer including the one who just sent you the message. Gossip must exclude the sender. The demo uses `n.Peers()` + `n.Send()` to iterate and skip, which works — but it means every gossip forward is O(peers) individual send calls rather than one broadcast call.

**Deduplication is caller responsibility.** If you remove the seen-message check from the handler, messages loop forever. The library has no built-in message ID tracking. This is correct design — the library cannot know what constitutes a "duplicate" for your protocol — but it means every flood-style protocol must implement its own loop prevention.

**Message ordering is not guaranteed.** If alice and bob publish simultaneously and carol is connected to both, carol may receive alice's message before bob's or vice versa, depending on goroutine scheduling. The gossip demo makes no ordering promises and the library provides none.

**Partial connectivity at publish time produces multi-hop delivery.** If carol is not yet directly connected to alice when alice publishes, alice's message reaches carol via a relay (hops > 0). This is correct gossip behavior but means early publishers reach a smaller initial audience. In the E2E test we wait for a full mesh before publishing to guarantee all messages arrive in one hop.

---

## Flags reference

| Flag | Default | Description |
|---|---|---|
| `--mode` | `node` | `bootstrap` or `node` |
| `--addr` | `0.0.0.0:6000` | UDP discovery + TCP listen address |
| `--bootstrap` | — | Bootstrap node address |
| `--message` | — | Message to publish after connecting (required in node mode) |
| `--advertise-addr` | — | Public IP:port for NAT |
| `--trusted` | false | Disable verified mode (no TLS, no Ed25519) |
| `--id` | `./<addr>.key` | Identity file path |
| `--log-level` | `info` | `debug`, `info`, `warn`, `error` |

---

## Make targets

| Target | Description |
|---|---|
| `make build` | Compile the `gossip` binary |
| `make clean` | Remove binary and generated identity files |
| `make test-e2e` | Run `TestGossip_E2E` — 1 bootstrap + 5 nodes, convergence + deduplication |
| `make vet` | Run `go vet ./...` |
| `make check` | `tidy` + `vet` |
| `make docker-build` | Build the `gossip` Docker image |
| `make docker-run` | Build and run — override with `PORT=` and `ARGS=` |
