# chat demo

A distributed chat application that models the most common P2P messaging pattern: room-based membership coordinated via DHT, direct peer-to-peer message delivery, and cryptographic identity. There is no central relay — every message goes directly from sender to recipient over a mTLS TCP connection. It demonstrates `WithHandlerFactory`, DHT room membership, and verified mode in one coherent application.

---

## Quick start

```bash
git clone git@github.com:m-sossich/note.git
cd note/demo/chat
make build
```

**Terminal 1 — bootstrap**

```bash
./chat --mode bootstrap --addr 127.0.0.1:8000
```

**Terminal 2 — Alice**

```bash
./chat --bootstrap 127.0.0.1:8000 --username alice --room general
```

**Terminal 3 — Bob**

```bash
./chat --addr 127.0.0.1:8002 --bootstrap 127.0.0.1:8000 --username bob --room general
```

Both terminals show join notifications. Type in either terminal and the message appears in the other.

```
--- bob joined the room
hello from alice
[alice] hello from alice
```

Commands: `/members` lists room members, `/quit` exits gracefully.

---

## What you'll see

Each node prints join/leave events as peers connect and disconnect. Messages are prefixed with the sender's username. The `/members` command queries the Kademlia DHT for all nodes registered under the room key — this works even for late-joining nodes that missed the initial `ANNOUNCE` exchange.

```
--- joining room #general as alice
--- node id: 3f2a91bc4d...
--- connected to 0 peer(s) — type a message and press Enter
--- /members to list room members, /quit to exit
--- bob joined the room
[bob] hello from alice
```

---

## Flags reference

| Flag | Default | Description |
|---|---|---|
| `--mode` | `join` | `bootstrap` or `join` |
| `--addr` | `0.0.0.0:8000` | UDP discovery + TCP listen address |
| `--bootstrap` | — | Bootstrap node address. Comma-separated for multiple. |
| `--room` | `general` | Chat room name |
| `--username` | — | Display name (required in join mode) |
| `--advertise-addr` | — | Public IP:port announced to peers. Required behind NAT. |
| `--trusted` | `false` | Disable verified mode (no TLS, no Ed25519). |
| `--id` | `./<addr>.key` | Path to the Ed25519 identity file. |
| `--log-level` | `info` | `debug`, `info`, `warn`, or `error` |

---

## Protocol details

See [spec/02-node.md](../../spec/02-node.md) for the connection and handshake model, [spec/03-discovery.md](../../spec/03-discovery.md) for the bootstrap and ANNOUNCE protocol, and [spec/04-dht.md](../../spec/04-dht.md) for DHT-based room membership.

---

## E2E test

```bash
make test-e2e
```

Runs `TestChat_E2E` — 1 bootstrap + 5 nodes, full discovery and messaging.

---

## Make targets

| Target | Description |
|---|---|
| `make build` | Compile the `chat` binary |
| `make clean` | Remove the binary and generated `.key` files |
| `make test-e2e` | Run `TestChat_E2E` |
| `make vet` | Run `go vet ./...` |
| `make check` | `tidy` + `vet` |
| `make docker-build` | Build the `chat` Docker image |
| `make docker-run` | Build and run — override with `PORT=` and `ARGS=` |
