# <p align="center"><img src="https://raw.githubusercontent.com/m-sossich/note/master/.github/logo.png" width="300"></p>

[![CI](https://github.com/m-sossich/note/actions/workflows/go.yml/badge.svg)](https://github.com/m-sossich/note/actions/workflows/go.yml)
[![codecov](https://codecov.io/gh/m-sossich/note/badge.svg)](https://codecov.io/gh/m-sossich/note)
[![Go Report Card](https://goreportcard.com/badge/github.com/m-sossich/note)](https://goreportcard.com/report/github.com/m-sossich/note)
[![Go Reference](https://pkg.go.dev/badge/github.com/m-sossich/gonphig/pkg/gonphig.svg)](https://pkg.go.dev/github.com/m-sossich/gonphig/pkg/gonphig)


<p align="center">
  A modular P2P framework for Go. Peer discovery, connection lifecycle, sub-protocol dispatch, and Kademlia DHT — each as an independent, composable package.
</p>

---

## Specifications

Language-agnostic protocol specifications in [`spec/`](spec/). 

| Spec | Description |
|---|---|
| [`spec/00-intro.md`](spec/00-intro.md) | How the layers fit together, security model, vocabulary |
| [`spec/01-wire.md`](spec/01-wire.md) | Transport contracts, framing, codec, envelope encoding |
| [`spec/02-node.md`](spec/02-node.md) | Connection lifecycle, handshake contract, sub-protocol dispatch |
| [`spec/03-discovery.md`](spec/03-discovery.md) | Bootstrap protocol, liveness probing, peer events |
| [`spec/04-dht.md`](spec/04-dht.md) | Kademlia routing, provider records, iterative lookup |

---


```bash
go get github.com/m-sossich/note
```

Requires Go 1.22+.

---

## Quick start

```go
import note "github.com/m-sossich/note"

p, err := note.NewPeer("0.0.0.0:9000",
    note.WithBootstrap("bootstrap.example.com:9000"),
    note.WithHandler("chat/1.0", chatHandler),
)
if err != nil {
    log.Fatal(err)
}
defer p.Close()
```

`NewPeer` generates and persists a node identity, registers handlers, starts discovery and the node layer, and returns a running peer. `Close` stops everything in the correct order.

---

## Handlers

Register handlers with `WithHandler` in the `NewPeer` call. Multiple protocols share the same connections.

```go
type ChatMessage struct {
    Author string
    Text   string
}

p, err := note.NewPeer("0.0.0.0:9000",
    note.WithBootstrap("bootstrap.example.com:9000"),
    note.WithHandler("chat/1.0", func(peerID, msgType string, decode func(any) error) error {
        switch msgType {
        case "TEXT":
            var msg ChatMessage
            if err := decode(&msg); err != nil {
                return err
            }
            fmt.Printf("[%s] %s: %s\n", peerID[:8], msg.Author, msg.Text)
        }
        return nil
    }),
    note.WithHandler("sync/1.0", syncHandler),
)
```

`decode` is a closure over the connection's codec — your handler never touches encoding directly. **Handler errors are non-fatal** — the message is dropped, the connection stays open.

---

## WithHandlerFactory

When a handler needs to send replies, it needs a reference to the `Peer`. `WithHandlerFactory` provides the constructed `node.Node` after all static handlers are registered but before the node starts listening, eliminating the forward-declaration workaround:

```go
var h *myprotocol.Handler

p, err := note.NewPeer(addr,
    note.WithHandlerFactory(func(n node.Node) {
        h = myprotocol.NewHandler(n, store)
    }),
)
```

`myprotocol.NewHandler` receives the node, registers its own handler via `n.Register`, and captures `n.Send` for outbound replies.

---

## Sending messages

```go
// Send to one peer.
p.Send(peerID, note.Msg("chat/1.0", "TEXT", ChatMessage{Author: "alice", Text: "hello"}))

// Or with a struct literal — named fields prevent transposing protocol and type.
p.Send(peerID, note.Message{
    Protocol: "chat/1.0",
    Type:     "TEXT",
    Payload:  ChatMessage{Author: "alice", Text: "hello"},
})

// Broadcast to all connected peers — returns count of successful sends.
sent := p.Broadcast(note.Msg("chat/1.0", "STATUS", status))
```

---

## Peer lifecycle hooks

```go
p, _ := note.NewPeer("0.0.0.0:9000",
    note.WithBootstrap("bootstrap.example.com:9000"),
    note.WithPeerConnected(func(peerID string) {
        slog.Info("peer joined", "id", peerID[:8])
    }),
    note.WithPeerDisconnected(func(peerID string) {
        slog.Info("peer left", "id", peerID[:8])
    }),
)
```

Both callbacks run in the connection goroutine. Dispatch slow work to a separate goroutine.

---

## DHT

Enable Kademlia with `WithDHT()`. Routing table seeding is automatic.

```go
p, err := note.NewPeer("0.0.0.0:9000",
    note.WithBootstrap("bootstrap.example.com:9000"),
    note.WithDHT(),
)

ctx := context.Background()

// Announce this node as a provider for a key.
res, err := p.Announce(ctx, []byte("my-service"), []byte("grpc://10.0.0.1:9090"))

// Find all providers.
providers, _ := p.FindProviders(ctx, []byte("my-service"))
for _, pr := range providers {
    fmt.Println(pr.NodeID, pr.Address, string(pr.Value))
}
```

Single-holder and multi-holder (torrent-style) are both supported. `StoreResult.Attempted` / `StoreResult.Replicated` report replication coverage; `Attempted == 0` means no other nodes exist yet.

```go
// Multi-holder: each node announces under the content hash.
p.Announce(ctx, []byte("sha256:abc123..."), nil)
providers, _ := p.FindProviders(ctx, []byte("sha256:abc123..."))
```

For raw lookups: `p.Lookup(ctx, key)`. Custom tuning: `note.WithDHT(dht.Config{BucketSize: 20, Alpha: 5, RequestTimeout: 30 * time.Second})`.

See [spec/04-dht.md](spec/04-dht.md) for the full Kademlia protocol.

---

## Bootstrap node

```go
p, err := note.NewPeer("0.0.0.0:9000",
    note.WithBootstrap("other-bootstrap.example.com:9000"), // optional: mesh of bootstraps
)
if err != nil {
    log.Fatal(err)
}
defer p.Close()
select {} // run until interrupted
```

A bootstrap node registers no protocols — peers skip dialing it for TCP. Use `p.Addr()` for the actual bound address. Set `WithAdvertiseAddr("203.0.113.42:9000")` behind NAT.

---

## Verified mode

Two-line change from trusted mode. Everything else — handlers, DHT, discovery — works identically.

```go
import (
    note     "github.com/m-sossich/note"
    "github.com/m-sossich/note/pkg/identity"
)

kp, _ := identity.LoadOrGenerate("./node.key")

p, _ := note.NewVerifiedPeer(kp, "0.0.0.0:9000",
    note.WithBootstrap("bootstrap.example.com:9000"),
    note.WithDHT(),
)
defer p.Close()
```

`p.ID()` returns `SHA-256(kp.PublicKey)`.

**What verified mode provides:**
- All TCP connections encrypted and integrity-protected (mTLS)
- Both sides prove they own their node identity
- Bootstrap peer tables reject fake identities — ANNOUNCE messages are signed
- DHT routing table injection closed — FIND_NODE responses carry public keys; fabricated entries are rejected

**What it does not fully close:**
- Discovery traffic (UDP) is plaintext
- Sybil via many real nodes — the cost is infrastructure, not cryptography
- Single-path DHT lookups — S/Kademlia disjoint paths are planned

---

## Options reference

| Option | Description |
|---|---|
| `WithBootstrap(addrs...)` | Addresses of well-known nodes to contact on startup |
| `WithHandler(protocol, handler)` | Register a sub-protocol handler |
| `WithHandlerFactory(fn)` | Register a factory called with the constructed node after static handlers, before the node starts |
| `WithDHT(cfg ...dht.Config)` | Enable Kademlia DHT |
| `WithAdvertiseAddr(addr)` | Externally-routable address for NAT/Docker |
| `WithMaxPeers(n)` | Total established peer budget (default 128) |
| `WithMaxInboundPeers(n)` | Inbound sub-limit within MaxPeers; reserves slots for outbound dials (default: MaxPeers × 2/3) |
| `WithMaxPendingPeers(n)` | Concurrent in-progress handshakes cap (default 32) |
| `WithPingInterval(d)` | How often to probe peer liveness (default 1s) |
| `WithPingMaxMissed(n)` | Consecutive unanswered pings before eviction (default 3) |
| `WithHandshakeTimeout(d)` | Deadline for completing a connection handshake (default 10s) |
| `WithMaxFrameSize(bytes)` | Global transport ceiling for max payload per message (default 64 KiB) |
| `WithProtocolFrameSize(protocol, bytes)` | Per-protocol frame size cap enforced at dispatch time |
| `WithCodec(c)` | Network codec — all peers must match (default JSON) |
| `WithIdentityPath(path)` | Custom path for the trusted-mode UUID file |
| `WithPeerConnected(fn)` | Called when a peer reaches CONNECTED |
| `WithPeerDisconnected(fn)` | Called when a peer connection ends |

---

## Rate limiting

The library applies no rate limiting — it cannot know what a legitimate message rate looks like for your protocol. Rate limiting is a wrapper on `note.Handler`:

```go
import "golang.org/x/time/rate"

func withPerPeerRateLimit(h note.Handler, r rate.Limit, burst int) note.Handler {
    var mu sync.Mutex
    limiters := make(map[string]*rate.Limiter)
    return func(peerID, msgType string, decode func(any) error) error {
        mu.Lock()
        l, ok := limiters[peerID]
        if !ok {
            l = rate.NewLimiter(r, burst)
            limiters[peerID] = l
        }
        mu.Unlock()
        if !l.Allow() {
            return fmt.Errorf("rate limit exceeded")
        }
        return h(peerID, msgType, decode)
    }
}

p, _ := note.NewPeer("0.0.0.0:9000",
    note.WithBootstrap("bootstrap.example.com:9000"),
    note.WithHandler("chat/1.0", withPerPeerRateLimit(chatHandler, 10, 20)),
    note.WithHandler("dht/1.0",  dhtHandler),
)
defer p.Close()
```

---

## Request/response protocols

`Send` and `Register` are fire-and-forget. Use `pkg/p2p.PendingMap[T]` to correlate responses:

```go
import "github.com/m-sossich/note/pkg/p2p"

type Handler struct {
    n       node.Node
    pending p2p.PendingMap[*Response]
}

func NewHandler(n node.Node) *Handler {
    h := &Handler{n: n, pending: *p2p.NewPendingMap[*Response]()}
    n.Register("myproto/1.0", h.handle)
    return h
}

func (h *Handler) Request(ctx context.Context, peerID, reqID string) (*Response, error) {
    return h.pending.Wait(ctx, reqID, func() error {
        _, err := h.n.Send(peerID, "myproto/1.0", "REQUEST", Req{ID: reqID})
        return err
    })
}

func (h *Handler) handle(peerID, msgType string, decode func(any) error) error {
    var resp Response
    if err := decode(&resp); err != nil {
        return err
    }
    h.pending.Deliver(resp.ID, &resp)
    return nil
}
```

---

## Advanced: low-level API

`note.NewPeer` covers most cases. When you need a custom codec, transport, or handshaker, use the packages directly.

```go
import (
    jsoncdc      "github.com/m-sossich/note/pkg/codec/json"
    "github.com/m-sossich/note/pkg/discovery"
    "github.com/m-sossich/note/pkg/node"
    "github.com/m-sossich/note/pkg/node/identify"
    tcptransport "github.com/m-sossich/note/pkg/transport/tcp"
    "github.com/m-sossich/note/pkg/transport/udp"
)

nodeID, _ := note.LoadOrGenerateID("./node.id")
codec      := jsoncdc.New()

udpTr, _ := udp.New("0.0.0.0:9000")
disc, _   := discovery.New(discovery.Config{
    NodeID:         nodeID,
    BindAddr:       "0.0.0.0:9000",
    BootstrapAddrs: []string{"bootstrap.example.com:9000"},
    Codec:          codec,
}, udpTr)

n, _ := node.New(node.Config{
    NodeID:     nodeID,
    ListenAddr: "0.0.0.0:9000",
    Transport:  tcptransport.New(0),
    Codec:      codec,
    Handshaker: identify.New(identify.Config{}),
}, disc)

n.Register("chat/1.0", chatHandler)
n.Start()
disc.Start(n.RegisteredProtocols())
defer disc.Stop()
defer n.Stop()
```

### Custom codec

```go
note.NewPeer("0.0.0.0:9000", note.WithCodec(&MsgpackCodec{}))

type MsgpackCodec struct{}
func (c *MsgpackCodec) ID() string                    { return "msgpack" }
func (c *MsgpackCodec) Encode(v any) ([]byte, error)  { return msgpack.Marshal(v) }
func (c *MsgpackCodec) Decode(b []byte, v any) error  { return msgpack.Unmarshal(b, v) }
```

### Custom transport

```go
type QuicTransport struct{}
func (t *QuicTransport) Dial(addr string) (transport.Conn, error)       { /* ... */ }
func (t *QuicTransport) Listen(addr string) (transport.Listener, error) { /* ... */ }
func (t *QuicTransport) Close() error                                   { /* ... */ }
```

### Custom handshaker

```go
type AuthHandshaker struct{ token string }

func (h *AuthHandshaker) Initiate(conn transport.Conn, cfg p2p.HandshakeConfig) (p2p.HandshakeResult, error) {
    // send an IDENT frame with token; return HandshakeResult{PeerID: cfg.PeerID}
}
func (h *AuthHandshaker) Accept(conn transport.Conn, cfg p2p.HandshakeConfig) (p2p.HandshakeResult, error) {
    // receive IDENT frame, verify token; return HandshakeResult{PeerID: extractedID}
}
```

### Max frame size

Default **64 KiB**. Raise the global ceiling for large payloads; add per-protocol caps to limit attack surface:

```go
note.NewPeer("0.0.0.0:9000",
    note.WithMaxFrameSize(4 * 1024 * 1024),         // global ceiling: 4 MiB for file chunks
    note.WithProtocolFrameSize("dht/1.0", 64*1024), // DHT messages capped at 64 KiB
)
```

### Peer limits

```go
note.NewPeer("0.0.0.0:9000",
    note.WithMaxPeers(50),        // total peer budget (inbound + outbound)
    note.WithMaxInboundPeers(30), // reserves 20 slots for outbound dials
    note.WithMaxPendingPeers(16), // concurrent handshakes
)
```

Defaults: 128 total / MaxPeers×2/3 inbound / 32 pending.

---

## Demos

| Demo | What it shows | README |
|---|---|---|
| `demo/chat` | Distributed chat with DHT room membership, mTLS, `WithHandlerFactory` | [chat/README.md](demo/chat/README.md) |
| `demo/cas` | IPFS-like content-addressed storage: SHA-256 CIDs, chunked parallel multi-provider fetch, deduplication | [cas/README.md](demo/cas/README.md) |
| `demo/gossip` | Epidemic broadcast; user-space deduplication, relay-skipping with `Peers()` + `Send()` | [gossip/README.md](demo/gossip/README.md) |
| `demo/fileshare` | DHT-based file discovery and direct transfer | [fileshare/README.md](demo/fileshare/README.md) |

---

## Testing

```bash
go test ./...
```

Targeted subsets:

```bash
go test ./pkg/codec/... ./pkg/dht/... ./pkg/wire/...  # pure logic, no network
go test ./pkg/discovery/... ./pkg/node/... -timeout 30s  # real UDP/TCP sockets
go test ./demo/... -timeout 60s  # multi-node end-to-end scenarios
```
