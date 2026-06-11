# cas demo

A content-addressed storage network modelled after IPFS. Files are split into fixed-size chunks (256 KiB), each identified by its SHA-256 hash (the CID). A manifest block ties the chunk CIDs together; sharing a file means sharing only the root CID. Retrieval is parallel: each chunk is fetched concurrently from whichever node the DHT says holds it. Identical files produce identical CIDs, so two nodes adding the same file never store it twice — the DHT accumulates multiple providers per CID automatically. This demo puts DHT multi-holder records, `PendingMap`-based request/response, and frame-size tuning all in one place.

---

## Quick start

```bash
git clone git@github.com:m-sossich/note.git
cd note/demo/cas
make build
```

**Terminal 1 — bootstrap**

```bash
./cas --mode bootstrap --addr 127.0.0.1:9000
```

**Terminal 2 — add a file**

```bash
dd if=/dev/urandom of=/tmp/testfile.bin bs=1024 count=768

./cas \
  --mode add \
  --addr 127.0.0.1:9001 \
  --bootstrap 127.0.0.1:9000 \
  --file /tmp/testfile.bin
```

Copy the printed root CID.

**Terminal 3 — get the file**

```bash
./cas \
  --mode get \
  --addr 127.0.0.1:9003 \
  --bootstrap 127.0.0.1:9000 \
  --cid <root-cid> \
  --out /tmp/received.bin

diff /tmp/testfile.bin /tmp/received.bin && echo "identical"
```

---

## What you'll see

The adder prints the root CID and waits for block requests:

```
file chunked: filename=testfile.bin bytes=786432 chunks=3
CIDs announced in DHT

root CID: e3f4a5b6c7d8e9f0...

[add] serving blocks — keep this running so others can fetch
```

The getter fetches chunks in parallel — arrival order is non-deterministic:

```
manifest fetched from=a1b2c3d4
chunk fetched index=1/3 cid=... from=a1b2c3d4 bytes=262144
chunk fetched index=3/3 cid=... from=f0e1d2c3 bytes=262144   ← different peer
chunk fetched index=2/3 cid=... from=a1b2c3d4 bytes=262144
[get] saved "testfile.bin" → /tmp/received.bin (786432 bytes)
```

Chunk 3 before chunk 2 is the parallel fetch in action.

---

## Flags reference

| Flag | Default | Description |
|---|---|---|
| `--mode` | — | `bootstrap`, `add`, or `get` (required) |
| `--addr` | `127.0.0.1:9000` | UDP discovery + TCP listen address |
| `--bootstrap` | — | Bootstrap node address. Comma-separated for multiple. |
| `--file` | — | `[add]` path to the file to share |
| `--cid` | — | `[get]` root CID to fetch |
| `--out` | `./received` | `[get]` output file path |
| `--id` | `./<addr>.key` | Ed25519 identity file (auto-generated, persisted across restarts) |
| `--log-level` | `info` | `debug`, `info`, `warn`, `error` |

---

## Protocol details

See [spec/04-dht.md](../../spec/04-dht.md) for multi-holder provider records and [spec/02-node.md](../../spec/02-node.md) for sub-protocol dispatch. The `cas/1.0` sub-protocol uses `WANT_BLOCK` / `BLOCK` / `NOT_FOUND` messages; the max frame size is raised to 4 MiB to accommodate base64-encoded 256 KiB chunks.

---

## E2E test

```bash
make test-e2e
```

Runs `TestCAS_E2E` — a 5-phase pipeline: peer discovery, upload from two independent nodes with the same content, DHT routing assertion, parallel multi-provider fetch, and deduplication verification.

---

## Make targets

| Target | Description |
|---|---|
| `make build` | Compile the `cas` binary |
| `make clean` | Remove binary and generated `.key` files |
| `make test-e2e` | Run `TestCAS_E2E` |
| `make vet` | Run `go vet ./...` |
| `make check` | `tidy` + `vet` |
| `make docker-build` | Build the `cas` Docker image |
| `make docker-run` | Build and run — override with `PORT=` and `ARGS=` |
