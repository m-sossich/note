// demo/cas demonstrates a content-addressed distributed storage network built
// on the note framework. It is inspired by IPFS but makes no claim of protocol
// compatibility. Files are split into fixed-size chunks, each identified by its
// SHA-256 hash (CID). The DHT routes requests to whichever peers hold each
// chunk. A getter downloads all chunks in parallel — potentially from different
// providers — then reassembles the file.
//
// Modes:
//
//	bootstrap  — run a discovery bootstrap node
//	add        — chunk a file, announce all CIDs, serve blocks indefinitely
//	get        — resolve a root CID, fetch all chunks in parallel, write file
//
// Usage:
//
//	# Start bootstrap (Terminal 1)
//	go run ./demo/cas --mode bootstrap --addr 127.0.0.1:9000
//
//	# Share a file (Terminal 2) — prints root CID and stays running
//	go run ./demo/cas --mode add --addr 127.0.0.1:9001 \
//	    --bootstrap 127.0.0.1:9000 --file /tmp/myfile.bin
//
//	# Fetch the file (Terminal 3)
//	go run ./demo/cas --mode get --addr 127.0.0.1:9002 \
//	    --bootstrap 127.0.0.1:9000 --cid <root-cid> --out /tmp/received.bin
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	note "github.com/m-sossich/note"
	"github.com/m-sossich/note/demo/cas/blockstore"
	"github.com/m-sossich/note/demo/cas/chunker"
	"github.com/m-sossich/note/demo/cas/protocol"
	"github.com/m-sossich/note/pkg/identity"
	"github.com/m-sossich/note/pkg/node"
)

const (
	// maxFrameSize is the transport ceiling for cas/1.0 block transfers.
	// base64(256 KiB chunk) ≈ 341 KiB; 4 MiB leaves headroom for larger custom chunks.
	maxFrameSize    = 4 * 1024 * 1024
	fetchMaxRetries = 20
	fetchRetryDelay = 500 * time.Millisecond
)

func main() {
	mode := flag.String("mode", "", "node mode: bootstrap | add | get")
	addr := flag.String("addr", "127.0.0.1:9000", "UDP discovery + TCP listen address")
	bootstrapFlag := flag.String("bootstrap", "", "bootstrap node address(es), comma-separated")
	filePath := flag.String("file", "", "[add] local file to share")
	chunkSize := flag.Int("chunk-size", 0, "[add] chunk size in bytes (default: 256 KiB)")
	rootCID := flag.String("cid", "", "[get] root CID to fetch")
	outPath := flag.String("out", "./received", "[get] output file path")
	idPath := flag.String("id", "", "identity file path (auto-generated on first run)")
	logLevel := flag.String("log-level", "info", "debug | info | warn | error")
	flag.Parse()

	setupLogging(*logLevel)

	if *mode == "" {
		slog.Error("--mode is required: bootstrap | add | get")
		os.Exit(1)
	}

	var bootstrapAddrs []string
	if *bootstrapFlag != "" {
		bootstrapAddrs = strings.Split(*bootstrapFlag, ",")
	}

	switch *mode {
	case "bootstrap":
		runBootstrap(*addr, *idPath)
	case "add":
		runAdd(*addr, *idPath, bootstrapAddrs, *filePath, *chunkSize)
	case "get":
		runGet(*addr, *idPath, bootstrapAddrs, *rootCID, *outPath)
	default:
		slog.Error("unknown mode", "mode", *mode)
		os.Exit(1)
	}
}

// ---------------------------------------------------------------------------
// Modes
// ---------------------------------------------------------------------------

func runBootstrap(addr, idPath string) {
	kp := loadOrGenerateKey(idPath, addr)
	p, err := note.NewVerifiedPeer(kp, addr,
		note.WithDiscoveryMaxPeers(1000),
	)
	if err != nil {
		slog.Error("start bootstrap", "err", err)
		os.Exit(1)
	}
	defer p.Close()
	slog.Info("bootstrap ready", "node_id", p.ID(), "addr", p.Addr())
	fmt.Printf("[bootstrap] ready on %s — share this address with peers\n", p.Addr())
	select {}
}

func runAdd(addr, idPath string, bootstraps []string, filePath string, chunkSize int) {
	if filePath == "" {
		slog.Error("--file is required for add mode")
		os.Exit(1)
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		slog.Error("read file", "err", err)
		os.Exit(1)
	}

	store := blockstore.New()
	p, _ := startPeer(addr, idPath, bootstraps, store)
	defer p.Close()

	ctx := context.Background()
	rootCID, err := addToNetwork(ctx, p, store, filepath.Base(filePath), data, chunkSize)
	if err != nil {
		slog.Error("add to network", "err", err)
		os.Exit(1)
	}

	fmt.Printf("\nroot CID: %s\n\n", rootCID)
	fmt.Println("[add] serving blocks — keep this running so others can fetch")
	select {}
}

func runGet(addr, idPath string, bootstraps []string, rootCID, outPath string) {
	if rootCID == "" {
		slog.Error("--cid is required for get mode")
		os.Exit(1)
	}

	store := blockstore.New()
	p, h := startPeer(addr, idPath, bootstraps, store)
	defer p.Close()

	ctx := context.Background()
	assembled, filename, err := getFromNetwork(ctx, p, h, rootCID)
	if err != nil {
		slog.Error("get from network", "err", err)
		os.Exit(1)
	}

	if err := os.WriteFile(outPath, assembled, 0644); err != nil {
		slog.Error("write output", "err", err)
		os.Exit(1)
	}
	fmt.Printf("\n[get] saved %q → %s (%d bytes)\n", filename, outPath, len(assembled))
}

// ---------------------------------------------------------------------------
// Core operations — called by both the CLI modes and the E2E test
// ---------------------------------------------------------------------------

// addToNetwork chunks data into blocks, stores them locally, and announces
// each block CID in the DHT so other peers can find and fetch them.
// chunkSize <= 0 uses chunker.DefaultChunkSize (256 KiB).
// Returns the root CID (the manifest's CID).
func addToNetwork(ctx context.Context, p *note.Peer, store *blockstore.MemStore, filename string, data []byte, chunkSize int) (string, error) {
	manifest, chunks := chunker.Split(filename, data, chunkSize)

	store.Put(manifest.CID, manifest.Data)
	for _, b := range chunks {
		store.Put(b.CID, b.Data)
	}
	slog.Info("file chunked",
		"filename", filename,
		"bytes", len(data),
		"chunks", len(chunks),
		"root_cid", manifest.CID[:16]+"...",
	)

	for _, b := range chunks {
		res, err := p.Announce(ctx, []byte(b.CID), []byte(p.ID()))
		if err != nil {
			slog.Warn("announce chunk failed", "cid", b.CID[:8], "err", err)
		} else if res.Attempted > 0 && res.Replicated == 0 {
			slog.Warn("announce chunk: no peers replicated", "cid", b.CID[:8], "attempted", res.Attempted)
		}
	}
	res, err := p.Announce(ctx, []byte(manifest.CID), []byte(p.ID()))
	if err != nil {
		return "", fmt.Errorf("announce manifest: %w", err)
	}
	if res.Attempted > 0 && res.Replicated == 0 {
		slog.Warn("announce manifest: no peers replicated", "root_cid", manifest.CID[:16]+"...", "attempted", res.Attempted)
	}
	slog.Info("CIDs announced in DHT", "root_cid", manifest.CID[:16]+"...", "chunks", len(chunks), "replicated", res.Replicated)
	return manifest.CID, nil
}

// getFromNetwork resolves rootCID from the DHT, fetches all chunks in parallel
// (each potentially from a different provider), verifies each chunk's CID, and
// reassembles the file in manifest order.
// Returns the raw bytes, the original filename from the manifest, and any error.
func getFromNetwork(ctx context.Context, p *note.Peer, h *protocol.Handler, rootCID string) ([]byte, string, error) {
	slog.Info("resolving manifest", "cid", rootCID[:16]+"...")
	manifestData, fromPeer := resolveBlock(ctx, p, h, rootCID)
	if manifestData == nil {
		return nil, "", fmt.Errorf("manifest %s... not found in network", rootCID[:8])
	}
	slog.Info("manifest fetched", "from", fromPeer[:8])

	manifest, err := chunker.DecodeManifest(manifestData)
	if err != nil {
		return nil, "", fmt.Errorf("decode manifest: %w", err)
	}
	slog.Info("manifest decoded", "filename", manifest.Filename, "size", manifest.Size, "chunks", len(manifest.Chunks))

	type chunkResult struct {
		idx    int
		data   []byte
		fromID string
		err    error
	}
	resultCh := make(chan chunkResult, len(manifest.Chunks))

	for i, cid := range manifest.Chunks {
		go func(idx int, cid string) {
			data, from := resolveBlock(ctx, p, h, cid)
			if data == nil {
				resultCh <- chunkResult{idx: idx, err: fmt.Errorf("chunk %d (%s...) not found", idx+1, cid[:8])}
				return
			}
			if got := chunker.CID(data); got != cid {
				resultCh <- chunkResult{idx: idx, err: fmt.Errorf("chunk %d CID mismatch: got %s, want %s", idx+1, got[:8], cid[:8])}
				return
			}
			slog.Info("chunk fetched",
				"index", fmt.Sprintf("%d/%d", idx+1, len(manifest.Chunks)),
				"cid", cid[:8]+"...",
				"from", from[:8],
				"bytes", len(data),
			)
			resultCh <- chunkResult{idx: idx, data: data, fromID: from}
		}(i, cid)
	}

	chunks := make([][]byte, len(manifest.Chunks))
	for range manifest.Chunks {
		r := <-resultCh
		if r.err != nil {
			return nil, "", r.err
		}
		chunks[r.idx] = r.data
	}

	var assembled []byte
	for _, chunk := range chunks {
		assembled = append(assembled, chunk...)
	}
	return assembled, manifest.Filename, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// startPeer builds and starts a verified-mode peer with DHT and the cas/1.0
// handler. The handler must be reused for all FetchBlock calls so that incoming
// BLOCK responses are routed to the correct pending channels.
func startPeer(addr, idPath string, bootstraps []string, store *blockstore.MemStore) (*note.Peer, *protocol.Handler) {
	kp := loadOrGenerateKey(idPath, addr)

	var h *protocol.Handler

	p, err := note.NewVerifiedPeer(kp, addr,
		note.WithBootstrap(bootstraps...),
		note.WithDHT(),
		note.WithHandlerFactory(func(n node.Node) {
			h = protocol.NewHandler(n, store)
		}),
		// Raise transport ceiling for cas/1.0 block transfers; cap DHT at 64 KiB.
		note.WithMaxFrameSize(maxFrameSize),
		note.WithProtocolFrameSize("dht/1.0", 64*1024),
	)
	if err != nil {
		slog.Error("start peer", "err", err)
		os.Exit(1)
	}
	slog.Info("node started", "node_id", p.ID(), "addr", p.Addr())
	return p, h
}

// resolveBlock finds providers for cid via DHT and fetches the block from the
// first responsive provider. Returns (nil, "") after all retries are exhausted.
func resolveBlock(ctx context.Context, p *note.Peer, h *protocol.Handler, cid string) ([]byte, string) {
	for attempt := 1; attempt <= fetchMaxRetries; attempt++ {
		providers, err := p.FindProviders(ctx, []byte(cid))
		if err != nil || len(providers) == 0 {
			time.Sleep(fetchRetryDelay)
			continue
		}
		for _, provider := range providers {
			data, err := h.FetchBlock(ctx, provider.NodeID, cid)
			if err != nil {
				slog.Debug("block fetch attempt failed", "provider", provider.NodeID[:8], "err", err)
				continue
			}
			return data, provider.NodeID
		}
		time.Sleep(fetchRetryDelay)
	}
	return nil, ""
}

func loadOrGenerateKey(idPath, addr string) *identity.Keypair {
	if idPath == "" {
		idPath = "./" + sanitizeAddr(addr) + ".key"
	}
	kp, err := identity.LoadOrGenerate(idPath)
	if err != nil {
		slog.Error("load identity", "err", err)
		os.Exit(1)
	}
	return kp
}

func sanitizeAddr(addr string) string {
	return strings.NewReplacer(":", "_", ".", "_").Replace(addr)
}

func setupLogging(level string) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l})))
}
