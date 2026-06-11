package main

// TestCAS_E2E exercises the full upload → announce → parallel fetch → reassemble
// pipeline end-to-end. Nothing is injected directly into block stores or bypassed.
//
// Scenario:
//
//	nodeA calls addToNetwork("alpha.bin", alphaData) — the same code path as the CLI.
//	  → chunks the file, stores blocks locally, announces each CID in DHT.
//	nodeB calls addToNetwork("beta.bin", betaData) — different content, different CIDs.
//	  → same flow, different provider.
//	nodeC calls getFromNetwork(rootCIDAlpha) and getFromNetwork(rootCIDBeta) in parallel.
//	  → resolves manifests, fetches all chunks concurrently, verifies each CID, reassembles.
//	  → alpha chunks can only come from nodeA; beta chunks can only come from nodeB.
//
// Deduplication:
//	nodeA also calls addToNetwork("beta.bin", betaData) — identical content to nodeB.
//	  → same CIDs produced; DHT now has two providers for every beta chunk.
//	  → nodeC re-fetches beta and must succeed (works with either provider).
//
// Ports 19920–19923. chunkSize=1 KiB keeps the test files small.

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	note "github.com/m-sossich/note"
	"github.com/m-sossich/note/demo/cas/blockstore"
	"github.com/m-sossich/note/demo/cas/protocol"
	"github.com/m-sossich/note/pkg/identity"
	"github.com/m-sossich/note/pkg/node"
)

const (
	testChunkSize = 1024 // 1 KiB — keeps test files small and fast
	testFrameSize = 4 * 1024 * 1024
)

// casTestNode is a running CAS peer wired for tests.
// It uses the same startPeer / addToNetwork / getFromNetwork functions as the CLI.
type casTestNode struct {
	p     *note.Peer
	h     *protocol.Handler
	store *blockstore.MemStore
}

// newCASNode creates a verified-mode CAS node. Identity is ephemeral (generated,
// not persisted) — suitable for tests. t.Cleanup shuts the peer down.
func newCASNode(t *testing.T, addr, bootstrapAddr string) *casTestNode {
	t.Helper()

	kp, err := identity.Generate()
	if err != nil {
		t.Fatalf("newCASNode %s: generate keypair: %v", addr, err)
	}

	store := blockstore.New()
	cn := &casTestNode{store: store}

	var h *protocol.Handler

	opts := []note.Option{
		note.WithDHT(),
		note.WithHandlerFactory(func(n node.Node) {
			h = protocol.NewHandler(n, store)
		}),
		note.WithMaxFrameSize(testFrameSize),
	}
	if bootstrapAddr != "" {
		opts = append(opts, note.WithBootstrap(bootstrapAddr))
	}

	p, err := note.NewVerifiedPeer(kp, addr, opts...)
	if err != nil {
		t.Fatalf("newCASNode %s: start peer: %v", addr, err)
	}
	t.Cleanup(func() { p.Close() })

	cn.p = p
	cn.h = h
	return cn
}

func TestCAS_E2E(t *testing.T) {
	const (
		bootstrapAddr = "127.0.0.1:19920"
		addrA         = "127.0.0.1:19921"
		addrB         = "127.0.0.1:19922"
		addrC         = "127.0.0.1:19923"
	)

	// 3-chunk files — distinct byte patterns to catch reassembly and routing bugs.
	alphaData := make([]byte, 3*testChunkSize)
	for i := range alphaData {
		alphaData[i] = byte(i % 251)
	}
	betaData := make([]byte, 3*testChunkSize)
	for i := range betaData {
		betaData[i] = byte((i + 128) % 251)
	}

	// ---- bootstrap (discovery only) ----
	bsKP, err := identity.Generate()
	if err != nil {
		t.Fatalf("bootstrap keypair: %v", err)
	}
	bs, err := note.NewVerifiedPeer(bsKP, bootstrapAddr)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	t.Cleanup(func() { bs.Close() })

	nodeA := newCASNode(t, addrA, bootstrapAddr)
	nodeB := newCASNode(t, addrB, bootstrapAddr)
	nodeC := newCASNode(t, addrC, bootstrapAddr)

	// ---- phase 1: peer discovery ----
	t.Log("phase 1: waiting for full peer mesh")
	waitUntilCAS(t, 20*time.Second, 200*time.Millisecond, func() (bool, string) {
		for _, n := range []*casTestNode{nodeA, nodeB, nodeC} {
			if len(n.p.Peers()) < 3 {
				return false, fmt.Sprintf("%s has %d/3 peers", n.p.ID()[:8], len(n.p.Peers()))
			}
		}
		return true, ""
	})
	t.Log("phase 1 complete")

	// ---- phase 2: full upload via addToNetwork ----
	//
	// Both nodes call the same addToNetwork function the CLI uses.
	// This exercises: chunker.Split → store.Put → p.Announce for every block.
	t.Log("phase 2: uploading files via addToNetwork")
	ctx := context.Background()

	rootCIDAlpha, err := addToNetwork(ctx, nodeA.p, nodeA.store, "alpha.bin", alphaData, testChunkSize)
	if err != nil {
		t.Fatalf("nodeA addToNetwork: %v", err)
	}
	t.Logf("nodeA uploaded alpha.bin → root CID %s...", rootCIDAlpha[:16])

	rootCIDBeta, err := addToNetwork(ctx, nodeB.p, nodeB.store, "beta.bin", betaData, testChunkSize)
	if err != nil {
		t.Fatalf("nodeB addToNetwork: %v", err)
	}
	t.Logf("nodeB uploaded beta.bin → root CID %s...", rootCIDBeta[:16])

	if rootCIDAlpha == rootCIDBeta {
		t.Fatal("alpha and beta have the same root CID — test data must be distinct")
	}

	// Give DHT time to propagate across the network.
	time.Sleep(2 * time.Second)

	// ---- phase 3: DHT routing assertion ----
	//
	// Alpha CIDs must resolve exclusively to nodeA; beta CIDs to nodeB.
	// This proves the DHT correctly maps each content hash to its provider.
	t.Log("phase 3: verifying DHT routing")

	waitForProviderCAS(t, ctx, nodeC, rootCIDAlpha, nodeA.p.ID())
	waitForProviderCAS(t, ctx, nodeC, rootCIDBeta, nodeB.p.ID())

	providers, _ := nodeC.p.FindProviders(ctx, []byte(rootCIDAlpha))
	for _, pr := range providers {
		if pr.NodeID == nodeB.p.ID() {
			t.Errorf("alpha root CID: nodeB should not be a provider")
		}
	}
	providers, _ = nodeC.p.FindProviders(ctx, []byte(rootCIDBeta))
	for _, pr := range providers {
		if pr.NodeID == nodeA.p.ID() {
			t.Errorf("beta root CID: nodeA should not be a provider")
		}
	}
	t.Log("phase 3 complete: DHT routing is correct")

	// ---- phase 4: parallel multi-provider fetch via getFromNetwork ----
	//
	// nodeC fetches both files simultaneously. Each file's chunks come from a
	// different provider, so both goroutines hit different peers in parallel.
	// getFromNetwork itself also fetches each file's chunks in parallel internally.
	t.Log("phase 4: fetching alpha and beta in parallel via getFromNetwork")

	type fileResult struct {
		name string
		data []byte
		err  error
	}
	resultCh := make(chan fileResult, 2)

	go func() {
		data, filename, err := getFromNetwork(ctx, nodeC.p, nodeC.h, rootCIDAlpha)
		resultCh <- fileResult{filename, data, err}
	}()
	go func() {
		data, filename, err := getFromNetwork(ctx, nodeC.p, nodeC.h, rootCIDBeta)
		resultCh <- fileResult{filename, data, err}
	}()

	results := make(map[string][]byte, 2)
	for range 2 {
		r := <-resultCh
		if r.err != nil {
			t.Fatalf("getFromNetwork: %v", r.err)
		}
		results[r.name] = r.data
		t.Logf("received %q (%d bytes)", r.name, len(r.data))
	}

	if !bytes.Equal(results["alpha.bin"], alphaData) {
		t.Errorf("alpha.bin content mismatch (%d bytes received, %d expected)", len(results["alpha.bin"]), len(alphaData))
	}
	if !bytes.Equal(results["beta.bin"], betaData) {
		t.Errorf("beta.bin content mismatch (%d bytes received, %d expected)", len(results["beta.bin"]), len(betaData))
	}
	t.Log("phase 4 complete: both files match originals")

	// ---- phase 5: deduplication ----
	//
	// nodeA now also adds beta.bin (same content as nodeB).
	// Identical bytes → identical CIDs → same root CID → two providers in DHT.
	t.Log("phase 5: deduplication — nodeA adds the same file as nodeB")

	rootCIDBetaFromA, err := addToNetwork(ctx, nodeA.p, nodeA.store, "beta.bin", betaData, testChunkSize)
	if err != nil {
		t.Fatalf("nodeA addToNetwork beta: %v", err)
	}
	if rootCIDBetaFromA != rootCIDBeta {
		t.Errorf("deduplication failed: nodeA produced CID %s, nodeB produced %s", rootCIDBetaFromA[:16], rootCIDBeta[:16])
	}
	t.Logf("phase 5 complete: same content produced same root CID %s...", rootCIDBeta[:16])

	// Wait for nodeA's new beta announcements to propagate.
	time.Sleep(time.Second)

	// nodeC must still be able to fetch beta — now with two providers.
	data, _, err := getFromNetwork(ctx, nodeC.p, nodeC.h, rootCIDBeta)
	if err != nil {
		t.Fatalf("get beta after deduplication: %v", err)
	}
	if !bytes.Equal(data, betaData) {
		t.Error("beta.bin content mismatch after deduplication")
	}
	t.Log("phase 5 complete: multi-holder fetch succeeded")
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// waitForProviderCAS polls FindProviders until expectedNodeID appears in the results.
func waitForProviderCAS(t *testing.T, ctx context.Context, n *casTestNode, cid, expectedNodeID string) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		providers, err := n.p.FindProviders(ctx, []byte(cid))
		if err == nil {
			for _, pr := range providers {
				if pr.NodeID == expectedNodeID {
					return
				}
			}
		}
		select {
		case <-deadline:
			t.Fatalf("FindProviders(%s...): expected node %s not found after 10s", cid[:8], expectedNodeID[:8])
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// waitUntilCAS polls cond every poll interval until it returns true or the
// timeout expires.
func waitUntilCAS(t *testing.T, timeout, poll time.Duration, cond func() (bool, string)) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if ok, _ := cond(); ok {
			return
		}
		select {
		case <-deadline:
			_, last := cond()
			t.Fatalf("timed out after %s: %s", timeout, last)
		case <-time.After(poll):
		}
	}
}
