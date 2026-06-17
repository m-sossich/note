package dht

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"errors"

	"github.com/google/uuid"
	"github.com/m-sossich/note/pkg/p2p"
)

// nodeLink is the narrow interface DHT needs from a node.
type nodeLink interface {
	Register(protocol string, handler p2p.Handler)
	Send(peerID, protocol, msgType string, payload any) (int, error)
	ConnectionInfo(peerID string) (p2p.ConnInfo, bool)
}

// StoreResult describes the outcome of a Store operation.
// Attempted==0 means no other nodes exist yet. Replicated<Attempted means partial replication.
type StoreResult struct {
	Replicated int
	Attempted  int
}

// ProviderRecord is returned by FindProviders.
type ProviderRecord struct {
	NodeID  string
	Address string // declared listening address; always dialable (not ephemeral)
	Value   []byte // optional; nil is valid
}

// DHT is a Kademlia routing and storage layer. Call Stop before stopping the node.
// storedRecord wraps a ProviderRecord with its insertion/refresh timestamp.
type storedRecord struct {
	rec      ProviderRecord
	storedAt time.Time
}

type DHT struct {
	cfg      Config
	local    NodeInfo
	table    *routingTable
	store    map[DHTKey][]storedRecord
	storeMu  sync.RWMutex
	n        nodeLink
	pending  p2p.PendingMap[func(any) error]
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// New creates a DHT node and starts the background bucket prober.
func New(n nodeLink, nodeID string, address string, cfg Config) *DHT {
	cfg.setDefaults()
	key := KeyFromString(nodeID)
	d := &DHT{
		cfg: cfg,
		local: NodeInfo{
			NodeID:  nodeID,
			Address: address,
			Key:     key,
		},
		table:   newRoutingTable(key, cfg.BucketSize),
		store:   make(map[DHTKey][]storedRecord),
		n:       n,
		pending: *p2p.NewPendingMap[func(any) error](),
		stopCh:  make(chan struct{}),
	}
	n.Register(Protocol, d.handleMessage)
	d.wg.Add(2)
	go d.bucketProber()
	go d.storeCleaner()
	return d
}

func (d *DHT) Stop() {
	d.stopOnce.Do(func() {
		close(d.stopCh)
		d.wg.Wait()
	})
}

func (d *DHT) LocalKey() []byte {
	k := d.local.Key
	return k[:]
}

func (d *DHT) LocalNodeInfo() NodeInfo {
	return d.local
}

func (d *DHT) PeersInTable() []NodeInfo {
	return d.table.FindClosest(d.local.Key, math.MaxInt)
}

// SeedPeer bypasses the liveness check — use for bootstrapping.
func (d *DHT) SeedPeer(nodeID, addr string, pubKey []byte) {
	d.table.Update(NodeInfo{
		NodeID:    nodeID,
		Address:   addr,
		Key:       KeyFromString(nodeID),
		PublicKey: pubKey,
	}, func(NodeInfo) bool { return false })
}

// RemovePeer drops a peer whose capabilities are now known to exclude DHT.
func (d *DHT) RemovePeer(peerID string) {
	d.table.Remove(NodeInfo{NodeID: peerID, Key: KeyFromString(peerID)})
}

// Lookup performs an iterative FIND_NODE and returns the k closest nodes.
func (d *DHT) Lookup(ctx context.Context, key []byte) ([]NodeInfo, error) {
	target := KeyFromBytes(key)
	return d.iterativeLookup(ctx, target)
}

// Store announces this node as provider for key and replicates to the k closest nodes.
// value is optional. Call periodically to refresh the record before it expires.
func (d *DHT) Store(ctx context.Context, key, value []byte) (StoreResult, error) {
	target := KeyFromBytes(key)
	d.storeLocal(target, ProviderRecord{
		NodeID:  d.local.NodeID,
		Address: d.local.Address,
		Value:   value,
	})

	closest, err := d.iterativeLookup(ctx, target)
	if err != nil {
		return StoreResult{}, err
	}

	peers := make([]NodeInfo, 0, len(closest))
	for _, p := range closest {
		if p.NodeID != d.local.NodeID {
			peers = append(peers, p)
		}
	}

	type outcome struct{ replicated bool }
	ch := make(chan outcome, len(peers))

	for _, peer := range peers {
		peer := peer
		go func() {
			if err := d.sendStore(ctx, peer, key, value); err != nil {
				slog.Warn("dht: store replication failed", "peer_id", peer.NodeID, "err", err)
				ch <- outcome{false}
			} else {
				ch <- outcome{true}
			}
		}()
	}

	result := StoreResult{Attempted: len(peers)}
	for range peers {
		if (<-ch).replicated {
			result.Replicated++
		}
	}
	return result, nil
}

// FindProviders searches the network for all nodes that have announced themselves as holders of key.
func (d *DHT) FindProviders(ctx context.Context, key []byte) ([]ProviderRecord, error) {
	target := KeyFromBytes(key)
	if recs, ok := d.lookupLocal(target); ok {
		return recs, nil
	}
	return d.iterativeFindProviders(ctx, target)
}

type findResult struct {
	nodes []NodeInfo
	err   error
}

type providerResult struct {
	providers []ProviderRecord
	nodes     []NodeInfo
	err       error
}

// roundFn fans out α RPCs for one lookup round. stop=true halts immediately.
type roundFn func(round []NodeInfo) (candidates []NodeInfo, stop bool)

// iterativeSearch is the Kademlia iterative core. convergeOnNoProgress exits when a round yields no new candidates.
func (d *DHT) iterativeSearch(ctx context.Context, target DHTKey, convergeOnNoProgress bool, execute roundFn) ([]NodeInfo, error) {
	queried := make(map[string]struct{})
	queried[d.local.NodeID] = struct{}{}

	current := d.table.FindClosest(target, d.cfg.Alpha)

	for {
		if err := ctx.Err(); err != nil {
			return current, err
		}
		round := selectUnqueried(current, queried, d.cfg.Alpha)
		if len(round) == 0 {
			break
		}
		for _, p := range round {
			queried[p.NodeID] = struct{}{}
		}

		candidates, stop := execute(round)
		if stop {
			break
		}

		improved := false
		for _, n := range candidates {
			d.table.Update(n, func(NodeInfo) bool { return false })
			if _, seen := queried[n.NodeID]; !seen {
				current = appendUnique(current, n)
				improved = true
			}
		}
		current = sortAndCap(current, target, d.cfg.BucketSize)

		if convergeOnNoProgress && !improved {
			break
		}
	}

	return current, nil
}

func (d *DHT) iterativeLookup(ctx context.Context, target DHTKey) ([]NodeInfo, error) {
	return d.iterativeSearch(ctx, target, true, func(round []NodeInfo) ([]NodeInfo, bool) {
		results := make(chan findResult, len(round))
		for _, p := range round {
			go func(p NodeInfo) {
				nodes, err := d.sendFindNode(ctx, p, target)
				results <- findResult{nodes, err}
			}(p)
		}
		var candidates []NodeInfo
		for range round {
			res := <-results
			if res.err != nil {
				continue
			}
			candidates = append(candidates, res.nodes...)
		}
		return candidates, false
	})
}

// iterativeFindProviders accumulates provider records from every responsive node, deduplicated by NodeID.
func (d *DHT) iterativeFindProviders(ctx context.Context, target DHTKey) ([]ProviderRecord, error) {
	var found []ProviderRecord
	_, err := d.iterativeSearch(ctx, target, true, func(round []NodeInfo) ([]NodeInfo, bool) {
		results := make(chan providerResult, len(round))
		for _, p := range round {
			go func(p NodeInfo) {
				providers, nodes, err := d.sendFindProviders(ctx, p, target)
				results <- providerResult{providers, nodes, err}
			}(p)
		}
		var candidates []NodeInfo
		for range round {
			res := <-results
			if res.err != nil {
				continue
			}
			found = append(found, res.providers...)
			candidates = append(candidates, res.nodes...)
		}
		return candidates, false
	})
	if err != nil {
		return nil, err
	}
	return dedupProviders(found), nil
}

// sendRPC sends msgType to peer and blocks until a response arrives or ctx is cancelled.
func (d *DHT) sendRPC(ctx context.Context, peer NodeInfo, reqID, msgType string, msg any) (func(any) error, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, d.cfg.RequestTimeout)
	defer cancel()
	decode, err := d.pending.Wait(rpcCtx, reqID, func() error {
		_, err := d.n.Send(peer.NodeID, Protocol, msgType, msg)
		if err != nil {
			return fmt.Errorf("send %s to %s: %w", msgType, peer.NodeID, err)
		}
		return nil
	})
	if errors.Is(err, context.DeadlineExceeded) {
		return nil, fmt.Errorf("%s timeout for peer %s", msgType, peer.NodeID)
	}
	if err != nil {
		return nil, fmt.Errorf("%s cancelled for peer %s: %w", msgType, peer.NodeID, err)
	}
	return decode, nil
}

func (d *DHT) sendFindNode(ctx context.Context, peer NodeInfo, target DHTKey) ([]NodeInfo, error) {
	reqID := uuid.New().String()
	msg := findNode{RequestID: reqID, Key: encodeHex(target)}
	decode, err := d.sendRPC(ctx, peer, reqID, msgFindNode, msg)
	if err != nil {
		return nil, err
	}
	var result findNodeResult
	if err := decode(&result); err != nil {
		return nil, fmt.Errorf("decode FIND_NODE_RESULT: %w", err)
	}
	return d.wireToNodeInfos(result.Nodes), nil
}

func (d *DHT) sendFindProviders(ctx context.Context, peer NodeInfo, target DHTKey) ([]ProviderRecord, []NodeInfo, error) {
	reqID := uuid.New().String()
	msg := findProviders{RequestID: reqID, Key: encodeHex(target)}
	decode, err := d.sendRPC(ctx, peer, reqID, msgFindProviders, msg)
	if err != nil {
		return nil, nil, err
	}
	var result findProvidersResult
	if err := decode(&result); err != nil {
		return nil, nil, fmt.Errorf("decode FIND_PROVIDERS_RESULT: %w", err)
	}
	if len(result.Providers) > 0 {
		return wireToProviderRecords(result.Providers), nil, nil
	}
	return nil, d.wireToNodeInfos(result.Nodes), nil
}

// sendStore sends STORE to peer. The declared address is included so the receiver stores a dialable address.
func (d *DHT) sendStore(ctx context.Context, peer NodeInfo, key, value []byte) error {
	reqID := uuid.New().String()
	dhtKey := KeyFromBytes(key)
	msg := storeMsg{
		RequestID: reqID,
		Key:       encodeHex(dhtKey),
		Value:     encodeB64(value),
		Address:   d.local.Address,
	}
	decode, err := d.sendRPC(ctx, peer, reqID, msgStore, msg)
	if err != nil {
		return err
	}
	var ack storeAck
	if err := decode(&ack); err != nil {
		return fmt.Errorf("decode STORE_ACK: %w", err)
	}
	if !ack.OK {
		return fmt.Errorf("STORE rejected by peer %s", peer.NodeID)
	}
	return nil
}

func (d *DHT) parseKeyedRequest(decode func(any) error, dst keyedRequest, msgName string) (DHTKey, error) {
	if err := decode(dst); err != nil {
		return DHTKey{}, fmt.Errorf("parse %s: %w", msgName, err)
	}
	target, err := parseHexKey(wireHex(dst.getKey()))
	if err != nil {
		return DHTKey{}, fmt.Errorf("%s: %w", msgName, err)
	}
	return target, nil
}

// handleMessage routes incoming DHT messages. Every message refreshes the sender in the routing table.
func (d *DHT) handleMessage(peerID string, msgType string, decode func(any) error) error {
	d.refreshSender(peerID)
	switch msgType {
	case msgFindNode:
		return d.handleFindNode(peerID, decode)
	case msgFindNodeResult:
		return d.dispatchResponse(decode)
	case msgFindProviders:
		return d.handleFindProviders(peerID, decode)
	case msgFindProvidersResult:
		return d.dispatchResponse(decode)
	case msgStore:
		return d.handleStore(peerID, decode)
	case msgStoreAck:
		return d.dispatchResponse(decode)
	default:
		return fmt.Errorf("dht: unknown message type %q", msgType)
	}
}

func (d *DHT) handleFindNode(peerID string, decode func(any) error) error {
	var req findNode
	target, err := d.parseKeyedRequest(decode, &req, "FIND_NODE")
	if err != nil {
		return err
	}
	closest := d.table.FindClosest(target, d.cfg.BucketSize)
	_, err = d.n.Send(peerID, Protocol, msgFindNodeResult, findNodeResult{
		RequestID: req.RequestID,
		Nodes:     nodeInfosToWire(closest),
	})
	return err
}

func (d *DHT) handleFindProviders(peerID string, decode func(any) error) error {
	var req findProviders
	target, err := d.parseKeyedRequest(decode, &req, "FIND_PROVIDERS")
	if err != nil {
		return err
	}
	result := findProvidersResult{RequestID: req.RequestID}
	if recs, ok := d.lookupLocal(target); ok {
		result.Providers = providerRecordsToWire(recs)
	} else {
		result.Nodes = nodeInfosToWire(d.table.FindClosest(target, d.cfg.BucketSize))
	}
	_, err = d.n.Send(peerID, Protocol, msgFindProvidersResult, result)
	return err
}

// handleStore persists the STORE payload. Address from the message body, not ConnectionInfo, for dialability.
func (d *DHT) handleStore(peerID string, decode func(any) error) error {
	var req storeMsg
	target, err := d.parseKeyedRequest(decode, &req, "STORE")
	if err != nil {
		return err
	}
	value, err := decodeB64(req.Value)
	if err != nil {
		return fmt.Errorf("decode STORE value: %w", err)
	}
	address := req.Address
	if address == "" {
		// Fallback for senders that omit Address — use declared address, never ephemeral.
		if info, ok := d.n.ConnectionInfo(peerID); ok {
			address = info.DeclaredAddr
		}
	}
	d.storeLocal(target, ProviderRecord{
		NodeID:  peerID,
		Address: address,
		Value:   value,
	})
	_, err = d.n.Send(peerID, Protocol, msgStoreAck, storeAck{RequestID: req.RequestID, OK: true})
	return err
}

// dispatchResponse routes a response to the sendRPC caller waiting on its RequestID.
func (d *DHT) dispatchResponse(decode func(any) error) error {
	var header struct {
		RequestID string
	}
	if err := decode(&header); err != nil {
		return fmt.Errorf("dht: parse response request_id: %w", err)
	}
	d.pending.Deliver(header.RequestID, decode)
	return nil
}

// refreshSender updates the routing table with the sender's declared address.
// Skips peers whose declared address is unknown — ephemeral source ports are not dialable (DHT-2).
func (d *DHT) refreshSender(peerID string) {
	info, ok := d.n.ConnectionInfo(peerID)
	if !ok || info.DeclaredAddr == "" {
		return
	}
	sender := NodeInfo{
		NodeID:    peerID,
		Address:   info.DeclaredAddr,
		Key:       KeyFromString(peerID),
		PublicKey: info.PublicKey,
	}
	d.table.Update(sender, func(NodeInfo) bool { return false })
}

// bucketProber periodically checks full k-buckets for dead entries. Probes fan out in parallel.
func (d *DHT) bucketProber() {
	defer d.wg.Done()
	ticker := time.NewTicker(d.cfg.BucketProbeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			d.probeFullBuckets()
		case <-d.stopCh:
			return
		}
	}
}

func (d *DHT) probeFullBuckets() {
	candidates := d.table.LRSOfFullBuckets()
	if len(candidates) == 0 {
		return
	}
	done := make(chan struct{}, len(candidates))
	for _, lrs := range candidates {
		go func(lrs NodeInfo) {
			defer func() { done <- struct{}{} }()
			if !d.probeLRS(lrs) {
				d.table.Remove(lrs)
			}
		}(lrs)
	}
	for range candidates {
		<-done
	}
}

// probeLRS verifies a node is still reachable via FIND_NODE (Kademlia §2.2 k-bucket liveness).
// Uses a shorter timeout: an unreachable node has no routing value and should be evicted fast.
func (d *DHT) probeLRS(node NodeInfo) bool {
	timeout := d.cfg.RequestTimeout / 2
	if timeout > 2*time.Second {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := d.sendFindNode(ctx, node, d.local.Key)
	return err == nil
}

func nodeInfoToWire(n NodeInfo) nodeInfoWire {
	return nodeInfoWire{
		NodeID:    n.NodeID,
		Address:   n.Address,
		DHTKey:    encodeHex(n.Key),
		PublicKey: encodeB64(n.PublicKey),
	}
}

// wireToNodeInfo converts a wire entry to NodeInfo, applying the entry validator when set.
func (d *DHT) wireToNodeInfo(w nodeInfoWire) (NodeInfo, error) {
	key, err := parseHexKey(w.DHTKey)
	if err != nil {
		return NodeInfo{}, fmt.Errorf("invalid dht_key: %w", err)
	}
	var pubKey []byte
	if w.PublicKey != "" {
		pubKeyBytes, err := decodeB64(w.PublicKey)
		if err != nil || len(pubKeyBytes) != ed25519.PublicKeySize {
			return NodeInfo{}, fmt.Errorf("invalid public key encoding for node %q", w.NodeID)
		}
		pubKey = pubKeyBytes
	}
	if d.cfg.EntryValidator != nil && !d.cfg.EntryValidator(w.NodeID, pubKey) {
		return NodeInfo{}, fmt.Errorf("routing entry rejected by validator for node %q", w.NodeID)
	}
	return NodeInfo{NodeID: w.NodeID, Address: w.Address, Key: key, PublicKey: pubKey}, nil
}

func nodeInfosToWire(nodes []NodeInfo) []nodeInfoWire {
	result := make([]nodeInfoWire, len(nodes))
	for i, n := range nodes {
		result[i] = nodeInfoToWire(n)
	}
	return result
}

func (d *DHT) wireToNodeInfos(wires []nodeInfoWire) []NodeInfo {
	var result []NodeInfo
	for _, w := range wires {
		n, err := d.wireToNodeInfo(w)
		if err != nil {
			continue
		}
		result = append(result, n)
	}
	return result
}

func providerRecordToWire(r ProviderRecord) providerRecordWire {
	return providerRecordWire{
		NodeID:  r.NodeID,
		Address: r.Address,
		Value:   encodeB64(r.Value),
	}
}

func wireToProviderRecord(w providerRecordWire) ProviderRecord {
	value, _ := decodeB64(w.Value)
	return ProviderRecord{NodeID: w.NodeID, Address: w.Address, Value: value}
}

func providerRecordsToWire(recs []ProviderRecord) []providerRecordWire {
	result := make([]providerRecordWire, len(recs))
	for i, r := range recs {
		result[i] = providerRecordToWire(r)
	}
	return result
}

func wireToProviderRecords(wires []providerRecordWire) []ProviderRecord {
	result := make([]ProviderRecord, len(wires))
	for i, w := range wires {
		result[i] = wireToProviderRecord(w)
	}
	return result
}

// storeLocal inserts or updates a provider record. Re-announcing refreshes address, value, and TTL.
func (d *DHT) storeLocal(target DHTKey, rec ProviderRecord) {
	now := time.Now()
	d.storeMu.Lock()
	defer d.storeMu.Unlock()
	records := d.store[target]
	for i, r := range records {
		if r.rec.NodeID == rec.NodeID {
			records[i] = storedRecord{rec: rec, storedAt: now}
			d.store[target] = records
			return
		}
	}
	d.store[target] = append(records, storedRecord{rec: rec, storedAt: now})
}

func (d *DHT) lookupLocal(target DHTKey) ([]ProviderRecord, bool) {
	now := time.Now()
	d.storeMu.RLock()
	entries, ok := d.store[target]
	d.storeMu.RUnlock()
	if !ok {
		return nil, false
	}
	var result []ProviderRecord
	for _, e := range entries {
		if now.Sub(e.storedAt) < d.cfg.RecordTTL {
			result = append(result, e.rec)
		}
	}
	if len(result) == 0 {
		return nil, false
	}
	return result, true
}

// storeCleaner periodically evicts expired provider records.
func (d *DHT) storeCleaner() {
	defer d.wg.Done()
	ticker := time.NewTicker(d.cfg.StoreCleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			d.evictExpiredRecords()
		case <-d.stopCh:
			return
		}
	}
}

func (d *DHT) evictExpiredRecords() {
	now := time.Now()
	d.storeMu.Lock()
	defer d.storeMu.Unlock()
	for key, entries := range d.store {
		var live []storedRecord
		for _, e := range entries {
			if now.Sub(e.storedAt) < d.cfg.RecordTTL {
				live = append(live, e)
			}
		}
		if len(live) == 0 {
			delete(d.store, key)
		} else {
			d.store[key] = live
		}
	}
}

func dedupProviders(records []ProviderRecord) []ProviderRecord {
	seen := make(map[string]struct{}, len(records))
	result := make([]ProviderRecord, 0, len(records))
	for _, r := range records {
		if _, ok := seen[r.NodeID]; !ok {
			seen[r.NodeID] = struct{}{}
			result = append(result, r)
		}
	}
	return result
}

func selectUnqueried(candidates []NodeInfo, queried map[string]struct{}, limit int) []NodeInfo {
	var result []NodeInfo
	for _, n := range candidates {
		if _, done := queried[n.NodeID]; !done {
			result = append(result, n)
			if len(result) == limit {
				break
			}
		}
	}
	return result
}

func sortAndCap(nodes []NodeInfo, target DHTKey, k int) []NodeInfo {
	nodes = sortByDistance(nodes, target)
	if len(nodes) > k {
		return nodes[:k]
	}
	return nodes
}

func appendUnique(nodes []NodeInfo, n NodeInfo) []NodeInfo {
	for _, existing := range nodes {
		if existing.NodeID == n.NodeID {
			return nodes
		}
	}
	return append(nodes, n)
}

func sortByDistance(nodes []NodeInfo, target DHTKey) []NodeInfo {
	// Insertion sort — lists are small (≤ 2k).
	for i := 1; i < len(nodes); i++ {
		key := nodes[i]
		keyDist := XOR(key.Key, target)
		j := i - 1
		for j >= 0 && Less(keyDist, XOR(nodes[j].Key, target)) {
			nodes[j+1] = nodes[j]
			j--
		}
		nodes[j+1] = key
	}
	return nodes
}
