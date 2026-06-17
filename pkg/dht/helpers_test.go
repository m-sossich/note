package dht

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/m-sossich/note/pkg/identity"
	"github.com/m-sossich/note/pkg/node"
	"github.com/m-sossich/note/pkg/p2p"
)

// jsonDecoder returns a decode closure that unmarshals the given JSON bytes.
func jsonDecoder(data []byte) func(any) error {
	return func(v any) error {
		return json.Unmarshal(data, v)
	}
}

// nopNode satisfies node.Node for tests that only need a non-nil node reference.
type nopNode struct{}

func (nopNode) Start() error                                  { return nil }
func (nopNode) Stop() error                                   { return nil }
func (nopNode) Register(_ string, _ node.ProtocolHandler)     {}
func (nopNode) Send(_, _, _ string, _ any) (int, error)       { return 0, nil }
func (nopNode) Peers() []string                               { return nil }
func (nopNode) ConnectionInfo(_ string) (node.ConnInfo, bool) { return node.ConnInfo{}, false }
func (nopNode) BoundAddr() string                             { return "" }
func (nopNode) PeerProtocols(_ string) ([]string, bool)       { return nil, false }
func (nopNode) RegisteredProtocols() []string                 { return nil }

// stubNode is a nopNode with a controllable ConnectionInfo response.
type stubNode struct {
	nopNode
	info node.ConnInfo
	ok   bool
}

func (s *stubNode) ConnectionInfo(_ string) (node.ConnInfo, bool) { return s.info, s.ok }

// ---------------------------------------------------------------------------
// refreshSender
// ---------------------------------------------------------------------------

// TestRefreshSender_UsesDeclaredAddr verifies that refreshSender inserts the
// peer's DeclaredAddr (not RemoteAddr) into the routing table — satisfying DHT-2.
func TestRefreshSender_UsesDeclaredAddr(t *testing.T) {
	n := &stubNode{
		info: node.ConnInfo{RemoteAddr: "10.0.0.1:54321", DeclaredAddr: "10.0.0.1:9000"},
		ok:   true,
	}
	d := &DHT{
		cfg:   Config{BucketSize: defaultBucketSize},
		local: NodeInfo{NodeID: "local", Key: KeyFromString("local")},
		table: newRoutingTable(KeyFromString("local"), defaultBucketSize),
		n:     n,
	}

	d.refreshSender("peer-A")

	nodes := d.table.FindClosest(KeyFromString("peer-A"), 1)
	if len(nodes) == 0 {
		t.Fatal("routing table should contain peer-A after refreshSender")
	}
	if nodes[0].Address != "10.0.0.1:9000" {
		t.Errorf("routing table address = %q, want declared addr %q", nodes[0].Address, "10.0.0.1:9000")
	}
}

// TestRefreshSender_SkipsWhenNoDeclaredAddr verifies that refreshSender does not
// insert an entry when DeclaredAddr is empty — prevents ephemeral ports in the
// routing table (DHT-2).
func TestRefreshSender_SkipsWhenNoDeclaredAddr(t *testing.T) {
	n := &stubNode{
		info: node.ConnInfo{RemoteAddr: "10.0.0.1:54321", DeclaredAddr: ""},
		ok:   true,
	}
	d := &DHT{
		cfg:   Config{BucketSize: defaultBucketSize},
		local: NodeInfo{NodeID: "local", Key: KeyFromString("local")},
		table: newRoutingTable(KeyFromString("local"), defaultBucketSize),
		n:     n,
	}

	d.refreshSender("peer-B")

	nodes := d.table.FindClosest(KeyFromString("peer-B"), 1)
	if len(nodes) > 0 {
		t.Errorf("routing table should be empty when DeclaredAddr is unknown, got %v", nodes)
	}
}

// ---------------------------------------------------------------------------
// wireToNodeInfo / wireToNodeInfos — now methods on *DHT
// ---------------------------------------------------------------------------

// dhtForTest builds a minimal *DHT for unit tests. validator may be nil.
func dhtForTest(validator func(string, []byte) bool) *DHT {
	return &DHT{cfg: Config{EntryValidator: validator, BucketSize: defaultBucketSize}}
}

func TestWireToNodeInfo_InvalidHex(t *testing.T) {
	d := dhtForTest(nil)
	_, err := d.wireToNodeInfo(nodeInfoWire{
		NodeID:  "node-1",
		Address: "127.0.0.1:9001",
		DHTKey:  "not-hex",
	})
	if err == nil {
		t.Fatal("expected error for invalid hex DHTKey, got nil")
	}
}

func TestWireToNodeInfo_WrongLength(t *testing.T) {
	d := dhtForTest(nil)
	// Valid hex but only 4 bytes, not 32.
	_, err := d.wireToNodeInfo(nodeInfoWire{
		NodeID:  "node-1",
		Address: "127.0.0.1:9001",
		DHTKey:  "deadbeef",
	})
	if err == nil {
		t.Fatal("expected error for short DHTKey (4 bytes), got nil")
	}
}

func TestWireToNodeInfo_ValidRoundTrip(t *testing.T) {
	d := dhtForTest(nil)
	key := KeyFromString("test-node")
	orig := NodeInfo{
		NodeID:  "test-node",
		Address: "127.0.0.1:9001",
		Key:     key,
	}
	wire := nodeInfoToWire(orig)
	got, err := d.wireToNodeInfo(wire)
	if err != nil {
		t.Fatalf("wireToNodeInfo: %v", err)
	}
	if got.NodeID != orig.NodeID {
		t.Errorf("NodeID: got %q, want %q", got.NodeID, orig.NodeID)
	}
	if got.Address != orig.Address {
		t.Errorf("Address: got %q, want %q", got.Address, orig.Address)
	}
	if got.Key != orig.Key {
		t.Errorf("Key: got %x, want %x", got.Key, orig.Key)
	}
}

// TestWireToNodeInfo_PublicKey_ValidRoundTrip verifies that a NodeInfo with a
// public key round-trips correctly through a validator-equipped DHT.
func TestWireToNodeInfo_PublicKey_ValidRoundTrip(t *testing.T) {
	d := dhtForTest(identity.ValidateNodeEntry)
	kp, _ := identity.Generate()
	orig := NodeInfo{
		NodeID:    kp.NodeID,
		Address:   "127.0.0.1:9001",
		Key:       KeyFromString(kp.NodeID),
		PublicKey: kp.PublicKey,
	}
	wire := nodeInfoToWire(orig)
	got, err := d.wireToNodeInfo(wire)
	if err != nil {
		t.Fatalf("wireToNodeInfo: %v", err)
	}
	if got.NodeID != orig.NodeID {
		t.Errorf("NodeID: got %q, want %q", got.NodeID, orig.NodeID)
	}
	if base64.StdEncoding.EncodeToString(got.PublicKey) != base64.StdEncoding.EncodeToString(orig.PublicKey) {
		t.Error("PublicKey not preserved through round-trip")
	}
}

// TestWireToNodeInfo_PublicKey_MismatchedNodeID verifies that an entry whose
// public key does not hash to its claimed NodeID is rejected by the validator.
func TestWireToNodeInfo_PublicKey_MismatchedNodeID(t *testing.T) {
	d := dhtForTest(identity.ValidateNodeEntry)
	kpA, _ := identity.Generate()
	kpB, _ := identity.Generate()
	// Claim kpB's NodeID but supply kpA's public key.
	keyB := KeyFromString(kpB.NodeID)
	w := nodeInfoWire{
		NodeID:    kpB.NodeID,
		Address:   "127.0.0.1:9001",
		DHTKey:    encodeHex(keyB),
		PublicKey: encodeB64(kpA.PublicKey),
	}
	_, err := d.wireToNodeInfo(w)
	if err == nil {
		t.Error("expected rejection for mismatched public key / NodeID, got nil error")
	}
}

// TestWireToNodeInfo_PublicKey_InvalidEncoding verifies that a malformed
// base64 public key is rejected before the validator is even called.
func TestWireToNodeInfo_PublicKey_InvalidEncoding(t *testing.T) {
	d := dhtForTest(identity.ValidateNodeEntry)
	kp, _ := identity.Generate()
	keyKP := KeyFromString(kp.NodeID)
	w := nodeInfoWire{
		NodeID:    kp.NodeID,
		Address:   "127.0.0.1:9001",
		DHTKey:    encodeHex(keyKP),
		PublicKey: "not-valid-base64!!!",
	}
	_, err := d.wireToNodeInfo(w)
	if err == nil {
		t.Error("expected rejection for invalid public key encoding, got nil error")
	}
}

// TestWireToNodeInfo_MissingPublicKey_Rejected verifies that in verified mode
// (validator set) an entry with no public key is rejected — closing the routing
// table injection vector where a malicious peer omits the key to bypass verification.
func TestWireToNodeInfo_MissingPublicKey_Rejected(t *testing.T) {
	d := dhtForTest(identity.ValidateNodeEntry)
	key := KeyFromString("some-node")
	w := nodeInfoWire{
		NodeID:  "some-node",
		Address: "127.0.0.1:9001",
		DHTKey:  encodeHex(key),
		// PublicKey intentionally absent
	}
	_, err := d.wireToNodeInfo(w)
	if err == nil {
		t.Error("expected rejection for missing public key in verified mode, got nil error")
	}
}

// TestWireToNodeInfo_MissingPublicKey_TrustedAccepted verifies that in trusted
// mode (nil validator) an entry without a public key is accepted.
func TestWireToNodeInfo_MissingPublicKey_TrustedAccepted(t *testing.T) {
	d := dhtForTest(nil)
	key := KeyFromString("some-node")
	w := nodeInfoWire{
		NodeID:  "some-node",
		Address: "127.0.0.1:9001",
		DHTKey:  encodeHex(key),
	}
	_, err := d.wireToNodeInfo(w)
	if err != nil {
		t.Errorf("trusted mode: unexpected rejection for entry without public key: %v", err)
	}
}

func TestWireToNodeInfos_SkipsBadEntries(t *testing.T) {
	d := dhtForTest(nil)
	validKey := KeyFromString("valid-node")
	entries := []nodeInfoWire{
		{NodeID: "valid", Address: "127.0.0.1:9001", DHTKey: encodeHex(validKey)},
		{NodeID: "bad", Address: "127.0.0.1:9002", DHTKey: "not-hex"},
	}
	result := d.wireToNodeInfos(entries)
	if len(result) != 1 {
		t.Fatalf("expected 1 valid entry, got %d: %+v", len(result), result)
	}
	if result[0].NodeID != "valid" {
		t.Errorf("expected valid node, got %q", result[0].NodeID)
	}
}

// ---------------------------------------------------------------------------
// selectUnqueried
// ---------------------------------------------------------------------------

func TestSelectUnqueried_AllQueried(t *testing.T) {
	nodes := []NodeInfo{
		{NodeID: "a"}, {NodeID: "b"},
	}
	queried := map[string]struct{}{"a": {}, "b": {}}
	got := selectUnqueried(nodes, queried, 10)
	if len(got) != 0 {
		t.Errorf("all queried: expected empty, got %v", got)
	}
}

func TestSelectUnqueried_NoneQueried(t *testing.T) {
	nodes := []NodeInfo{
		{NodeID: "a"}, {NodeID: "b"}, {NodeID: "c"},
	}
	queried := map[string]struct{}{}
	got := selectUnqueried(nodes, queried, 2)
	if len(got) != 2 {
		t.Errorf("none queried, limit=2: expected 2, got %d", len(got))
	}
}

// ---------------------------------------------------------------------------
// appendUnique
// ---------------------------------------------------------------------------

func TestAppendUnique_DuplicateNotAdded(t *testing.T) {
	nodes := []NodeInfo{{NodeID: "a"}}
	result := appendUnique(nodes, NodeInfo{NodeID: "a"})
	if len(result) != 1 {
		t.Errorf("duplicate: expected length 1, got %d", len(result))
	}
}

func TestAppendUnique_NewNodeAppended(t *testing.T) {
	nodes := []NodeInfo{{NodeID: "a"}}
	result := appendUnique(nodes, NodeInfo{NodeID: "b"})
	if len(result) != 2 {
		t.Errorf("new node: expected length 2, got %d", len(result))
	}
	if result[1].NodeID != "b" {
		t.Errorf("appended node: got %q, want %q", result[1].NodeID, "b")
	}
}

// ---------------------------------------------------------------------------
// sortByDistance
// ---------------------------------------------------------------------------

func TestSortByDistance_OrderedAscending(t *testing.T) {
	target := KeyFromString("target")
	// Use deterministic keys and pick two that differ in XOR distance to target.
	k1 := KeyFromString("node-alpha")
	k2 := KeyFromString("node-beta")
	nodes := []NodeInfo{{NodeID: "alpha", Key: k1}, {NodeID: "beta", Key: k2}}
	sorted := sortByDistance(nodes, target)
	if len(sorted) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(sorted))
	}
	d0 := XOR(sorted[0].Key, target)
	d1 := XOR(sorted[1].Key, target)
	// d0 must be ≤ d1 (ascending XOR distance).
	if Less(d1, d0) {
		t.Errorf("sortByDistance not ascending: [0]=%s farther than [1]=%s", sorted[0].NodeID, sorted[1].NodeID)
	}
}

func TestSortByDistance_SingleNode(t *testing.T) {
	target := KeyFromString("target")
	nodes := []NodeInfo{{NodeID: "only", Key: KeyFromString("only")}}
	got := sortByDistance(nodes, target)
	if len(got) != 1 || got[0].NodeID != "only" {
		t.Errorf("single-node sort: unexpected result %+v", got)
	}
}

func TestSortByDistance_Empty(t *testing.T) {
	got := sortByDistance(nil, KeyFromString("target"))
	if len(got) != 0 {
		t.Errorf("empty sort: expected empty slice, got %+v", got)
	}
}

// ---------------------------------------------------------------------------
// sortAndCap
// ---------------------------------------------------------------------------

func TestSortAndCap_NoTruncation(t *testing.T) {
	target := KeyFromString("target")
	nodes := []NodeInfo{
		{NodeID: "a", Key: KeyFromString("a")},
		{NodeID: "b", Key: KeyFromString("b")},
	}
	// limit=10, only 2 nodes — no truncation path exercised.
	result := sortAndCap(nodes, target, 10)
	if len(result) != 2 {
		t.Errorf("expected 2 nodes (no truncation), got %d", len(result))
	}
}

func TestSortAndCap_Truncates(t *testing.T) {
	target := KeyFromString("target")
	nodes := []NodeInfo{
		{NodeID: "a", Key: KeyFromString("aaa")},
		{NodeID: "b", Key: KeyFromString("bbb")},
		{NodeID: "c", Key: KeyFromString("ccc")},
	}
	result := sortAndCap(nodes, target, 2)
	if len(result) != 2 {
		t.Errorf("expected 2 nodes after truncation, got %d", len(result))
	}
}

// ---------------------------------------------------------------------------
// parseKeyedRequest
// ---------------------------------------------------------------------------

func TestParseKeyedRequest_BadJSON(t *testing.T) {
	d := &DHT{cfg: Config{BucketSize: defaultBucketSize}}
	var req findNode
	_, err := d.parseKeyedRequest(jsonDecoder([]byte("not-json!!!")), &req, "FIND_NODE")
	if err == nil {
		t.Fatal("expected error for malformed JSON, got nil")
	}
}

func TestParseKeyedRequest_InvalidHexKey(t *testing.T) {
	d := &DHT{cfg: Config{BucketSize: defaultBucketSize}}
	var req findNode
	_, err := d.parseKeyedRequest(jsonDecoder([]byte(`{"request_id":"abc","key":"not-hex"}`)), &req, "FIND_NODE")
	if err == nil {
		t.Fatal("expected error for invalid hex key, got nil")
	}
}

// ---------------------------------------------------------------------------
// handleMessage — unknown type
// ---------------------------------------------------------------------------

func TestHandleMessage_UnknownType(t *testing.T) {
	d := &DHT{n: nopNode{}, pending: *p2p.NewPendingMap[func(any) error]()}
	err := d.handleMessage("peer-1", "UNKNOWN_MSG_TYPE", jsonDecoder([]byte("{}")))
	if err == nil {
		t.Fatal("expected error for unknown message type, got nil")
	}
}

// ---------------------------------------------------------------------------
// lookupLocal (via storeLocal — both are unexported, same package)
// ---------------------------------------------------------------------------

// TestLookupLocal_ReturnsCopy verifies that lookupLocal returns a copy:
// mutating the returned slice must not affect the stored records.
func TestLookupLocal_ReturnsCopy(t *testing.T) {
	d := &DHT{store: make(map[DHTKey][]ProviderRecord)}
	key := KeyFromString("copy-test")
	rec := ProviderRecord{NodeID: "n1", Address: "127.0.0.1:9001", Value: []byte("hello world")}
	d.storeLocal(key, rec)

	first, ok := d.lookupLocal(key)
	if !ok {
		t.Fatal("lookupLocal: key not found after storeLocal")
	}

	// Mutate the returned slice.
	first[0].NodeID = "mutated"

	second, ok := d.lookupLocal(key)
	if !ok {
		t.Fatal("lookupLocal: key not found on second lookup")
	}
	if second[0].NodeID != "n1" {
		t.Errorf("lookupLocal returned mutable reference: got %q, want %q", second[0].NodeID, "n1")
	}
}
