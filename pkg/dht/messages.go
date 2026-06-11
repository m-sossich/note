package dht

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
)

const Protocol = "dht/1.0"

const (
	msgFindNode            = "FIND_NODE"
	msgFindNodeResult      = "FIND_NODE_RESULT"
	msgFindProviders       = "FIND_PROVIDERS"
	msgFindProvidersResult = "FIND_PROVIDERS_RESULT"
	msgStore               = "STORE"
	msgStoreAck            = "STORE_ACK"
)

// wireHex is a lowercase hex-encoded 32-byte DHTKey on the wire.
type wireHex string

// wireB64 is a standard base64-encoded byte slice on the wire.
type wireB64 string

func encodeHex(k DHTKey) wireHex { return wireHex(hex.EncodeToString(k[:])) }
func encodeB64(b []byte) wireB64 { return wireB64(base64.StdEncoding.EncodeToString(b)) }
func decodeB64(s wireB64) ([]byte, error) {
	return base64.StdEncoding.DecodeString(string(s))
}
func parseHexKey(s wireHex) (DHTKey, error) {
	b, err := hex.DecodeString(string(s))
	if err != nil || len(b) != keySpaceBits/8 {
		return DHTKey{}, fmt.Errorf("invalid hex key %q", s)
	}
	var k DHTKey
	copy(k[:], b)
	return k, nil
}

type nodeInfoWire struct {
	NodeID    string
	Address   string
	DHTKey    wireHex
	PublicKey wireB64
}

type providerRecordWire struct {
	NodeID  string
	Address string
	Value   wireB64
}

// keyedRequest lets parseKeyedRequest extract the hex key without reflection.
type keyedRequest interface {
	getKey() string
}

type findNode struct {
	RequestID string
	Key       wireHex
}

func (r *findNode) getKey() string { return string(r.Key) }

type findNodeResult struct {
	RequestID string
	Nodes     []nodeInfoWire
}

type findProviders struct {
	RequestID string
	Key       wireHex
}

func (r *findProviders) getKey() string { return string(r.Key) }

// findProvidersResult: exactly one of Providers or Nodes is non-empty (DHT-7).
type findProvidersResult struct {
	RequestID string
	Providers []providerRecordWire
	Nodes     []nodeInfoWire
}

// storeMsg carries the sender's declared address so the receiver stores a dialable entry.
type storeMsg struct {
	RequestID string
	Key       wireHex
	Value     wireB64
	Address   string
}

func (r *storeMsg) getKey() string { return string(r.Key) }

type storeAck struct {
	RequestID string
	OK        bool
}
