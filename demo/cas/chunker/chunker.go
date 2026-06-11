// Package chunker splits files into fixed-size content-addressed blocks and
// produces a Manifest that links them together. This is the heart of CAS:
// identical bytes always produce the same CID, regardless of who holds them.
package chunker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// DefaultChunkSize is 256 KiB — the same default used by IPFS/Kubo.
const DefaultChunkSize = 256 * 1024

// Block is a single unit of content-addressed storage.
type Block struct {
	CID  string // hex(SHA-256(Data))
	Data []byte
}

// Manifest is the root block that describes a complete file.
// It contains the ordered list of chunk CIDs needed to reassemble the file.
// The manifest is itself content-addressed: its CID is SHA-256(JSON(manifest)),
// which becomes the file's root CID.
type Manifest struct {
	Filename string   `json:"filename"`
	Size     int64    `json:"size"`
	Chunks   []string `json:"chunks"` // ordered chunk CIDs
}

// CID computes the content identifier for arbitrary bytes: hex(SHA-256(data)).
func CID(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// Split divides data into fixed-size blocks and builds a Manifest block.
// Returns the manifest block (must be stored and announced alongside the data
// blocks) and the ordered list of data blocks.
//
// chunkSize <= 0 uses DefaultChunkSize.
// Two calls to Split with identical data always produce identical CIDs.
func Split(filename string, data []byte, chunkSize int) (manifest Block, chunks []Block) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	var chunkCIDs []string
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := data[i:end]
		cid := CID(chunk)
		chunks = append(chunks, Block{CID: cid, Data: chunk})
		chunkCIDs = append(chunkCIDs, cid)
	}
	m := Manifest{
		Filename: filename,
		Size:     int64(len(data)),
		Chunks:   chunkCIDs,
	}
	mData, _ := json.Marshal(m)
	manifest = Block{CID: CID(mData), Data: mData}
	return manifest, chunks
}

// DecodeManifest parses a manifest block's raw bytes.
func DecodeManifest(data []byte) (Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest: %w", err)
	}
	return m, nil
}
