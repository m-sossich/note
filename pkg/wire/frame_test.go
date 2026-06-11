package wire_test

import (
	"testing"

	"github.com/m-sossich/note/pkg/wire"
)

func TestFrame_EncodeDecodeRoundTrip(t *testing.T) {
	cases := []wire.Frame{
		{Type: wire.TypeIdent, Payload: []byte(`{"node_id":"abc"}`)},
		{Type: wire.TypeApplication, Payload: []byte("hello world")},
		{Type: wire.TypeDisconnect, Payload: []byte{}},
	}
	for _, f := range cases {
		encoded := wire.Encode(f)
		decoded, err := wire.Decode(encoded)
		if err != nil {
			t.Fatalf("Decode error for type 0x%02x: %v", f.Type, err)
		}
		if decoded.Type != f.Type {
			t.Errorf("type mismatch: got 0x%02x, want 0x%02x", decoded.Type, f.Type)
		}
		if string(decoded.Payload) != string(f.Payload) {
			t.Errorf("payload mismatch: got %q, want %q", decoded.Payload, f.Payload)
		}
	}
}

func TestFrame_DecodeEmpty(t *testing.T) {
	_, err := wire.Decode([]byte{})
	if err == nil {
		t.Fatal("expected error decoding empty data, got nil")
	}
}

func TestFrame_TypeBytePreserved(t *testing.T) {
	f := wire.Frame{Type: 0xAB, Payload: []byte("payload")}
	encoded := wire.Encode(f)
	decoded, err := wire.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.Type != 0xAB {
		t.Errorf("type byte not preserved: got 0x%02x", decoded.Type)
	}
}

func TestFrame_AllMessageTypes(t *testing.T) {
	types := []byte{
		wire.TypeIdent,
		wire.TypeDisconnect,
		wire.TypeError,
		wire.TypeApplication,
	}
	for _, typ := range types {
		f := wire.Frame{Type: typ, Payload: []byte("test")}
		decoded, err := wire.Decode(wire.Encode(f))
		if err != nil {
			t.Errorf("type 0x%02x: %v", typ, err)
		}
		if decoded.Type != typ {
			t.Errorf("type 0x%02x: round-trip failed", typ)
		}
	}
}

// TestDecode_SingleByte — a single byte is a valid type-only frame with empty payload.
func TestDecode_SingleByte(t *testing.T) {
	f, err := wire.Decode([]byte{0x01})
	if err != nil {
		t.Fatalf("Decode(single byte): unexpected error: %v", err)
	}
	if f.Type != 0x01 {
		t.Errorf("type: got 0x%02x, want 0x01", f.Type)
	}
	if len(f.Payload) != 0 {
		t.Errorf("payload: got %q, want empty", f.Payload)
	}
}
