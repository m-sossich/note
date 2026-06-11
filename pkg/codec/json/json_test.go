package json_test

import (
	"testing"

	jsoncdc "github.com/m-sossich/note/pkg/codec/json"
)

func TestJSONCodec_RoundTrip(t *testing.T) {
	c := jsoncdc.New()
	type msg struct {
		ID     string `json:"id"`
		Value  int    `json:"value"`
		Nested struct {
			Flag bool `json:"flag"`
		} `json:"nested"`
	}
	original := msg{ID: "abc", Value: 42}
	original.Nested.Flag = true

	data, err := c.Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	var decoded msg
	if err := c.Decode(data, &decoded); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.ID != original.ID || decoded.Value != original.Value || decoded.Nested.Flag != original.Nested.Flag {
		t.Errorf("round-trip mismatch: got %+v, want %+v", decoded, original)
	}
}

func TestJSONCodec_ID(t *testing.T) {
	if jsoncdc.New().ID() != "json" {
		t.Error("expected ID() == \"json\"")
	}
}

func TestJSONCodec_DecodeError_ReturnsError(t *testing.T) {
	c := jsoncdc.New()
	var out map[string]any
	err := c.Decode([]byte("not valid json {{"), &out)
	if err == nil {
		t.Fatal("expected error decoding invalid JSON, got nil")
	}
	if out != nil {
		t.Error("expected nil output on decode failure")
	}
}

func TestJSONCodec_DecodeError_NoPartialValue(t *testing.T) {
	c := jsoncdc.New()
	type msg struct{ Value int }
	var out msg
	_ = c.Decode([]byte("{bad json"), &out)
	if out.Value != 0 {
		t.Errorf("expected zero value on decode failure, got %+v", out)
	}
}
