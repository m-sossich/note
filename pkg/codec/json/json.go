package json

import (
	stdjson "encoding/json"
	"fmt"
)

type Codec struct{}

func New() *Codec { return &Codec{} }

func (c *Codec) ID() string { return "json" }

func (c *Codec) Encode(v any) ([]byte, error) {
	return stdjson.Marshal(v)
}

func (c *Codec) Decode(data []byte, v any) error {
	if err := stdjson.Unmarshal(data, v); err != nil {
		return fmt.Errorf("json decode: %w", err)
	}
	return nil
}
