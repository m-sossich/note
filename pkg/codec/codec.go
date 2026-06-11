package codec

// Codec encodes and decodes wire messages.
type Codec interface {
	ID() string
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
}
