package dht

import "time"

const (
	defaultBucketSize          = 8
	defaultAlpha               = 3
	defaultRequestTimeout      = 10 * time.Second
	defaultBucketProbeInterval = 2 * time.Minute
)

// Config controls DHT tuning. Zero values use defaults.
type Config struct {
	BucketSize          int           // k-bucket capacity (Kademlia "k"); default: 8
	Alpha               int           // concurrent RPCs per lookup round; default: 3
	RequestTimeout      time.Duration // per-RPC deadline; default: 10s
	BucketProbeInterval time.Duration // how often to probe full buckets; default: 2m
}

func (c *Config) setDefaults() {
	if c.BucketSize == 0 {
		c.BucketSize = defaultBucketSize
	}
	if c.Alpha == 0 {
		c.Alpha = defaultAlpha
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = defaultRequestTimeout
	}
	if c.BucketProbeInterval == 0 {
		c.BucketProbeInterval = defaultBucketProbeInterval
	}
}
