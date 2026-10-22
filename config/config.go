// Package config holds the server's settings. main fills them in from
// command-line flags at startup, and the rest of the server reads them.
// They are not changed after startup.
package config

var (
	// Host is the address to listen on (--host, default 0.0.0.0, meaning
	// every network interface).
	Host string
	// Port is the TCP port to listen on (--port, default 7379).
	Port int
	// AppendOnly turns on the append-only file (--appendOnly).
	AppendOnly bool
	// AppendOnlyFile is the path of the append-only file (--appendOnlyFile).
	AppendOnlyFile string

	// MaxMemory is the memory limit in megabytes (--maxmemory). 0 means
	// no limit.
	MaxMemory int
	// MaxMemoryPolicy says what happens when MaxMemory is reached
	// (--maxmemory-policy): "noeviction" (the default) rejects writes;
	// "allkeys-lru", "allkeys-lfu" and "volatile-lfu" evict keys.
	MaxMemoryPolicy string
	// MaxMemorySamples is how many keys eviction samples per round
	// (--maxmemory-samples, default 5). More samples evict more accurately
	// but cost more time.
	MaxMemorySamples int
	// LFULogFactor controls how fast LFU counters grow (--lfu-log-factor,
	// default 10). Higher means slower growth.
	LFULogFactor int
	// LFUDecayTime is how many idle minutes it takes for a key's LFU
	// counter to drop by one (--lfu-decay-time, default 1).
	LFUDecayTime int
	// ClusterEnabled turns on cluster mode (--cluster-enabled).
	ClusterEnabled bool
	// ClusterConfigFile is the cluster layout file listing every node and
	// the slots it owns (--cluster-config-file). Required with
	// ClusterEnabled.
	ClusterConfigFile string
)
