package config

var (
	Host             string
	Port             int
	AppendOnly       bool
	AppendOnlyFile   string
	MaxMemory        int    // 0 = no limit
	MaxMemoryPolicy  string // "noeviction" default
	MaxMemorySamples int    // 5 default
	LFULogFactor     int    // 10 default; controls LFU counter growth rate
	LFUDecayTime     int
)
