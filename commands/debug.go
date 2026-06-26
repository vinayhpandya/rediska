package commands

import (
	"strings"

	"rediska/core"
	"rediska/store"
)

// DEBUG is a non-write command (not in writeCommands), so it never triggers
// eviction or the OOM check — you can inspect memory even while over the limit.
func init() {
	Register("DEBUG", handleDebug)
}

func handleDebug(args []string) []byte {
	if len(args) == 0 {
		return core.EncodeError("ERR DEBUG subcommand required")
	}
	switch strings.ToUpper(args[0]) {
	case "MEMORY":
		// live byte counter that drives eviction
		return core.EncodeInteger(store.Default.UsedMemory())
	case "KEYS":
		return core.EncodeInteger(int64(store.Default.KeyCount()))
	default:
		return core.EncodeError("ERR unknown DEBUG subcommand '" + args[0] + "'")
	}
}
