package commands

import (
	"strings"

	"github.com/vinayhpandya/rediska/core"
	"github.com/vinayhpandya/rediska/replication"
	"github.com/vinayhpandya/rediska/store"
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
	case "RECONNECT":
		// On a replica: drop the link to the master, as if the network had
		// failed. It connects again a second later and asks for just the
		// changes it missed. For testing partial resync.
		if replication.Upstream == nil {
			return core.EncodeError("ERR DEBUG RECONNECT only works on a replica")
		}
		replication.Upstream.Reconnect()
		return core.EncodeSimpleString("OK")
	case "DIGEST":
		// A fingerprint of all the data: a replica that holds exactly its
		// master's data shows the same digest.
		return core.EncodeBulkString(store.Default.Digest())
	default:
		return core.EncodeError("ERR unknown DEBUG subcommand '" + args[0] + "'")
	}
}
