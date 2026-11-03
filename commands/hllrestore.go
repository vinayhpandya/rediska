package commands

import (
	"strconv"
	"strings"
	"time"

	"github.com/vinayhpandya/rediska/core"
	"github.com/vinayhpandya/rediska/store"
)

func init() {
	Register("_HLLRESTORE", handleHLLRestore)
}

// _HLLRESTORE key sketch [PXAT unix-ms] stores a HyperLogLog from its raw
// bytes (see hll.Bytes). It's internal, like _EXPIRY: a master's snapshot
// uses it to copy HyperLogLogs to a replica, since no client command can
// recreate a sketch exactly.
func handleHLLRestore(args []string) []byte {
	if len(args) != 2 && len(args) != 4 {
		return core.EncodeError("ERR wrong number of arguments for '_hllrestore' command")
	}
	var expiresAt time.Time
	if len(args) == 4 {
		ms, err := strconv.ParseInt(args[3], 10, 64)
		if !strings.EqualFold(args[2], "PXAT") || err != nil || ms <= 0 {
			return core.EncodeError("ERR syntax error")
		}
		expiresAt = time.UnixMilli(ms)
	}
	if err := store.Default.RestoreHLL(args[0], []byte(args[1]), expiresAt); err != nil {
		return core.EncodeError("ERR " + err.Error())
	}
	return core.EncodeSimpleString("OK")
}
