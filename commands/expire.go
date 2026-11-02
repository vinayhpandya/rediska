package commands

import (
	"strconv"
	"time"

	"github.com/vinayhpandya/rediska/core"
	"github.com/vinayhpandya/rediska/store"
)

func init() {
	Register("EXPIRE", handleExpire)
	Register("PEXPIREAT", handlePExpireAt)
}

func handleExpire(args []string) []byte {
	if len(args) != 2 {
		return core.EncodeError("ERR wrong number of arguments for EXPIRE command")
	}
	key := args[0]
	n, error := strconv.Atoi(args[1])
	if error != nil {
		return core.EncodeError("ERR invalid expire time for EXPIRE command")
	}
	if !store.Default.Expire(key, time.Duration(n)*time.Second) {
		return core.EncodeError("Cannot find key in the hashmap")
	}
	return core.EncodeInteger(1)
}

// PEXPIREAT key unix-ms sets key to expire at an absolute time, in Unix
// milliseconds. The AOF records every EXPIRE as PEXPIREAT, so a replay
// keeps the original deadline instead of starting a new countdown.
func handlePExpireAt(args []string) []byte {
	if len(args) != 2 {
		return core.EncodeError("ERR wrong number of arguments for 'pexpireat' command")
	}
	ms, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return core.EncodeError("ERR value is not an integer or out of range")
	}
	if !store.Default.ExpireAt(args[0], time.UnixMilli(ms)) {
		return core.EncodeInteger(0) // no such key, as in Redis
	}
	return core.EncodeInteger(1)
}
