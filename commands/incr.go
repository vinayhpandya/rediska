package commands

import (
	"rediska/core"
	"rediska/store"
	"strconv"
	"time"
)

func init() {
	Register("INCR", handleIncr)
}

func handleIncr(args []string) []byte {
	if len(args) != 1 {
		return core.EncodeError("ERR incorrect arguments for INCR")
	}
	key := args[0]
	n, exists, err, expiry := store.Default.GetInt(key)
	if !exists {
		return core.EncodeError("-1")
	}
	if err != nil {
		return core.EncodeError("ERR while incrementing")
	}
	var ttl time.Duration
	if !expiry.IsZero() {
		ttl = time.Until(expiry)
	}
	n++
	store.Default.Set(key, strconv.FormatInt(n, 10), ttl)
	return core.EncodeInteger(n)
}
