package commands

import (
	"rediska/core"
	"rediska/store"
	"strconv"
	"time"
)

func init() {
	Register("EXPIRE", handleExpire)
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
	value, err := store.Default.Get(key)
	if !err {
		return core.EncodeError("Cannot find key in the hashmap")
	}
	store.Default.Set(key, value, time.Duration(n)*time.Second)
	return core.EncodeInteger(1)
}
