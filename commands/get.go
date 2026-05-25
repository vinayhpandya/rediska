package commands

import (
	"rediska/core"
	"rediska/store"
)

func init() {
	Register("GET", handleGet)
}

func handleGet(args []string) []byte {
	if len(args) != 1 {
		return core.EncodeError("ERR wrong number of arguments for 'get' command")
	}

	key := args[0]

	value, ok := store.Default.Get(key)
	if !ok {
		return core.EncodeNullBulkString()
	}

	return core.EncodeBulkString(value)
}
