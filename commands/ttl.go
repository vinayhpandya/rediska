// commands/ttl.go
package commands

import (
	"rediska/core"
	"rediska/store"
)

func init() {
	Register("TTL", handleTTL)
}

func handleTTL(args []string) []byte {
	if len(args) != 1 {
		return core.EncodeError("ERR wrong number of arguments for 'ttl' command")
	}

	seconds := store.Default.TTL(args[0])

	return core.EncodeInteger(seconds)
}
