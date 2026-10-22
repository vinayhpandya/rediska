package commands

import (
	"github.com/vinayhpandya/rediska/core"
	"github.com/vinayhpandya/rediska/store"
)

func init() {
	Register("APPEND", handleAppend)
}

func handleAppend(args []string) []byte {
	if len(args) != 2 {
		return core.EncodeError("ERR wrong number of arguments for 'append' command")
	}
	key := args[0]
	value := args[1]

	newLen := store.Default.Append(key, value)

	return core.EncodeInteger(newLen)
}
