package commands

import (
	"rediska/core"
	"rediska/store"
)

func init() {
	Register("DEL", handleDel)
}

func handleDel(args []string) []byte {
	if len(args) < 1 {
		return core.EncodeError("ERR wrong number of arguments for 'del' command")
	}
	return core.EncodeInteger(store.Default.Delete(args...))
}
