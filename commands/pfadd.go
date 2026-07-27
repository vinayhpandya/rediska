package commands

import (
	"rediska/core"
	"rediska/store"
)

func init() {
	Register("PFADD", handlePFAdd)
}

func handlePFAdd(args []string) []byte {
	if len(args) < 1 {
		return core.EncodeError("Incorrect number of arguments for pfadd")
	}
	key := args[0]
	values := args[1:]
	changed := store.Default.PFAdd(key, values...)
	if changed {
		return core.EncodeInteger(1)
	}
	return core.EncodeInteger(0)
}
