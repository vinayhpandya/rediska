package commands

import (
	"rediska/core"
	"rediska/store"
)

func init() {
	Register("PFCOUNT", handlePFCount)
}

func handlePFCount(args []string) []byte {
	if len(args) != 1 {
		return core.EncodeError("Incorrect number of arguments for PFCount")
	}

	key := args[0]
	count := store.Default.PFCount(key)
	return core.EncodeInteger(count)

}
