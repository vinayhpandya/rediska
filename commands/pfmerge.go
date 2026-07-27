package commands

import (
	"rediska/core"
	"rediska/store"
)

func init() {
	Register("PFMERGE", handlePFMerge)
}

func handlePFMerge(args []string) []byte {
	if len(args) < 1 {
		return core.EncodeError("Incorrect number of arguments for PFMerge")
	}
	dest := args[0]
	sources := args[1:]
	store.Default.PFMerge(dest, sources...)
	return core.EncodeSimpleString("OK")
}
