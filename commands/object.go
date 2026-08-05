package commands

import (
	"rediska/core"
	"rediska/store"
	"strings"
)

func init() {
	Register("OBJECT", handleObject)
}

func handleObject(args []string) []byte {
	if len(args) < 2 {
		return core.EncodeError("ERR wrong number of arguments for 'object' command")
	}
	subcommand := strings.ToUpper(args[0])
	key := args[1]
	switch subcommand {
	case "ENCODING":
		enc, ok := store.Default.GetEncoding(key)
		if !ok {
			return core.EncodeError("ERR no such key")
		}
		switch enc {
		case store.EncodingINT:
			return core.EncodeBulkString("int")
		case store.EncodingEMBSTR:
			return core.EncodeBulkString("embstr")
		case store.EncodingRAW:
			return core.EncodeBulkString("str")
		}
	case "FREQ":
		freq, ok, policyOK := store.Default.GetFreq(key)
		if !policyOK {
			return core.EncodeError("ERR An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between maxmemory policies at runtime LFU and LRU data will take some time to adjust.")
		}
		if !ok {
			return core.EncodeError("ERR no such key")
		}
		return core.EncodeInteger(int64(freq))
	default:
		return core.EncodeError("ERR unknown subcommand for 'object'")
	}
	return core.EncodeError("ERR unknown encoding")
}
