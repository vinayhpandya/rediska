package commands

import "rediska/core"

func init() {
	Register("PING", handlePing)
}

func handlePing(args []string) []byte {
	switch len(args) {
	case 0:
		return core.EncodeSimpleString("PONG")
	case 1:
		return core.EncodeBulkString(args[0])
	default:
		return core.EncodeError("ERR wrong number of arguments for 'ping' command")
	}
}
