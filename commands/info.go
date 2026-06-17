package commands

import (
	"fmt"
	"rediska/core"
	"rediska/store"
	"strings"
)

func init() {
	Register("INFO", handleInfo)
}

func handleInfo(args []string) []byte {
	section := "all"
	if len(args) > 0 {
		section = strings.ToLower(args[0])
	}
	switch section {
	case "keyspace", "all":
		keys := store.KeyspaceStat[0]["keys"]
		expires := store.KeyspaceStat[0]["expires"]
		avg_ttl := store.KeyspaceStat[0]["avg_ttl"]
		output := fmt.Sprintf("# Keyspace\r\ndb0:keys=%d,expires=%d,avg_ttl=%d\r\n",
			keys, expires, avg_ttl)
		return core.EncodeBulkString(output)
	default:
		return core.EncodeBulkString("")
	}

}
