package commands

import (
	"fmt"
	"rediska/config"
	"rediska/core"
	"rediska/store"
	"strings"
)

func init() {
	Register("INFO", handleInfo)
}

func infoCluster() string {
	enabled := 0
	if config.ClusterEnabled {
		enabled = 1
	}
	return fmt.Sprintf("# Cluster\r\ncluster_enabled:%d\r\n", enabled)
}

func infoKeyspace() string {
	keys := store.KeyspaceStat[0]["keys"]
	expires := store.KeyspaceStat[0]["expires"]
	avg_ttl := store.KeyspaceStat[0]["avg_ttl"]
	return fmt.Sprintf("# Keyspace\r\ndb0:keys=%d,expires=%d,avg_ttl=%d\r\n",
		keys, expires, avg_ttl)
}

func handleInfo(args []string) []byte {
	section := "all"
	if len(args) > 0 {
		section = strings.ToLower(args[0])
	}
	switch section {
	case "cluster":
		return core.EncodeBulkString(infoCluster())
	case "keyspace":
		return core.EncodeBulkString(infoKeyspace())
	case "all":
		// Same order as real Redis: Cluster comes before Keyspace.
		return core.EncodeBulkString(infoCluster() + "\r\n" + infoKeyspace())
	default:
		return core.EncodeBulkString("")
	}
}
