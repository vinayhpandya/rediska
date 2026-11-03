package commands

import (
	"fmt"
	"net"
	"strings"

	"github.com/vinayhpandya/rediska/cluster"
	"github.com/vinayhpandya/rediska/config"
	"github.com/vinayhpandya/rediska/core"
	"github.com/vinayhpandya/rediska/replication"
	"github.com/vinayhpandya/rediska/store"
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

// infoReplication shows this node's role. A replica also shows its
// master's address and whether its link to the master is up; a master
// shows how many replicas are connected to it.
func infoReplication() string {
	var b strings.Builder
	b.WriteString("# Replication\r\n")
	if l := cluster.Current; l != nil && l.Self.IsReplica() {
		b.WriteString("role:slave\r\n")
		if up := replication.Upstream; up != nil {
			host, port, _ := net.SplitHostPort(up.MasterAddr)
			status := "down"
			if up.Up() {
				status = "up"
			}
			fmt.Fprintf(&b, "master_host:%s\r\nmaster_port:%s\r\nmaster_link_status:%s\r\n", host, port, status)
		}
		return b.String()
	}
	fmt.Fprintf(&b, "role:master\r\nconnected_slaves:%d\r\n", replication.Default.Count())
	return b.String()
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
	case "replication":
		return core.EncodeBulkString(infoReplication())
	case "cluster":
		return core.EncodeBulkString(infoCluster())
	case "keyspace":
		return core.EncodeBulkString(infoKeyspace())
	case "all":
		// Same order as real Redis: Replication, then Cluster, then Keyspace.
		return core.EncodeBulkString(infoReplication() + "\r\n" + infoCluster() + "\r\n" + infoKeyspace())
	default:
		return core.EncodeBulkString("")
	}
}
