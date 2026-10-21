package commands

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"rediska/cluster"
	"rediska/config"
	"rediska/core"
)

func init() {
	Register("CLUSTER", handleCluster)
}

// handleCluster dispatches CLUSTER subcommands. Error replies match
// Redis 7 word for word, so clients behave the same against either server.
func handleCluster(args []string) []byte {
	if !config.ClusterEnabled {
		return core.EncodeError("ERR This instance has cluster support disabled")
	}
	if len(args) == 0 {
		return core.EncodeError("ERR wrong number of arguments for 'cluster' command")
	}
	sub := strings.ToUpper(args[0])
	if sub != "KEYSLOT" && len(args) != 1 {
		return core.EncodeError(fmt.Sprintf("ERR wrong number of arguments for 'cluster|%s' command", strings.ToLower(sub)))
	}
	switch sub {
	case "KEYSLOT":
		if len(args) != 2 {
			return core.EncodeError("ERR wrong number of arguments for 'cluster|keyslot' command")
		}
		return core.EncodeInteger(int64(cluster.KeyHashSlot(args[1])))
	case "MYID":
		return core.EncodeBulkString(cluster.Current.Self.ID)
	case "SLOTS":
		return clusterSlots(cluster.Current)
	case "NODES":
		return core.EncodeBulkString(clusterNodes(cluster.Current))
	case "INFO":
		return core.EncodeBulkString(clusterInfo(cluster.Current))
	default:
		return core.EncodeError(fmt.Sprintf("ERR unknown subcommand '%s'. Try CLUSTER HELP.", args[0]))
	}
}

func clusterSlots(l *cluster.Layout) []byte {
	type entry struct {
		r cluster.SlotRange
		n *cluster.Node
	}
	var entries []entry
	for _, n := range l.Nodes {
		for _, r := range n.Slots {
			entries = append(entries, entry{r, n})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].r.Start < entries[j].r.Start })

	out := make([][]byte, 0, len(entries))
	for _, e := range entries {
		host, port := splitAddr(e.n.Addr)
		owner := core.EncodeRawArray([][]byte{
			core.EncodeBulkString(host),
			core.EncodeInteger(int64(port)),
			core.EncodeBulkString(e.n.ID),
			core.EncodeRawArray(nil), // empty array
		})
		out = append(out, core.EncodeRawArray([][]byte{
			core.EncodeInteger(int64(e.r.Start)),
			core.EncodeInteger(int64(e.r.End)),
			owner,
		}))
	}
	return core.EncodeRawArray(out)
}

// clusterNodes builds the CLUSTER NODES text: one line per node.
//
//	<id> <ip:port@busport> <flags> <master> <ping-sent> <pong-recv> <epoch> <link> <slots...>
//
// Fields we can't know yet (no gossip until Phase 4) get fixed values:
// ping/pong times are 0, every link is "connected", and each node's epoch
// is its line number in cluster.conf.
func clusterNodes(l *cluster.Layout) string {
	var b strings.Builder
	for i, n := range l.Nodes {
		host, port := splitAddr(n.Addr)
		flags := "master"
		if n == l.Self {
			flags = "myself,master"
		}
		fmt.Fprintf(&b, "%s %s:%d@%d %s - 0 0 %d connected",
			n.ID, host, port, port+10000, flags, i+1)
		for _, r := range n.Slots {
			if r.Start == r.End {
				fmt.Fprintf(&b, " %d", r.Start)
			} else {
				fmt.Fprintf(&b, " %d-%d", r.Start, r.End)
			}
		}
		b.WriteByte('\n')
	}
	return b.String()
}

// clusterInfo builds the CLUSTER INFO text. cluster_state is "ok" only if
// every slot has an owner, which is what real Redis reports. (We still serve
// the slots we do own; unowned ones get CLUSTERDOWN, as decided in 2a.)
func clusterInfo(l *cluster.Layout) string {
	assigned, size := 0, 0
	myEpoch := 0
	for i, n := range l.Nodes {
		for _, r := range n.Slots {
			assigned += r.End - r.Start + 1
		}
		if len(n.Slots) > 0 {
			size++ // cluster_size = nodes serving at least one slot
		}
		if n == l.Self {
			myEpoch = i + 1
		}
	}
	state := "ok"
	if assigned < cluster.NumSlots {
		state = "fail"
	}
	lines := []string{
		"cluster_state:" + state,
		"cluster_slots_assigned:" + strconv.Itoa(assigned),
		"cluster_slots_ok:" + strconv.Itoa(assigned),
		"cluster_slots_pfail:0",
		"cluster_slots_fail:0",
		"cluster_known_nodes:" + strconv.Itoa(len(l.Nodes)),
		"cluster_size:" + strconv.Itoa(size),
		"cluster_current_epoch:" + strconv.Itoa(len(l.Nodes)),
		"cluster_my_epoch:" + strconv.Itoa(myEpoch),
	}
	return strings.Join(lines, "\r\n") + "\r\n"
}

// splitAddr turns "127.0.0.1:7001" into ("127.0.0.1", 7001). The address
// was already validated when cluster.conf was loaded.
func splitAddr(addr string) (string, int) {
	i := strings.LastIndexByte(addr, ':')
	port, _ := strconv.Atoi(addr[i+1:])
	return addr[:i], port
}
